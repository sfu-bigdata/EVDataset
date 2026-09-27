#!/usr/bin/env python3
"""Download one ChargePoint update in the foreground."""
import argparse
from datetime import datetime, timezone
import sys
import pandas as pd
import numpy as np
from lib.ChargePointDatasetUtils import load_config, read_csv, save_csv, get_logger
from lib.anomalies import find_anomalies, refresh_anomalies

ALARM_KEY = ['station_id', 'port_no', 'alarm_type', 'alarm_ts']

def merge_records(old, new, keys, sort):
    if new.empty:
        return old if len(old.columns) else new.copy()
    combined = pd.concat([old, new], ignore_index=True) if not old.empty else new.copy()
    for col in ('session_id', 'credential_id', 'station_id', 'org_id', 'user_id'):
        if col in combined:
            combined[col] = combined[col].astype('string').fillna('')
    for col in ('start_ts', 'end_ts', 'alarm_ts'):
        if col in combined:
            combined[col] = pd.to_numeric(combined[col], errors='raise').astype('Int64')
    if 'port_no' in combined:
        combined['port_no'] = combined['port_no'].map(port_key)
    return combined.drop_duplicates(keys, keep='last').sort_values(sort, kind='stable').reset_index(drop=True)

def port_key(value):
    try:
        number = float(value)
        return str(int(number)) if number.is_integer() else str(number)
    except (ValueError, TypeError):
        return str(value)

def query_session_for_id(alarms, sessions):
    alarms = alarms.copy()
    alarms['session_id'] = ''
    if alarms.empty or sessions.empty:
        return alarms
    # Restrict matching to station/port groups, preserving first-match semantics.
    groups = {(str(station), port_key(port)): group for (station, port), group in sessions.groupby(['station_id', 'port_no'])}
    for (station, port), group in alarms.groupby(['station_id', 'port_no']):
        candidates = groups.get((str(station), port_key(port)))
        if candidates is None:
            continue
        starts = pd.to_numeric(candidates.start_ts, errors='coerce').to_numpy()
        ends = pd.to_numeric(candidates.end_ts, errors='coerce').to_numpy()
        ids = candidates.session_id.to_numpy()
        valid = np.isfinite(starts) & np.isfinite(ends) & (ends > starts)
        order = np.argsort(starts[valid], kind='stable')
        sorted_starts, sorted_ends, sorted_ids = starts[valid][order], ends[valid][order], ids[valid][order]
        timestamps = pd.to_numeric(group.alarm_ts).to_numpy()
        # Binary search is safe when a port's session intervals do not overlap.
        if len(sorted_starts) and np.all(sorted_starts[1:] >= sorted_ends[:-1]):
            positions = np.searchsorted(sorted_starts, timestamps, side='left') - 1
            bounded = np.maximum(positions, 0)
            matched = (positions >= 0) & (timestamps < sorted_ends[bounded])
            alarms.loc[group.index[matched], 'session_id'] = sorted_ids[bounded[matched]].astype(str)
            continue
        for index, ts in zip(group.index, timestamps):
            matches = (starts < ts) & (ends > ts)
            if matches.any():
                alarms.at[index, 'session_id'] = str(ids[matches][0])
    return alarms

def start_time(frame, column, overlap, since=None):
    if since is not None:
        return since
    if frame.empty:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    times = pd.to_numeric(frame[column], errors='coerce')
    latest = times.max()
    if pd.isna(latest):
        raise ValueError(f'No valid {column} values in existing dataset; use --since to recover')
    # Keep revisiting unfinished sessions, even if older than the overlap window.
    if column == 'start_ts':
        unfinished = pd.to_numeric(frame.end_ts, errors='coerce').isna() | (pd.to_numeric(frame.end_ts, errors='coerce') <= 0)
        if unfinished.any():
            latest = min(latest, times[unfinished].min())
    return datetime.fromtimestamp(max(0, latest - overlap * 86400), timezone.utc)

def utc_date(value):
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('Use an ISO date or timestamp') from exc

def run(client, config, root, args, logger):
    paths = {key: root / config.get('Paths', key) for key in
             ('session_data_path', 'station_data_path', 'alarm_data_path', 'anomaly_data_path')}
    sessions = read_csv(paths['session_data_path'])
    alarms = read_csv(paths['alarm_data_path'])
    end = datetime.now(timezone.utc)
    start = start_time(sessions, 'start_ts', args.overlap_days, args.since)
    logger.info('Downloading sessions from %s to %s', start, end)
    latest = client.queryChargingSession(start, end)
    merged = merge_records(sessions, latest, ['session_id'], ['start_ts', 'session_id'])
    if not latest.empty or not paths['session_data_path'].exists():
        save_csv(merged, paths['session_data_path'], previous=sessions)
    logger.info('Sessions: %d fetched; %d total', len(latest), len(merged))
    # Recompute only revisited sessions; first run builds the full anomaly file.
    anomaly_path = paths['anomaly_data_path']
    if not anomaly_path.exists():
        save_csv(find_anomalies(merged), anomaly_path)
    else:
        previous = read_csv(anomaly_path)
        # Also tags legacy rows when the API returns no new sessions.
        refreshed = refresh_anomalies(previous, merged, latest.session_id if not latest.empty else [])
        save_csv(refreshed, anomaly_path, previous=previous)
    start = start_time(alarms, 'alarm_ts', args.overlap_days, args.since)
    logger.info('Downloading alarms from %s to %s', start, end)
    latest_alarms = client.getAlarms(start, end)
    previous_alarms = alarms
    alarms = merge_records(alarms, latest_alarms, ALARM_KEY, ['alarm_ts', 'station_id', 'port_no'])
    # Rematch historical alarms too: a corrected session can change an old association.
    alarms = query_session_for_id(alarms, merged)
    save_csv(alarms, paths['alarm_data_path'], previous=previous_alarms)
    logger.info('Alarms: %d fetched; %d total', len(latest_alarms), len(alarms))
    stations = client.getStations()
    if not stations.empty:
        save_csv(stations, paths['station_data_path'], previous=read_csv(paths['station_data_path']))
    logger.info('Stations: %d port records fetched. Update complete.', len(stations))

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=str, default=None, help='Private config path (or SFU_EVP_CONFIG; default: ~/.config/sfu-evp/config.ini)')
    parser.add_argument('--since', type=utc_date, help='Override history start (ISO date/time, UTC by default)')
    parser.add_argument('--overlap-days', type=int, default=7, help='Revisit recent history (default: 7 days)')
    args = parser.parse_args(argv)
    if args.overlap_days < 0:
        parser.error('--overlap-days must be nonnegative')
    if args.since and args.since > datetime.now(timezone.utc):
        parser.error('--since must not be in the future')
    try:
        config, root = load_config(args.config)
        logger = get_logger('Update', root, config.get('Paths', 'session_log_path'))
        from lib.ChargePointApiClient import ChargePointApiClient
        with ChargePointApiClient(config.get('ChargePoint', 'api_key'), config.get('ChargePoint', 'secret')) as client:
            run(client, config, root, args, logger)
        return 0
    except KeyboardInterrupt:
        print('Update interrupted.', file=sys.stderr)
        return 130
    except Exception as exc:
        print(f'Update failed: {exc}', file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
