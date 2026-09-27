"""Versioned anomaly rules. Version 1 remains available for reproducibility."""
from pathlib import Path
import numpy as np
import pandas as pd
from lib.ChargePointDatasetUtils import read_csv, save_csv

RULES_VERSION = 2
COLUMNS = ['session_id', 'anomaly_description', 'value', 'unit', 'ver']


def version_history(frame):
    """Tag legacy rows without re-evaluating them or changing their values."""
    frame = frame.copy()
    if 'ver' not in frame:
        frame['ver'] = 1
    versions = pd.to_numeric(frame['ver'], errors='raise')
    if versions.isna().any() or (~versions.isin([1, 2])).any():
        raise ValueError('Anomaly history contains an unsupported or missing rule version')
    frame['ver'] = versions.astype('int64')
    return frame.reindex(columns=COLUMNS)


def _port(values):
    return values.astype('string').fillna('').str.replace(r'\.0$', '', regex=True)


def _context_values(context):
    """Find overlap/retry evidence using history, without emitting historical flags."""
    c = context.copy().reset_index(drop=True)
    if c.empty or not {'station_id', 'port_no', 'user_id'}.issubset(c):
        return {}, {}
    c['_start'] = pd.to_numeric(c.start_ts, errors='coerce')
    c['_end'] = pd.to_numeric(c.end_ts, errors='coerce')
    c['_port'] = _port(c.port_no)
    c = c[np.isfinite(c._start) & np.isfinite(c._end) & (c._end > c._start)
          & c.station_id.notna() & c.station_id.ne('') & c._port.ne('')]
    c = c.sort_values(['_start', 'session_id'], kind='stable')
    previous_end = c.groupby(['station_id', '_port'])['_end'].transform(lambda v: v.cummax().shift())
    # Intersection length, not just the previous end minus this start.
    overlap = (np.minimum(c._end, previous_end) - c._start).clip(lower=0)
    overlaps = dict(zip(c.session_id.astype(str), overlap))
    # Empty and legacy hashed missing-user IDs must not look like the same driver.
    import hashlib
    missing_users = {'', 'nan', 'None', '<NA>'}
    missing_users |= {hashlib.blake2b(v.encode(), digest_size=5).hexdigest() for v in ['None', 'nan', '']}
    c = c[c.user_id.notna() & ~c.user_id.astype(str).isin(missing_users)]
    previous_end = c.groupby(['station_id', '_port', 'user_id'])['_end'].shift()
    gap = c._start - previous_end
    return overlaps, dict(zip(c.session_id.astype(str), gap))


def find_anomalies(sessions, *, context=None, rules_version=RULES_VERSION):
    """Evaluate selected sessions; context supplies neighbours for v2 rules."""
    if rules_version not in (1, 2):
        raise ValueError('Supported rule versions are 1 and 2')
    if sessions.empty:
        return pd.DataFrame(columns=COLUMNS)
    s = sessions.reset_index(drop=True)
    duration = pd.to_timedelta(s.total_session_duration, errors='coerce').dt.total_seconds()
    charging = pd.to_timedelta(s.total_charging_duration, errors='coerce').dt.total_seconds()
    start = pd.to_numeric(s.start_ts, errors='coerce')
    end = pd.to_numeric(s.end_ts, errors='coerce')
    energy = pd.to_numeric(s.energy, errors='coerce')
    elapsed = end - start
    session_power = (energy / (elapsed / 3600)).where(elapsed >= 36, 0)
    rules = [
        (duration >= 86400, 'User plugged in for longer than 24 hours' if rules_version == 1 else 'Session duration at least 24 hours', s.total_session_duration, 'hh:mm:ss'),
        (session_power > 7, 'Charging power exceeds 7 kW' if rules_version == 1 else 'Session-average power exceeds 7 kW', session_power, 'kW'),
        (charging >= 43200, 'User actively charging for longer than 12 hours' if rules_version == 1 else 'Active charging duration at least 12 hours', s.total_charging_duration, 'hh:mm:ss'),
    ]
    if rules_version == 2:
        # Missing end times represent unfinished sessions, not a timestamp error.
        has_end = s.end_ts.notna() & s.end_ts.astype(str).str.strip().ne('')
        closed = has_end & np.isfinite(end) & np.isfinite(start) & (elapsed >= 0)
        active_power = energy / (charging / 3600)
        rules += [
            (~np.isfinite(start), 'Missing or invalid start timestamp', s.start_ts, 'raw'),
            (has_end & ~np.isfinite(end), 'Invalid end timestamp', s.end_ts, 'raw'),
            (~np.isfinite(energy), 'Missing or invalid energy', s.energy, 'raw'),
            (duration.isna(), 'Missing or invalid session duration', s.total_session_duration, 'raw'),
            (charging.isna(), 'Missing or invalid active charging duration', s.total_charging_duration, 'raw'),
            (energy < 0, 'Negative energy', energy, 'kWh'),
            (duration < 0, 'Negative session duration', duration, 'seconds'),
            (charging < 0, 'Negative active charging duration', charging, 'seconds'),
            (has_end & (elapsed < 0), 'End timestamp precedes start', elapsed, 'seconds'),
            (closed & ((duration - elapsed).abs() > 60), 'Session duration disagrees with timestamps by more than 60 seconds', (duration - elapsed).abs(), 'seconds'),
            (charging > duration + 60, 'Active charging exceeds session duration by more than 60 seconds', charging - duration, 'seconds'),
            (closed & (energy > 0) & (elapsed == 0), 'Positive energy with zero elapsed time', energy, 'kWh'),
            ((energy > 0) & (charging == 0), 'Positive energy with zero active charging time', energy, 'kWh'),
            (closed & (energy == 0) & (duration >= 300), 'Zero energy during a session of at least 5 minutes', duration / 60, 'minutes'),
            (closed & (charging >= 0) & (duration - charging >= 43200), 'Noncharging occupancy at least 12 hours', (duration - charging) / 3600, 'hours'),
            ((charging >= 300) & (active_power > 7), 'Active-time average power exceeds 7 kW (at least 5 charging minutes)', active_power, 'kW'),
        ]
        overlaps, retries = _context_values(s if context is None else context)
        overlap = s.session_id.astype(str).map(overlaps)
        retry = s.session_id.astype(str).map(retries)
        rules += [
            (overlap > 60, 'Same-port session overlap exceeds 60 seconds', overlap, 'seconds'),
            ((retry >= 0) & (retry <= 300), 'Same-driver same-port restart within 5 minutes', retry, 'seconds'),
        ]
    frames = []
    for order, (mask, description, values, unit) in enumerate(rules):
        mask = mask.fillna(False)
        part = pd.DataFrame({'session_id': s.loc[mask, 'session_id'], 'anomaly_description': description,
                             'value': values[mask], 'unit': unit, 'ver': rules_version})
        part['_row'] = part.index
        part['_rule'] = order
        if not part.empty:
            frames.append(part)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames).sort_values(['_row', '_rule'])[COLUMNS].reset_index(drop=True)


def refresh_anomalies(previous, sessions, selected_ids):
    """Replace results only for selected IDs; retain historical version labels."""
    previous = version_history(previous)
    ids = {str(value) for value in selected_ids}
    kept = previous[~previous.session_id.astype(str).isin(ids)]
    selected = sessions[sessions.session_id.astype(str).isin(ids)] if not sessions.empty else sessions
    refreshed = find_anomalies(selected, context=sessions)
    parts = [p for p in [kept, refreshed] if not p.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS)


def scan_anomalies(cwd, session_data_path, anomaly_data_path, logger):
    """Explicit full rebuild (not used by the incremental downloader)."""
    save_csv(find_anomalies(read_csv(Path(cwd) / session_data_path)), Path(cwd) / anomaly_data_path)
    logger.info('Anomalies rebuilt using rule version %s.', RULES_VERSION)
