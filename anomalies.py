import pandas as pd
import numpy as np
import os, configparser

def scan_anomalies(cwd, session_data_path, anomaly_data_path, logger):

  sessions = pd.read_csv(f'{cwd}/{session_data_path}',keep_default_na=False)

  anomalies = dict(session_id=[], anomaly_description=[],value=[],unit=[])
  for i, row in sessions.iterrows():
    # Anomaly #1: Checks if plugged in time is longer than a day
    plugged_in_duration = row['total_session_duration']
    hour = pd.to_timedelta(plugged_in_duration) / np.timedelta64(1, 'h')
    if hour >= 24:
      anomalies['session_id'].append(row['session_id'])
      anomalies['anomaly_description'].append('User plugged in for longer than 24 hours')
      anomalies['value'].append(plugged_in_duration)
      anomalies['unit'].append('hh:mm:ss')

    
    # Anomaly #2: Checks if charging power exceeds 7 kW
    start_time_seconds = row['start_ts']
    end_time_seconds  = row['end_ts']
    plugged_in_time_hours = (end_time_seconds-start_time_seconds) / (3600)
    if plugged_in_time_hours < 0.01:
      power = 0
    else:
      power = row['energy'] / plugged_in_time_hours

    if power > 7:
      anomalies['session_id'].append(row['session_id'])
      anomalies['anomaly_description'].append('Charging power exceeds 7 kW')
      anomalies['value'].append(power)
      anomalies['unit'].append('kW')

    # Anomaly #3: Checks if active charging time is longer than 12 hours
    charging_duration = row['total_charging_duration']
    hour_c = pd.to_timedelta(charging_duration) / np.timedelta64(1, 'h')
    if hour_c >= 12:
      anomalies['session_id'].append(row['session_id'])
      anomalies['anomaly_description'].append('User actively charging for longer than 12 hours')
      anomalies['value'].append(charging_duration)
      anomalies['unit'].append('hh:mm:ss')

  df = pd.DataFrame(anomalies)
  df.to_csv(f'{cwd}/{anomaly_data_path}', encoding='utf-8', index=False)
  logger.info('Anomalies saved.')
