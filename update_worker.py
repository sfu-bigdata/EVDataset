# update_worker.py

import os, sys, daemon, time, threading, configparser
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from ChargePointApiClient import ChargePointApiClient as API
from ChargePointDatasetUtils import get_logger
from anomalies import scan_anomalies

cwd = os.getcwd()
config = configparser.ConfigParser()
config.read('config.ini')

def update_session_data(client):
  try:
    # read variables from config.ini
    log_path = config.get('Paths','session_log_path')
    session_data_path = config.get('Paths','session_data_path')
    update_freq = int(config.get('Parameters', 'session_update_frequency'))
    anomaly_data_path = config.get('Paths','anomaly_data_path')
  except Exception as e:
    print('An error occurred:', str(e))
    return

  logger = get_logger('Session Worker', cwd, log_path)
  # set default data path
  default_path = os.path.join(cwd, session_data_path)

  while True:
    try:
      
      if os.path.exists(default_path):
        main_data = pd.read_csv(default_path)
      else:
        main_data = pd.DataFrame() # if file not found, create an empty df, then a query from 1970-1-1 to now will be performed.

      if not main_data.empty: # if old dataset exists
        logger.info(f"Historical data Found in {session_data_path}.")
        start_datetime_str = main_data.iloc[-1]["end_ts"] # UTC time in the dataset
        start_datetime = datetime.fromtimestamp(int(start_datetime_str)+1)
        latest_data = client.queryChargingSession(start_datetime)
        
        old_size = len(main_data)
        # merge old and new data
        main_data = pd.concat([main_data, latest_data]).drop_duplicates(subset='session_id')
        main_data.to_csv(default_path, index=False)
        if old_size != len(main_data):
          logger.info("Data merged. Old size: {}, New size: {}.".format(old_size, len(main_data)))
        else:
          logger.info("No new data found.")
        
      else: # perform a query with time range (1970-1-1 00:00:00, Current UTC datetime) and save to the default path
        start_datetime = datetime(1970, 1, 1)
        end_datetime = datetime.utcnow()
        # start_datetime = datetime(2023,5,20)
        # end_datetime = datetime(2023,6,1)
        logger.info("Sessions data file {} does not exist.".format(session_data_path))
        logger.info("Performing a default query from {}(UTC) to {}(UTC)".format(start_datetime, end_datetime.strftime("%Y-%m-%d %H:%M:%S")))

        main_data = client.queryChargingSession(startTime=start_datetime, endTime=end_datetime)
        main_data.to_csv(default_path, index=False)
        logger.info("Query completed.")
        logger.info("Found {} row(s) of new data from {}(UTC) to {}(UTC).".format(len(main_data), start_datetime, end_datetime))
        logger.info("Sessions data has been saved to {}.".format(session_data_path))
      
      scan_anomalies(cwd, session_data_path, anomaly_data_path, logger)
      time.sleep(update_freq)
   
    except Exception as e:
      logger.error(f"An exception occurred: {str(e)}")
      return

def update_station_data(client):
  try:
    # read variables from config.ini
    log_path = config.get('Paths','station_log_path')
    station_data_path = config.get('Paths','station_data_path')
    update_freq = int(config.get('Parameters', 'station_update_frequency'))
  except Exception as e:
    print('An error occurred:', str(e))
    return
  
  while True:
    try:
      logger = get_logger('Station Worker', cwd, log_path)
      default_path = os.path.join(cwd, station_data_path)

      station_data = client.getStations()
      logger.info('{} station records found.'.format(len(station_data)))

      station_data.to_csv(default_path, index=False)
      logger.info('Stations data has been saved to {}'.format(station_data_path))


      time.sleep(update_freq)

    except Exception as e:
      logger.error('An error occurred:', str(e))
      return

def query_session_for_id(alarms, sessions):
  if alarms.empty or sessions.empty: return alarms
  alarms['session_id'] = ''
  for i, alarm_row in alarms.iterrows():
      filtered_sessions = sessions[
          (sessions['station_id'] == alarm_row['station_id'])&
          (sessions['port_no'] == alarm_row['port_no'])
      ]
      matching_sessions = filtered_sessions[
          (filtered_sessions['start_ts'] < alarm_row['alarm_ts']) &
          (filtered_sessions['end_ts'] > alarm_row['alarm_ts'])
      ]
      if not matching_sessions.empty:
          alarms.at[i, 'session_id'] = matching_sessions.iloc[0]['session_id']

  return alarms

def update_alarm_data(client):
  try:
    alarm_data_path = config.get('Paths', 'alarm_data_path')
    alarm_log_path = config.get('Paths', 'alarm_log_path')
    alarm_update_frequency = int(config.get('Parameters', 'alarm_update_frequency'))
    session_data_path = config.get('Paths','session_data_path')
  except Exception as e:
    print(str(e))
    return
  logger = get_logger('Alarm Worker', cwd, alarm_log_path)
  # set default data path
  default_path = os.path.join(cwd, alarm_data_path)
  while True:
    try:
      if os.path.exists(default_path):
        main_data = pd.read_csv(default_path)
      else:
        main_data = pd.DataFrame()

      if not main_data.empty:
        logger.info(f"Historical data Found in {alarm_data_path}.")
        start_datetime_str = main_data.iloc[-1]["alarm_ts"] # UTC time in the dataset
        start_datetime = datetime.fromtimestamp(int(start_datetime_str))
        end_datetime = datetime.utcnow()
        latest_data = client.getAlarms(start_datetime, end_datetime)

        old_size = len(main_data)
        # Query session_ids from sessions
        sessions = pd.read_csv(os.path.join(cwd, session_data_path))
        latest_data = query_session_for_id(latest_data, sessions)

        # merge old and new data
        main_data = pd.concat([main_data, latest_data]).drop_duplicates(subset='alarm_ts')
        main_data.to_csv(default_path, index=False)
        if old_size != len(main_data):
          logger.info("Data merged. Old size: {}, New size: {}.".format(old_size, len(main_data)))
        else:
          logger.info("No new data found.")

      else:
        start_datetime = datetime(1970, 1, 1)
        end_datetime = datetime.utcnow()
        # start_datetime = datetime(2023,7,1)
        # end_datetime = datetime(2023,7,2)
        logger.info("Alarms data file {} does not exist.".format(alarm_data_path))
        logger.info("Performing a default query from {}(UTC) to {}(UTC)".format(start_datetime, end_datetime.strftime("%Y-%m-%d %H:%M:%S")))

        main_data = client.getAlarms(startTime=start_datetime, endTime=end_datetime)
        # Query session_ids from sessions
        sessions = pd.read_csv(session_data_path)
        main_data = query_session_for_id(main_data, sessions)

        main_data.to_csv(default_path, index=False)
        start_ts, end_ts = datetime.fromtimestamp(int(main_data.iloc[0]['alarm_ts'])), datetime.fromtimestamp(int(main_data.iloc[-1]['alarm_ts']))
        logger.info("Query completed.")
        logger.info("Found {} row(s) of new data from {}(UTC) to {}(UTC).".format(len(main_data), start_ts, end_ts))
        logger.info("Alarms data has been saved to {}.".format(alarm_data_path))

      time.sleep(alarm_update_frequency)
    except Exception as e:
      print(str(e))
      logger.error('An error occurred:', str(e))
      return


def start_worker(func, client):
  th = threading.Thread(target=func, args=(client, ))
  th.start()
  print(f'Thread {func.__name__} started.')
  return th

if __name__ == "__main__":
    threads = []
    with daemon.DaemonContext(stdout=sys.stdout) as context:
        print(os.getpid())
        try:
          api_key, secret = [v for k,v in config.items('ChargePoint')]
          client = API(api_key, secret)

          functions = [update_session_data, update_station_data, update_alarm_data]

          threads = [start_worker(func, client) for func in functions]
          
        except Exception as e:
          print(f"An exception occurred: {str(e)}")
          for th in threads:
            th.join()