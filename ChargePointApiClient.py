# -*- coding: utf-8 -*-

from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.helpers import serialize_object
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
import os
import hashlib


cwd=os.getcwd()

# ChargePointApiClient.py

class ChargePointApiClient:

  def __init__(self, api_key, api_secret):
    self.serv_impl = self.getApiServiceImpl(api_key, api_secret)
    self.local_timezone = pytz.timezone('America/Vancouver')
    self.dt_format = '%Y-%m-%d %H:%M:%S'
    self.earliest = datetime(1970, 1, 1, tzinfo=pytz.UTC)

  def encrypt(self, obj, output_length=5):

    hash_object = hashlib.blake2b(str(obj).encode(), digest_size=output_length)
    return hash_object.hexdigest()


  def getApiServiceImpl(self, api_key, api_secret):
    """
    Returns a Chargepoint API service implementation instance using the provided API key and secret.

    Parameters:
    api_key (str): The Chargepoint API key to use for authentication.
    api_secret (str): The Chargepoint API secret to use for authentication.

    Returns:
    zeep.ClientService: A Chargepoint API service implementation instance.
    """
    wsdl_url = "https://webservices.chargepoint.com/cp_api_5.1.wsdl"
    client = Client(wsdl_url, wsse=UsernameToken(api_key, api_secret))
    return client.service

  def getAlarms(self, startTime, endTime):
    alarm_data_list = []
    moreFlag = 1
    startRecord, recordLimit = 1, 100

    searchQuery={
        'startTime': startTime,
        'endTime': endTime
    }

    while moreFlag:
      searchQuery['startRecord'] = startRecord
      response = self.serv_impl.getAlarms(searchQuery)
      alarm_data_list.extend(serialize_object(response['Alarms']))
      moreFlag = response['moreFlag']
      startRecord += recordLimit

    df_alarms = pd.DataFrame(alarm_data_list)
    if df_alarms.empty: return df_alarms
    
    columns_selected = ['stationID',	'stationName',	'stationModel',	'orgID',	'portNumber',	'alarmType',	'alarmTime']
    columns_alias = ['station_id',	'station_name',	'model',	'org_id', 'port_no', 'alarm_type', 'alarm_ts', 'alarm_dt']
    df_alarms = df_alarms[columns_selected]
    df_alarms['alarmDt'] = df_alarms['alarmTime']

    df_alarms[['stationID', 'orgID']] = df_alarms[['stationID', 'orgID']].apply(lambda x : x.apply(self.encrypt)) # hash
    df_alarms['alarmTime'] = df_alarms['alarmTime'].apply(lambda x : str(int((x-self.earliest).total_seconds()))) # UTC timestamp integers
    df_alarms['alarmDt'] = df_alarms['alarmDt'].apply(lambda x : x.tz_convert(self.local_timezone))\
                                               .apply(lambda x : x.strftime(self.dt_format))
    df_alarms.columns = columns_alias
    df_alarms['alarm_ts'] = df_alarms['alarm_ts'].astype(np.int64)
    df_alarms = df_alarms.sort_values('alarm_ts', ascending=True)
    df_alarms['port_no'] = df_alarms['port_no'].astype(np.float16)

    return df_alarms
    

  def getCPNInstances(self):
    return self.serv_impl.getCPNInstances()

  def getOrgsAndStationGroups(self, searchQuery={}):
    self.serv_impl.getOrgsAndStationGroups(searchQuery)

  def getStationGroups(self, orgID):
    return self.serv_impl.getStationGroups(orgID)

  def getStationRights(self, searchQuery={}):
    return self.serv_impl.getStationRights(searchQuery)

  def getStationRightsProfile(self, sgID):
    return self.serv_impl.getStationRightsProfile(sgID)

  def getStationStatus(self, searchQuery={}):
    return self.serv_impl.getStationStatus(searchQuery)

  def getStations(self, searchQuery={}):
    response = self.serv_impl.getStations(searchQuery)
    df_stations = pd.DataFrame(serialize_object(response["stationData"])) # raw data

    columns_selected = ['stationID', 'orgID', 'sgID', 'stationModel', 'stationActivationDate', 'timezoneOffset', 'Port', 'Address', 'stationManufacturer', 'stationName', 'Description']

    df_stations = df_stations[columns_selected] # filtered data

    # hash ID fields
    df_stations[['stationID', 'orgID']] = df_stations[['stationID', 'orgID']].apply(lambda x : x.apply(self.encrypt))

    df_stations['sgID'] = df_stations['sgID'].apply(lambda x : x.replace(', ', ';')) # replace separators with semicolons

    # convert date to local timezone
    df_stations['stationActivationDate'] = df_stations['stationActivationDate'].apply(lambda x : x.astimezone(self.local_timezone))\
    .dt.strftime(self.dt_format)

    # address nested port information
    buf = list()
    for index, row in df_stations.iterrows():
      row_copy = row.copy()
      row.Port = row.Port[0] # set the first port information
      df_stations.iloc[index] = row # replace this row
      row_copy.Port = row_copy.Port[1] # set the second port information
      buf.append(row_copy) # add it to a buffer

    df_new = pd.concat([df_stations, pd.DataFrame(buf)], axis=0).reset_index(drop=True) # concat the new rows to the end of dataframe
    expanded_port = pd.json_normalize(df_new.Port) # break down all fields to the dataframe
    expanded_port = expanded_port.drop('Connectors', axis=1)
    df_new = pd.concat([df_new.drop('Port', axis=1), expanded_port], axis=1) # concat the expanded fields to the right

    columns_alias = ['station_id', 'org_id', 'station_group', 'model', 'activation_dt',
       'timezone_offset', 'address', 'manufacturer', 'station_name' ,
       'description', 'port_no', 'reservable', 'status', 'level',
       'time_stamp', 'mode', 'connector', 'voltage', 'current', 'power',
       'estimated_cost', 'location_lat', 'location_long']
    df_new.columns = columns_alias
    return df_new


  def queryChargingSession(self, startTime, endTime=None):
    """Queries charging session data within a specified time range.

    Args:
        startTime (datetime): The start time of the query range.
        endTime (datetime): The end time of the query range.

    Returns:
        pandas.DataFrame: A DataFrame containing the charging session data.
    """
    session_data_list = [] # the result list to be returned
    moreFlag = 1 # flag that indicates whether the number of records exceeds 100
    startRecord = 1 # the index of the first row in the result
    recordLimit = 100 # number of return limit of the API service
    searchQuery = {
      'fromTimeStamp': startTime,
      # 'startRecord': startRecord
      # 'toTimeStamp': endTime
    }

    if endTime: # if specified an end timestamp
      searchQuery['toTimeStamp'] = endTime

    while moreFlag:
      searchQuery['startRecord'] = startRecord # update the start record
      response = self.serv_impl.getChargingSessionData(searchQuery)
      session_data_list.extend(serialize_object(response['ChargingSessionData']))
      moreFlag = response['MoreFlag'] # update the flag
      startRecord += recordLimit # increment the index

    # columns selected
    columns_selected = ['sessionID','userID','credentialID','stationID','portNumber',
                        'startTime','endTime','Energy', 'totalChargingDuration', 'totalSessionDuration' ,'Address']
    columns_alias = ['session_id', 'user_id', 'credential_id', 'station_id', 'port_no',
                     'start_ts', 'end_ts', 'start_dt', 'end_dt', 'energy', 'total_charging_duration', 'total_session_duration', 'address']

    # add two more columns for logging in local timezone
    df_session = pd.DataFrame(session_data_list)[columns_selected]
    df_session.insert(df_session.columns.get_loc('endTime') + 1, 'startTime_local', df_session['startTime'])
    df_session.insert(df_session.columns.get_loc('endTime') + 2, 'endTime_local', df_session['endTime'])

    # convert startTime and endTime to UTC integer seconds
    
    df_session[['startTime','endTime']] = df_session[['startTime','endTime']].apply(lambda x : (x-self.earliest).apply(lambda y: str(int(y.total_seconds()))))

    # convert to local timezone and format the datetime
    df_session[['startTime_local','endTime_local']] = df_session[['startTime_local','endTime_local']].apply(lambda x : x.dt.tz_convert(self.local_timezone))\
                                                                                                     .apply(lambda x : x.dt.strftime(self.dt_format))

    # hash ID feilds
    df_session[['userID','stationID']] = df_session[['userID','stationID']].apply(lambda x : x.apply(self.encrypt))

    # rename the columns
    df_session.columns = columns_alias

    return df_session