"""ChargePoint SOAP client using the Canadian production service."""
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
import time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zeep import Client
from zeep.cache import SqliteCache
from zeep.exceptions import Fault, TransportError
from zeep.helpers import serialize_object
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

WSDL_URL = 'https://webservices-ca.chargepoint.com/cp_api_5.1.wsdl'
SOAP_ENDPOINT = 'https://webservices-ca.chargepoint.com/webservices/chargepoint/services/5.1'
BINDING = '{urn:dictionary:com.chargepoint.webservices}chargepointservicesSOAP'
SESSION_COLUMNS = ['session_id', 'user_id', 'credential_id', 'station_id', 'port_no', 'start_ts', 'end_ts', 'start_dt', 'end_dt', 'energy', 'total_charging_duration', 'total_session_duration', 'address']
ALARM_COLUMNS = ['station_id', 'station_name', 'model', 'org_id', 'port_no', 'alarm_type', 'alarm_ts', 'alarm_dt']
STATION_COLUMNS = ['station_id', 'org_id', 'station_group', 'model', 'activation_dt', 'timezone_offset', 'address', 'manufacturer', 'station_name', 'description', 'port_no', 'reservable', 'status', 'level', 'time_stamp', 'mode', 'connector', 'voltage', 'current', 'power', 'estimated_cost', 'location_lat', 'location_long']

class ChargePointApiClient:
    def __init__(self, api_key, api_secret, *, cache_path=None):
        self.local_timezone = 'America/Vancouver'
        self.dt_format = '%Y-%m-%d %H:%M:%S'
        self.logger = logging.getLogger('Update')
        self.session = requests.Session()
        # GET retries are for WSDL retrieval; SOAP read retries are handled below.
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 502, 503, 504], allowed_methods={'GET'}, respect_retry_after_header=False)
        self.session.mount('https://', HTTPAdapter(max_retries=retry))
        cache_path = Path(cache_path) if cache_path else Path.home() / '.cache' / 'SFU-EVP' / 'wsdl.sqlite'
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            transport = Transport(session=self.session, cache=SqliteCache(str(cache_path), timeout=86400), timeout=30, operation_timeout=120)
            self.client = Client(WSDL_URL, transport=transport, wsse=UsernameToken(api_key, api_secret))
            # Do not use client.service: its WSDL default can point at the US host.
            self.serv_impl = self.client.create_service(BINDING, SOAP_ENDPOINT)
        except Exception:
            self.session.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def encrypt(self, obj, output_length=5):
        return hashlib.blake2b(str(obj).encode(), digest_size=output_length).hexdigest()

    def _call(self, operation, query):
        for attempt in range(3):
            try:
                return serialize_object(getattr(self.serv_impl, operation)(query))
            except (requests.Timeout, requests.ConnectionError, TransportError) as exc:
                if isinstance(exc, TransportError) and exc.status_code not in (429, 502, 503, 504):
                    raise RuntimeError(f'{operation}: HTTP {exc.status_code}') from exc
                if attempt == 2:
                    raise RuntimeError(f'{operation}: connection failed after 3 attempts') from exc
                self.logger.warning('%s: temporary connection failure; retrying', operation)
                time.sleep(2 ** attempt)
            except Fault as exc:
                if str(exc.code).split(':')[-1].split('}')[-1] in ('InvalidSecurity', 'FailedAuthentication'):
                    raise RuntimeError(
                        f'{operation}: Canadian ChargePoint API rejected the WS-Security credentials '
                        f'({exc.code}). Check [ChargePoint] api_key and secret in your private '
                        'configuration (default ~/.config/sfu-evp/config.ini; --config or '
                        'SFU_EVP_CONFIG may override it). Use the Canadian organization API '
                        'license key and generated API password, not the website login. '
                        'If they match the Canadian portal, confirm API access with ChargePoint support.'
                    ) from exc
                raise RuntimeError(f'{operation}: SOAP fault {exc.code}: {exc.message}') from exc

    def _pages(self, operation, query, field, flag):
        query = dict(query)
        start = int(query.get('startRecord', 1))
        rows, seen = [], set()
        while True:
            query['startRecord'] = start
            response = self._call(operation, query)
            code = str(response.get('responseCode'))
            if operation == 'getChargingSessionData' and code == '136':
                break
            if code != '100':
                raise RuntimeError(f'{operation}: API code {code}: {response.get("responseText", "No error description")}')
            page = response.get(field) or []
            more = str(response.get(flag) or 0).lower() in ('1', 'true')
            if not page:
                if more:
                    raise RuntimeError(f'{operation}: empty page with more results advertised')
                break
            signature = hashlib.sha256(repr(page).encode()).digest()
            if signature in seen:
                raise RuntimeError(f'{operation}: repeated page; refusing an endless download')
            seen.add(signature)
            rows.extend(page)
            self.logger.info('%s: fetched %d records', operation, len(rows))
            if not more:
                break
            start += len(page)
        return rows

    @staticmethod
    def _utc(value):
        stamp = pd.Timestamp(value)
        return (stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')).to_pydatetime()

    def _dates(self, values):
        dates = pd.to_datetime(values, utc=True, errors='raise', format='mixed')
        seconds = dates.map(lambda value: int(value.timestamp()) if pd.notna(value) else pd.NA).astype('Int64')
        local = dates.dt.tz_convert(self.local_timezone).dt.strftime(self.dt_format)
        return seconds, local

    def queryChargingSession(self, startTime, endTime=None):
        query = {'fromTimeStamp': self._utc(startTime), 'toTimeStamp': self._utc(endTime or datetime.now(timezone.utc))}
        rows = self._pages('getChargingSessionData', query, 'ChargingSessionData', 'MoreFlag')
        if not rows:
            return pd.DataFrame(columns=SESSION_COLUMNS)
        raw = pd.DataFrame(rows)
        for field in ['sessionID', 'stationID', 'startTime']:
            if field not in raw or raw[field].isna().any():
                raise ValueError(f'Session response missing required {field}')
        raw = raw.reindex(columns=['sessionID', 'userID', 'credentialID', 'stationID', 'portNumber', 'startTime', 'endTime', 'Energy', 'totalChargingDuration', 'totalSessionDuration', 'Address'])
        out = pd.DataFrame(index=raw.index)
        for source, target in [('sessionID','session_id'), ('credentialID','credential_id')]:
            out[target] = raw[source].astype('string').fillna('')
        for source, target in [('userID','user_id'), ('stationID','station_id')]:
            out[target] = raw[source].map(self.encrypt)
        out['port_no'] = raw.portNumber
        out['start_ts'], out['start_dt'] = self._dates(raw.startTime)
        out['end_ts'], out['end_dt'] = self._dates(raw.endTime)
        out['energy'] = pd.to_numeric(raw.Energy, errors='raise')
        out['total_charging_duration'] = raw.totalChargingDuration
        out['total_session_duration'] = raw.totalSessionDuration
        out['address'] = raw.Address
        return out[SESSION_COLUMNS].sort_values(['start_ts', 'session_id']).reset_index(drop=True)

    def getAlarms(self, startTime, endTime):
        rows = self._pages('getAlarms', {'startTime': self._utc(startTime), 'endTime': self._utc(endTime)}, 'Alarms', 'moreFlag')
        if not rows:
            return pd.DataFrame(columns=ALARM_COLUMNS)
        raw = pd.DataFrame(rows)
        for field in ['stationID', 'alarmType', 'alarmTime']:
            if field not in raw or raw[field].isna().any():
                raise ValueError(f'Alarm response missing required {field}')
        raw = raw.reindex(columns=['stationID','stationName','stationModel','orgID','portNumber','alarmType','alarmTime'])
        out = raw.rename(columns=dict(zip(raw.columns, ALARM_COLUMNS[:-1]))).copy()
        for col in ['station_id','org_id']:
            out[col] = out[col].map(self.encrypt)
        out['alarm_ts'], out['alarm_dt'] = self._dates(raw.alarmTime)
        return out[ALARM_COLUMNS].sort_values('alarm_ts').reset_index(drop=True)

    def getStations(self, searchQuery=None):
        rows = self._pages('getStations', searchQuery or {}, 'stationData', 'moreFlag')
        result = []
        fields = {'stationModel':'model', 'timezoneOffset':'timezone_offset', 'Address':'address', 'stationManufacturer':'manufacturer', 'stationName':'station_name', 'Description':'description'}
        port_fields = dict(zip(['portNumber','Reservable','Status','Level','timeStamp','Mode','Connector','Voltage','Current','Power','estimatedCost'], STATION_COLUMNS[10:21]))
        for station in rows:
            if not station.get('stationID'):
                raise ValueError('Station response missing stationID')
            base = {target: station.get(source) for source, target in fields.items()}
            base['station_id'] = self.encrypt(station['stationID'])
            base['org_id'] = self.encrypt(station['orgID']) if station.get('orgID') is not None else ''
            base['station_group'] = str(station.get('sgID') or '').replace(', ', ';')
            activation = pd.to_datetime(station.get('stationActivationDate'), utc=True)
            base['activation_dt'] = activation.tz_convert(self.local_timezone).strftime(self.dt_format) if pd.notna(activation) else ''
            for port in station.get('Port') or []:
                row = {**base, **{target: port.get(source) for source, target in port_fields.items()}}
                geo = port.get('Geo') or {}
                row.update(location_lat=geo.get('Lat'), location_long=geo.get('Long'))
                result.append(row)
        return pd.DataFrame(result, columns=STATION_COLUMNS)

    def getCPNInstances(self):
        return self.serv_impl.getCPNInstances()

    def getOrgsAndStationGroups(self, searchQuery=None):
        return self.serv_impl.getOrgsAndStationGroups(searchQuery or {})

    def getStationGroups(self, orgID):
        return self.serv_impl.getStationGroups(orgID)

    def getStationRights(self, searchQuery=None):
        return self.serv_impl.getStationRights(searchQuery or {})

    def getStationRightsProfile(self, sgID):
        return self.serv_impl.getStationRightsProfile(sgID)

    def getStationStatus(self, searchQuery=None):
        return self.serv_impl.getStationStatus(searchQuery or {})
