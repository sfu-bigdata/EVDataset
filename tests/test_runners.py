import argparse
import configparser
from datetime import datetime, timezone
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import pandas as pd
import requests
from lib.ChargePointApiClient import ChargePointApiClient, SOAP_ENDPOINT, SESSION_COLUMNS, ALARM_COLUMNS, STATION_COLUMNS
from lib.ChargePointDatasetUtils import read_csv, save_csv, load_config, ROOT
# CLI filenames contain hyphens, so load them by file path for testing.
import importlib.util

def load_runner(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

download = load_runner('download_chargepoint_data', 'download_chargepoint-data.py')
refresh = load_runner('refresh_public_dataset', 'refresh_public-dataset.py')
merge_records = download.merge_records
start_time = download.start_time
query_session_for_id = download.query_session_for_id
run = download.run
upload_files = refresh.upload_files
upload_main = refresh.main
from lib.anomalies import find_anomalies

class Tests(unittest.TestCase):
    def api(self, responses):
        api = ChargePointApiClient.__new__(ChargePointApiClient)
        api.local_timezone = 'America/Vancouver'
        api.dt_format = '%Y-%m-%d %H:%M:%S'
        api.logger = Mock()
        api._call = Mock(side_effect=responses)
        return api

    def test_private_config_selection_and_data_root(self):
        with tempfile.TemporaryDirectory() as folder:
            home = Path(folder)
            default = home/'.config/sfu-evp/config.ini'
            default.parent.mkdir(parents=True)
            default.write_text('[ChargePoint]\napi_key = dummy\nsecret = 50%literal\n')
            alternate = home/'other.ini'
            alternate.write_text('[Paths]\nroot = outputs\n')
            with patch('pathlib.Path.home', return_value=home), patch.dict('os.environ', {}, clear=True):
                config, data_root = load_config()
                self.assertEqual(config.get('ChargePoint','secret'),'50%literal')
                self.assertEqual(data_root,ROOT)
                with patch.dict('os.environ', {'SFU_EVP_CONFIG':str(alternate)}):
                    self.assertEqual(load_config()[1],(home/'outputs').resolve())
                    self.assertEqual(load_config(default)[1],ROOT)
                default.unlink()
                with self.assertRaisesRegex(ValueError,'config.example.ini'):
                    load_config()

    def test_canadian_binding(self):
        with tempfile.TemporaryDirectory() as folder, patch('lib.ChargePointApiClient.Client') as client:
            with ChargePointApiClient('test', 'test', cache_path=Path(folder)/'cache'):
                self.assertIn('webservices-ca.', client.call_args.args[0])
                self.assertEqual(client.return_value.create_service.call_args.args[1], SOAP_ENDPOINT)
                self.assertEqual(client.call_args.kwargs['transport'].operation_timeout, 120)

    def test_pagination_and_no_data(self):
        api = self.api([{'responseCode':'100','MoreFlag':1,'ChargingSessionData':[{'id':1}, {'id':2}]}, {'responseCode':'100','MoreFlag':0,'ChargingSessionData':[{'id':3}]}])
        calls = []
        original = api._call.side_effect
        iterator = iter(original)
        api._call.side_effect = lambda op,q: (calls.append(dict(q)), next(iterator))[1]
        self.assertEqual(len(api._pages('getChargingSessionData', {}, 'ChargingSessionData', 'MoreFlag')),3)
        self.assertEqual([q['startRecord'] for q in calls], [1,3])
        api = self.api([{'responseCode':'136'}])
        self.assertEqual(list(api.queryChargingSession(datetime.now()).columns),SESSION_COLUMNS)

    def test_broken_pagination_and_api_error(self):
        for responses in [
            [{'responseCode':'100','moreFlag':1,'Alarms':[]}],
            [{'responseCode':'100','moreFlag':1,'Alarms':[{'id':1}]}]*2,
            [{'responseCode':'101','responseText':'Invalid key'}],
        ]:
            with self.assertRaises(RuntimeError):
                self.api(responses)._pages('getAlarms', {}, 'Alarms', 'moreFlag')

    def test_retry_is_bounded(self):
        api = self.api([])
        del api._call
        api.serv_impl = Mock()
        api.serv_impl.getAlarms.side_effect = requests.Timeout()
        with patch('lib.ChargePointApiClient.time.sleep'), self.assertRaises(RuntimeError):
            api._call('getAlarms', {})
        self.assertEqual(api.serv_impl.getAlarms.call_count,3)

    def test_session_conversion_and_missing_end(self):
        row = dict(sessionID=12, stationID='1:2', userID='u', startTime=datetime(2024,1,1,tzinfo=timezone.utc), endTime=None, Energy=1)
        api = self.api([{'responseCode':'100','ChargingSessionData':[row]}])
        frame = api.queryChargingSession(datetime(2024,1,1))
        self.assertEqual(frame.iloc[0].start_ts,1704067200)
        self.assertTrue(pd.isna(frame.iloc[0].end_ts))
        self.assertEqual(frame.iloc[0].start_dt,'2023-12-31 16:00:00')
        self.assertEqual(frame.iloc[0].station_id,api.encrypt('1:2'))

    def test_security_fault_is_actionable_and_not_retried(self):
        from zeep.exceptions import Fault
        api = self.api([])
        del api._call
        api.serv_impl = Mock()
        api.serv_impl.getAlarms.side_effect = Fault('sensitive server detail', code='wsse:InvalidSecurity')
        with self.assertRaisesRegex(RuntimeError, 'Canadian ChargePoint API rejected') as caught:
            api._call('getAlarms', {})
        self.assertNotIn('sensitive server detail', str(caught.exception))
        self.assertIn('SFU_EVP_CONFIG', str(caught.exception))
        self.assertEqual(api.serv_impl.getAlarms.call_count, 1)

    def test_stations_variable_ports(self):
        rows = [{'stationID':'s1','Port':[{'portNumber':'1','Geo':{'Lat':49,'Long':-123}}]}, {'stationID':'s2','Port':[{'portNumber':str(i)} for i in range(3)]}]
        frame = self.api([{'responseCode':'100','stationData':rows}]).getStations()
        self.assertEqual(len(frame),4)
        self.assertEqual(list(frame.columns),STATION_COLUMNS)
        self.assertEqual(frame.iloc[0].location_lat,49)

    def test_merge_corrections_and_alarm_collisions(self):
        old = pd.DataFrame([{'session_id':'12','start_ts':2,'end_ts':4,'energy':1}])
        new = pd.DataFrame([{'session_id':'12','start_ts':2,'end_ts':5,'energy':2}])
        self.assertEqual(merge_records(old,new,['session_id'],['start_ts']).iloc[0].energy,2)
        old = pd.DataFrame([dict(station_id='a',port_no=1.0,alarm_type='x',alarm_ts=2)])
        new = pd.DataFrame([dict(station_id='a',port_no='1',alarm_type='x',alarm_ts=2),dict(station_id='b',port_no='1',alarm_type='x',alarm_ts=2)])
        self.assertEqual(len(merge_records(old,new,['station_id','port_no','alarm_type','alarm_ts'],['alarm_ts'])),2)

    def test_watermark_unsorted_and_unfinished(self):
        frame = pd.DataFrame({'start_ts':[200,100], 'end_ts':[300,150]})
        self.assertEqual(start_time(frame,'start_ts',0).timestamp(),200)
        frame.loc[1,'end_ts'] = float('nan')
        self.assertEqual(start_time(frame,'start_ts',0).timestamp(),100)
        self.assertEqual(start_time(frame,'start_ts',0).tzinfo,timezone.utc)

    def test_matching(self):
        sessions = pd.DataFrame([dict(session_id='12',station_id='a',port_no=1,start_ts=0,end_ts=10)])
        alarms = pd.DataFrame([dict(station_id='a',port_no='1',alarm_ts=5),dict(station_id='b',port_no='1',alarm_ts=5)])
        self.assertEqual(query_session_for_id(alarms,sessions).session_id.tolist(),['12',''])

    def test_atomic_unchanged_and_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'data.csv'
            frame = pd.DataFrame({'session_id':['001'],'x':[1]})
            save_csv(frame,path)
            before = path.stat().st_mtime_ns
            self.assertFalse(save_csv(frame,path))
            self.assertEqual(path.stat().st_mtime_ns,before)
            self.assertEqual(read_csv(path).session_id.iloc[0],'001')
            with patch('os.replace',side_effect=OSError('disk error')), self.assertRaises(OSError):
                save_csv(pd.DataFrame({'x':[2]}),path)
            self.assertEqual(read_csv(path).session_id.iloc[0],'001')
            self.assertEqual(len(list(Path(folder).iterdir())),1)

    def test_vector_anomalies(self):
        frame = pd.DataFrame([dict(session_id='1',total_session_duration='25:00:00',total_charging_duration='13:00:00',start_ts=0,end_ts=3600,energy=8)])
        self.assertEqual(len(find_anomalies(frame, rules_version=1)),3)

    def test_two_runs_and_empty_first_run(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            config=configparser.ConfigParser()
            config['Paths']={key+'_data_path': key+'.csv' for key in ['session','station','alarm','anomaly']}
            api=Mock()
            api.queryChargingSession.return_value=pd.DataFrame(columns=SESSION_COLUMNS)
            api.getAlarms.return_value=pd.DataFrame(columns=ALARM_COLUMNS)
            api.getStations.return_value=pd.DataFrame(columns=STATION_COLUMNS)
            args=argparse.Namespace(since=None,overlap_days=7)
            run(api,config,root,args,Mock())
            before={p.name:p.stat().st_mtime_ns for p in root.glob('*.csv')}
            run(api,config,root,args,Mock())
            self.assertEqual(before,{p.name:p.stat().st_mtime_ns for p in root.glob('*.csv')})

    def test_populated_repeat_run_and_correction(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = configparser.ConfigParser()
            config['Paths'] = {key+'_data_path': key+'.csv' for key in ['session','station','alarm','anomaly']}
            api = self.api([{'responseCode':'100','ChargingSessionData':[dict(sessionID=12, userID='u', stationID='a', portNumber='1', startTime=datetime(2024,1,1,tzinfo=timezone.utc),endTime=datetime(2024,1,1,1,tzinfo=timezone.utc),Energy=8,totalSessionDuration='01:00:00',totalChargingDuration='01:00:00')]}])
            sessions = api.queryChargingSession(datetime(2024,1,1))
            fake = Mock()
            fake.queryChargingSession.return_value = sessions
            fake.getAlarms.return_value = pd.DataFrame(columns=ALARM_COLUMNS)
            fake.getStations.return_value = pd.DataFrame(columns=STATION_COLUMNS)
            args = argparse.Namespace(since=None,overlap_days=7)
            run(fake,config,root,args,Mock())
            before = {p.name:p.stat().st_mtime_ns for p in root.glob('*.csv')}
            run(fake,config,root,args,Mock())
            self.assertEqual(before,{p.name:p.stat().st_mtime_ns for p in root.glob('*.csv')})
            self.assertEqual(len(read_csv(root/'anomaly.csv')),2)
            sessions.loc[0,'energy']=2
            run(fake,config,root,args,Mock())
            self.assertEqual(len(read_csv(root/'anomaly.csv')),0)
            self.assertEqual(read_csv(root/'session.csv').energy.iloc[0],2)

    def test_upload_client_without_context_manager_is_closed(self):
        for outcome, expected in [(None, 0), (OSError('upload failure'), 1), (KeyboardInterrupt(), 130)]:
            with self.subTest(outcome=type(outcome).__name__), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                (root/'data').mkdir()
                (root/'data'/'a.csv').write_text('a')
                config = root/'config.ini'
                config.write_text(f'[DataPort]\napi_key = test\nsecret = test\n[Paths]\nroot = {root}\n')
                # Plain Mock has no context-manager magic methods, like the S3 client.
                client = Mock(spec=['upload_file', 'close'])
                client.upload_file.side_effect = outcome
                with patch('boto3.client', return_value=client), patch.object(refresh, 'get_logger', return_value=Mock()), patch('builtins.print'):
                    self.assertEqual(upload_main(['--config', str(config)]), expected)
                client.upload_file.assert_called_once()
                client.close.assert_called_once()

    def test_upload_failure_and_dry_run(self):
        client=Mock()
        client.upload_file.side_effect=[OSError('failure'),None]
        self.assertEqual(upload_files(client,[Path('a'),Path('b')],Mock()),1)
        self.assertEqual(client.upload_file.call_count,2)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); (root/'data').mkdir(); (root/'data'/'a.csv').write_text('a')
            (root/'config.ini').write_text(f'[DataPort]\n[Paths]\nroot = {root}\n')
            with patch('boto3.client') as client, patch('builtins.print'):
                self.assertEqual(upload_main(['--config',str(root/'config.ini'),'--dry-run']),0)
                client.assert_not_called()

if __name__ == '__main__':
    unittest.main()
