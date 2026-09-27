import unittest
import pandas as pd
from lib.anomalies import find_anomalies, refresh_anomalies, version_history, COLUMNS


def session(sid='1', **changes):
    return dict(session_id=sid, station_id='station', port_no='1', user_id='user',
                **{**dict(start_ts=0, end_ts=3600, energy=1,
                          total_session_duration='01:00:00', total_charging_duration='01:00:00'), **changes})


class AnomalyTests(unittest.TestCase):
    def test_version_one_legacy_rules(self):
        s=pd.DataFrame([session(total_session_duration='25:00:00',total_charging_duration='13:00:00',energy=8)])
        out=find_anomalies(s,rules_version=1)
        self.assertEqual(len(out),3)
        self.assertEqual(set(out.ver),{1})

    def test_validation_and_usage_flags(self):
        cases=[
            ({'energy':-1},'Negative energy'),
            ({'energy':'bad'},'Missing or invalid energy'),
            ({'total_session_duration':'bad'},'Missing or invalid session duration'),
            ({'end_ts':-1},'End timestamp precedes start'),
            ({'end_ts':0,'energy':1},'Positive energy with zero elapsed time'),
            ({'total_charging_duration':'00:00:00','energy':1},'Positive energy with zero active charging time'),
            ({'end_ts':4000},'Session duration disagrees with timestamps by more than 60 seconds'),
            ({'total_charging_duration':'02:00:00'},'Active charging exceeds session duration by more than 60 seconds'),
            ({'energy':0},'Zero energy during a session of at least 5 minutes'),
            ({'end_ts':86400,'total_session_duration':'24:00:00'},'Noncharging occupancy at least 12 hours'),
        ]
        for values, expected in cases:
            with self.subTest(expected=expected):
                out=find_anomalies(pd.DataFrame([session(**values)]))
                self.assertIn(expected,set(out.anomaly_description))
                self.assertEqual(set(out.ver),{2})

    def test_active_power_not_hidden_by_idle(self):
        out=find_anomalies(pd.DataFrame([session(end_ts=36000,energy=10,total_session_duration='10:00:00')]))
        self.assertTrue(out.anomaly_description.str.startswith('Active-time').any())
        self.assertFalse(out.anomaly_description.str.startswith('Session-average').any())

    def test_boundaries_and_open_session(self):
        normal=session(end_ts=3660)
        self.assertTrue(find_anomalies(pd.DataFrame([normal])).empty)
        out=find_anomalies(pd.DataFrame([session(end_ts='',energy=0)]))
        self.assertFalse(out.anomaly_description.str.contains('Zero energy|Invalid end|disagrees').any())

    def test_neighbours_outside_selected_batch(self):
        context=pd.DataFrame([session('old'), session('overlap',start_ts=3500,end_ts=7100),
                              session('retry',start_ts=7200,end_ts=10800)])
        out=find_anomalies(context.iloc[1:],context=context)
        overlap=out[out.anomaly_description.str.startswith('Same-port')]
        self.assertEqual(overlap.session_id.tolist(),['overlap'])
        self.assertEqual(overlap.value.tolist(),[100])
        retry=out[out.anomaly_description.str.startswith('Same-driver')]
        self.assertEqual(retry.session_id.tolist(),['retry'])
        self.assertEqual(retry.value.tolist(),[100])
        self.assertNotIn('old',out.session_id.tolist())

    def test_overlap_tolerance_and_short_nested_interval(self):
        c=pd.DataFrame([session('old'),session('new',start_ts=3590,end_ts=7190)])
        self.assertFalse(find_anomalies(c).anomaly_description.str.startswith('Same-port').any())
        c=pd.DataFrame([session('old'),session('new',start_ts=100,end_ts=110,total_session_duration='00:00:10',total_charging_duration='00:00:10')])
        self.assertFalse(find_anomalies(c).anomaly_description.str.startswith('Same-port').any())

    def test_version_migration_preserves_history_and_replaces_revisited(self):
        previous=pd.DataFrame([['old','Legacy description','25:00:00','hh:mm:ss'],
                               ['new','Legacy description','25:00:00','hh:mm:ss']],columns=COLUMNS[:-1])
        s=pd.DataFrame([session('old'),session('new',energy=0)])
        out=refresh_anomalies(previous,s,['new'])
        old=out[out.session_id=='old']
        self.assertEqual(old.iloc[0].ver,1)
        self.assertEqual(old.iloc[0]['value'],'25:00:00')
        self.assertEqual(set(out[out.session_id=='new'].ver),{2})
        self.assertNotIn('Legacy description',out[out.session_id=='new'].anomaly_description.tolist())
        pd.testing.assert_frame_equal(out,refresh_anomalies(out,s,['new']))
        no_new=refresh_anomalies(previous,s,[])
        self.assertEqual(set(no_new.ver),{1})
        pd.testing.assert_frame_equal(no_new[COLUMNS[:-1]],previous)

    def test_corrected_session_removes_resolved_flag(self):
        s=pd.DataFrame([session()])
        old=find_anomalies(pd.DataFrame([session(energy=0)]))
        self.assertTrue(refresh_anomalies(old,s,['1']).empty)
        self.assertEqual(list(refresh_anomalies(old,s,['1']).columns),COLUMNS)

    def test_missing_user_not_retry(self):
        c=pd.DataFrame([session('a'),session('b',start_ts=3601,end_ts=7201)])
        c['user_id']=''
        self.assertFalse(find_anomalies(c).anomaly_description.str.startswith('Same-driver').any())

    def test_invalid_version_fails(self):
        old=pd.DataFrame([['1','x',1,'kWh','']],columns=COLUMNS)
        with self.assertRaises(ValueError): version_history(old)
