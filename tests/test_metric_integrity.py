"""
Tests that the weekly report's metric functions read real query results and
never disguise a failure as missing data.

The Athena connection is replaced with a stub that returns a frame, so the
post-query logic under test runs exactly as it does in production. No
participant data is used.
"""
import datetime as dt
import sys
import unittest

import pandas as pd

sys.path.insert(0, '/home/duo/sensorfabric-uh')
from ultrahuman.helper import Helper  # noqa: E402

WINDOW = (dt.date(2026, 10, 1), dt.date(2026, 10, 7))


class _Returns:
    def __init__(self, df):
        self.df = df
        self.last = None

    def execQuery(self, q):
        self.last = q
        return self.df


class _Raises:
    def __init__(self, exc):
        self.exc = exc

    def execQuery(self, q):
        raise self.exc


def helper(athena):
    h = object.__new__(Helper)
    h.participant_id = 'TEST-ONLY'
    h.start_date, h.end_date = WINDOW
    h.athena_mdh = athena
    return h


def bp_frame(rows):
    return pd.DataFrame({
        'reading_date': [r[0] for r in rows],
        'systolic': [r[1] for r in rows],
        'diastolic': [r[2] for r in rows],
        'week_type': ['current'] * len(rows),
    })


class QueryFailuresMustSurface(unittest.TestCase):
    """A permission error or Athena failure must never render as No Data."""

    METRICS = ['sleepSummary', 'hrvSummary', 'heartRateSummary',
               'movementSummary', 'bloodPressure', 'topSymptomsRecorded']

    def test_exception_propagates_for_every_metric(self):
        for name in self.METRICS:
            h = helper(_Raises(RuntimeError('AccessDenied: simulated')))
            with self.assertRaises(RuntimeError, msg='%s swallowed a failure' % name):
                getattr(h, name)()

    def test_schema_drift_in_steps_is_not_reported_as_no_data(self):
        # 'avg_prev' missing entirely -> a real problem, must not become None.
        h = helper(_Returns(pd.DataFrame({'avg_curr': [7000]})))
        with self.assertRaises(KeyError):
            h.movementSummary()


class GenuineMissingDataIsNoData(unittest.TestCase):
    """An empty or NULL result set is real missing data and returns None."""

    def test_sleep(self):
        self.assertIsNone(helper(_Returns(pd.DataFrame(
            {'total_seconds': [None], 'avg_day_seconds': [None]}))).sleepSummary())
        self.assertIsNone(helper(_Returns(pd.DataFrame(
            {'total_seconds': [], 'avg_day_seconds': []}))).sleepSummary())

    def test_hrv(self):
        self.assertIsNone(helper(_Returns(pd.DataFrame({'avg_hrv': [None]}))).hrvSummary())

    def test_resting_heart_rate(self):
        self.assertIsNone(helper(_Returns(pd.DataFrame({'avg_rhr': [None]}))).heartRateSummary())

    def test_steps(self):
        self.assertIsNone(helper(_Returns(pd.DataFrame(
            {'avg_curr': [None], 'avg_prev': [None]}))).movementSummary())

    def test_blood_pressure(self):
        self.assertIsNone(helper(_Returns(bp_frame([]))).bloodPressure())


class HighBloodPressureIsNeverDropped(unittest.TestCase):
    """A reading high on one value with the other NULL must still be surfaced."""

    def test_high_systolic_with_missing_diastolic_is_counted_and_listed(self):
        r = helper(_Returns(bp_frame([
            (dt.date(2026, 10, 2), 152.0, None),
            (dt.date(2026, 10, 4), 118.0, 76.0),
        ]))).bloodPressure()
        self.assertEqual(r['above_threshold_counts'], 1)
        self.assertTrue(r['is_high'])
        self.assertEqual(len(r['high_readings']), 1)
        self.assertEqual(r['high_readings'][0]['systolic'], 152)
        self.assertIsNone(r['high_readings'][0]['diastolic'])

    def test_high_diastolic_with_missing_systolic_is_counted(self):
        r = helper(_Returns(bp_frame([(dt.date(2026, 10, 2), None, 97.0)]))).bloodPressure()
        self.assertEqual(r['above_threshold_counts'], 1)
        self.assertIsNone(r['high_readings'][0]['systolic'])
        self.assertEqual(r['high_readings'][0]['diastolic'], 97)

    def test_count_and_list_never_disagree(self):
        r = helper(_Returns(bp_frame([
            (dt.date(2026, 10, 1), 152.0, None),
            (dt.date(2026, 10, 2), None, 97.0),
            (dt.date(2026, 10, 3), 145.0, 92.0),
            (dt.date(2026, 10, 4), 118.0, 76.0),
        ]))).bloodPressure()
        self.assertEqual(r['above_threshold_counts'], len(r['high_readings']))
        self.assertEqual(r['above_threshold_counts'], 3)

    def test_normal_readings_with_nulls_are_not_flagged(self):
        r = helper(_Returns(bp_frame([
            (dt.date(2026, 10, 1), 118.0, None),
            (dt.date(2026, 10, 2), None, 76.0),
            (dt.date(2026, 10, 3), None, None),
        ]))).bloodPressure()
        self.assertEqual(r['above_threshold_counts'], 0)
        self.assertFalse(r['is_high'])
        self.assertEqual(r['high_readings'], [])

    def test_threshold_boundary_is_strictly_greater(self):
        r = helper(_Returns(bp_frame([(dt.date(2026, 10, 1), 140.0, 90.0)]))).bloodPressure()
        self.assertEqual(r['above_threshold_counts'], 0)


class QueriesTargetTheRightSourceAndWindow(unittest.TestCase):
    CASES = [
        ('sleepSummary', {'total_seconds': [1.0], 'avg_day_seconds': [1.0]},
         ['ourasleep', 'totalsleepduration', 'group by day',
          "participantidentifier = 'TEST-ONLY'",
          "day between '2026-10-01' and '2026-10-07'"]),
        ('hrvSummary', {'avg_hrv': [40]},
         ['ourasleep', 'averagehrv', "participantidentifier = 'TEST-ONLY'",
          "day between '2026-10-01' and '2026-10-07'"]),
        ('heartRateSummary', {'avg_rhr': [60]},
         ['ourasleep', 'lowestheartrate', "participantidentifier = 'TEST-ONLY'",
          "day between '2026-10-01' and '2026-10-07'"]),
        ('movementSummary', {'avg_curr': [1], 'avg_prev': [1]},
         ['ouradailyactivity', 'MAX(steps)', "participantidentifier = 'TEST-ONLY'",
          "BETWEEN '2026-09-24' AND '2026-09-30'"]),
    ]

    def test_sources_and_filters(self):
        for name, frame, needles in self.CASES:
            a = _Returns(pd.DataFrame(frame))
            getattr(helper(a), name)()
            for n in needles:
                self.assertIn(n, a.last, '%s query missing %r' % (name, n))

    def test_blood_pressure_query(self):
        a = _Returns(bp_frame([(dt.date(2026, 10, 1), 120.0, 80.0)]))
        helper(a).bloodPressure()
        for n in ['omronbloodpressure', "participantIdentifier = 'TEST-ONLY'",
                  "date('2026-10-01') and date('2026-10-07')",
                  "date('2026-09-24') and date('2026-09-30')"]:
            self.assertIn(n, a.last)

    def test_symptoms_query(self):
        a = _Returns(pd.DataFrame({'symptom': ['back_pain'], 'total_count': [3], 'days': [2]}))
        helper(a).topSymptomsRecorded()
        for n in ['projectdevicedata', "participantidentifier = 'TEST-ONLY'",
                  "date('2026-10-01')", "date('2026-10-07')", "type = 'symptom'"]:
            self.assertIn(n, a.last)


class UnitsAndCalculations(unittest.TestCase):
    def test_sleep_seconds_to_hours(self):
        # 7 nights, 193740 s total
        r = helper(_Returns(pd.DataFrame(
            {'total_seconds': [193740.0], 'avg_day_seconds': [193740 / 7]}))).sleepSummary()
        self.assertEqual(r['hours'], 54)                 # int(round(53.8167))
        self.assertEqual(r['average_per_night'], 7.7)    # round(7.6881, 1)

    def test_hrv_and_rhr_are_passed_through_as_ints(self):
        self.assertEqual(helper(_Returns(pd.DataFrame({'avg_hrv': [36]}))).hrvSummary(),
                         {'avg_hrv': 36})
        self.assertEqual(helper(_Returns(pd.DataFrame({'avg_rhr': [58]}))).heartRateSummary(),
                         {'hr_counts': None, 'avg_rhr': 58})

    def test_steps_trend_is_current_minus_previous(self):
        r = helper(_Returns(pd.DataFrame({'avg_curr': [7431], 'avg_prev': [6919]}))).movementSummary()
        self.assertEqual(r['average_steps_int'], '7,431')
        self.assertEqual(r['trend'], '512')

    def test_negative_trend_is_formatted_without_a_stray_comma(self):
        r = helper(_Returns(pd.DataFrame({'avg_curr': [6204], 'avg_prev': [6716]}))).movementSummary()
        self.assertEqual(r['trend'], '-512')
        self.assertNotIn('-,', r['trend'])

    def test_symptoms_shape_matches_template_keys(self):
        r = helper(_Returns(pd.DataFrame(
            {'symptom': ['back_pain'], 'total_count': [4], 'days': [3]}))).topSymptomsRecorded()
        self.assertEqual(r, [{'name': 'Back pain', 'count': 4, 'days': 3}])

    def test_symptom_counts_are_ints_even_when_athena_returns_strings(self):
        # Athena returns count(*) and cardinality() as strings. The template
        # compares days against the integer 1 to choose "day" vs "days", so a
        # string made every symptom render "1 days".
        r = helper(_Returns(pd.DataFrame(
            {'symptom': ['sore_nipples'], 'total_count': ['1'], 'days': ['1']}
        ))).topSymptomsRecorded()
        self.assertEqual(r[0]['count'], 1)
        self.assertEqual(r[0]['days'], 1)
        self.assertIsInstance(r[0]['count'], int)
        self.assertIsInstance(r[0]['days'], int)
        self.assertTrue(r[0]['days'] == 1, 'singular branch must be reachable')

    def test_symptom_singular_and_plural_render_correctly(self):
        from jinja2 import Environment, FileSystemLoader
        env = Environment(loader=FileSystemLoader(
            '/home/duo/sensorfabric-uh/ultrahuman/templates'))
        tpl = env.get_template('reportv2.html')
        rows = helper(_Returns(pd.DataFrame(
            {'symptom': ['sore_nipples', 'fatigue'],
             'total_count': ['1', '5'], 'days': ['1', '5']}
        ))).topSymptomsRecorded()
        html = tpl.render(
            ringwear=None, temp=None, weight=None, weeks_enrolled=24,
            current_pregnancy_week=24, surveys_completed=3, symptoms=rows,
            sleep=None, hrv=None, hr=None, movement=None, bp=None,
            blood_pressure_enabled=True, heart_rate_enabled=True,
            temperature_enabled=True, sleep_enabled=True, weight_enabled=True,
            movement_enabled=True, start_str='October 01',
            end_str='October 07, 2026')
        import re
        i = html.find('Top Symptoms')
        items = [' '.join(re.sub(r'<[^>]+>', ' ', m.group(1)).split())
                 for m in re.finditer(r'<div class="metric-item">(.*?)</div>',
                                      html[i:i + 2600], re.S)]
        self.assertIn('1 day Sore nipples', items)
        self.assertIn('5 days Fatigue', items)
        self.assertNotIn('1 days Sore nipples', items)


class NoMockDataInProductionMode(unittest.TestCase):
    def test_debug_outputs_requires_template_mode_present(self):
        import os
        self.assertNotEqual(os.getenv('TEMPLATE_MODE', 'PRODUCTION'), 'PRESENT')

    def test_production_path_uses_the_stubbed_query_not_a_fixture(self):
        # _debugOutputs() would return avg_hrv 48; the query returns 41.
        r = helper(_Returns(pd.DataFrame({'avg_hrv': [41]}))).hrvSummary()
        self.assertEqual(r['avg_hrv'], 41)


if __name__ == '__main__':
    unittest.main(verbosity=2)
