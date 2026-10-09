"""
Regression tests for daily-average aggregation of HRV and resting heart rate.

Shravan's requirement: average the valid measurements within each calendar day
first, then average across the days that have data, so every day carries equal
weight regardless of how many sleep sessions it contains. Missing days must not
count as zero.

ourasleep holds one row per sleep session, so a day with a nap has several rows.
These tests capture the SQL the helper actually emits and execute it against
synthetic rows in SQLite, so the query's own semantics are under test rather
than a reimplementation of them. Fixtures are confined to this file.
"""
import datetime as dt
import math
import sqlite3
import sys
import unittest

import pandas as pd

sys.path.insert(0, '/home/duo/sensorfabric-uh')
from ultrahuman.helper import Helper  # noqa: E402

START, END = dt.date(2026, 10, 1), dt.date(2026, 10, 7)


class _Capture:
    """Records the query and returns whatever frame the test supplies."""

    def __init__(self, df=None):
        self.df = df if df is not None else pd.DataFrame()
        self.last = None

    def execQuery(self, q):
        self.last = q
        return self.df


def helper(athena):
    h = object.__new__(Helper)
    h.participant_id = 'TEST-ONLY'
    h.start_date, h.end_date = START, END
    h.athena_mdh = athena
    return h


def emitted_sql(method):
    a = _Capture(pd.DataFrame({'avg_hrv': [1], 'avg_rhr': [1]}))
    getattr(helper(a), method)()
    return a.last


def run_sql_on(rows, sql):
    """Execute the helper's own SQL against synthetic ourasleep rows."""
    con = sqlite3.connect(':memory:')
    con.create_function('floor', 1, lambda v: None if v is None else math.floor(v))
    con.execute('create table ourasleep ('
                'participantidentifier text, day text, '
                'averagehrv real, lowestheartrate real)')
    con.executemany('insert into ourasleep values (?,?,?,?)', rows)
    value = con.execute(sql).fetchone()[0]
    con.close()
    return value


def row(day, hrv=None, rhr=None, pid='TEST-ONLY'):
    return (pid, day, hrv, rhr)


class SqlShape(unittest.TestCase):
    def test_both_queries_group_by_day_in_a_cte(self):
        for method, inner, outer in (
                ('hrvSummary', 'avg(averagehrv) as day_hrv', 'avg(day_hrv)'),
                ('heartRateSummary', 'avg(lowestheartrate) as day_rhr', 'avg(day_rhr)')):
            sql = emitted_sql(method)
            self.assertIn('with daily as', sql, method)
            self.assertIn('group by day', sql, method)
            self.assertIn(inner, sql, method)
            self.assertIn(outer, sql, method)
            self.assertIn('from daily', sql, method)

    def test_participant_filter_and_window_are_present(self):
        for method in ('hrvSummary', 'heartRateSummary'):
            sql = emitted_sql(method)
            self.assertIn("participantidentifier = 'TEST-ONLY'", sql, method)
            self.assertIn("day between '2026-10-01' and '2026-10-07'", sql, method)

    def test_null_rows_are_filtered_before_grouping(self):
        self.assertIn('averagehrv is not null', emitted_sql('hrvSummary'))
        self.assertIn('lowestheartrate is not null', emitted_sql('heartRateSummary'))


class EqualWeightPerDay(unittest.TestCase):
    """A day with two sessions must not outweigh a day with one."""

    ROWS = [
        row('2026-10-01', hrv=20, rhr=50),   # day 1, session A
        row('2026-10-01', hrv=40, rhr=70),   # day 1, session B (nap)
        row('2026-10-02', hrv=60, rhr=90),   # day 2, single session
    ]

    def test_hrv_is_day_weighted_not_session_weighted(self):
        got = run_sql_on(self.ROWS, emitted_sql('hrvSummary'))
        # day means 30 and 60 -> 45.  session mean would be 40.
        self.assertEqual(got, 45)
        self.assertNotEqual(got, 40, 'still session-weighted')

    def test_rhr_is_day_weighted_not_session_weighted(self):
        got = run_sql_on(self.ROWS, emitted_sql('heartRateSummary'))
        # day means 60 and 90 -> 75.  session mean would be 70.
        self.assertEqual(got, 75)
        self.assertNotEqual(got, 70, 'still session-weighted')

    def test_many_sessions_on_one_day_still_count_once(self):
        rows = [row('2026-10-01', hrv=10) for _ in range(9)]
        rows.append(row('2026-10-02', hrv=50))
        # day means 10 and 50 -> 30, despite 9:1 session imbalance
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 30)


class MissingDataIsNotZero(unittest.TestCase):
    def test_days_without_records_are_excluded_not_zeroed(self):
        rows = [row('2026-10-01', hrv=40), row('2026-10-07', hrv=60)]
        # two days with data out of seven -> 50, not 100/7
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 50)

    def test_null_measurement_drops_its_session(self):
        rows = [row('2026-10-01', hrv=None), row('2026-10-01', hrv=40),
                row('2026-10-02', hrv=60)]
        # day 1 mean is 40 from the one valid session -> (40+60)/2 = 50
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 50)

    def test_day_with_only_nulls_drops_out_entirely(self):
        rows = [row('2026-10-01', hrv=None), row('2026-10-02', hrv=60)]
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 60)

    def test_no_valid_rows_yields_null_which_becomes_no_data(self):
        rows = [row('2026-10-01', hrv=None)]
        self.assertIsNone(run_sql_on(rows, emitted_sql('hrvSummary')))
        # and the post-query guard turns that into None
        self.assertIsNone(helper(_Capture(
            pd.DataFrame({'avg_hrv': [None]}))).hrvSummary())
        self.assertIsNone(helper(_Capture(
            pd.DataFrame({'avg_rhr': [None]}))).heartRateSummary())

    def test_hrv_and_rhr_are_independent_when_one_is_null(self):
        rows = [row('2026-10-01', hrv=40, rhr=None),
                row('2026-10-02', hrv=None, rhr=70)]
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 40)
        self.assertEqual(run_sql_on(rows, emitted_sql('heartRateSummary')), 70)


class DateBoundaries(unittest.TestCase):
    def test_start_and_end_days_are_included(self):
        rows = [row('2026-10-01', hrv=30), row('2026-10-07', hrv=50)]
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 40)

    def test_days_outside_the_window_are_excluded(self):
        rows = [row('2026-09-30', hrv=1000),   # before window
                row('2026-10-03', hrv=40),
                row('2026-10-08', hrv=1000)]   # after window
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 40)

    def test_other_participants_are_excluded(self):
        rows = [row('2026-10-01', hrv=40),
                row('2026-10-01', hrv=1000, pid='SOMEONE-ELSE')]
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 40)


class FloorNotRound(unittest.TestCase):
    def test_result_is_floored(self):
        # day means 40 and 41 -> 40.5 -> floor 40
        rows = [row('2026-10-01', hrv=40), row('2026-10-02', hrv=41)]
        self.assertEqual(run_sql_on(rows, emitted_sql('hrvSummary')), 40)

    def test_return_shape_is_unchanged(self):
        self.assertEqual(helper(_Capture(pd.DataFrame({'avg_hrv': [43]}))).hrvSummary(),
                         {'avg_hrv': 43})
        self.assertEqual(helper(_Capture(pd.DataFrame({'avg_rhr': [67]}))).heartRateSummary(),
                         {'hr_counts': None, 'avg_rhr': 67})


class OtherMetricsUnaffected(unittest.TestCase):
    """Sleep and steps keep their own daily logic; BP and symptoms are untouched."""

    def test_sleep_still_groups_by_day_and_sums_sessions(self):
        a = _Capture(pd.DataFrame({'total_seconds': [1.0], 'avg_day_seconds': [1.0]}))
        helper(a).sleepSummary()
        self.assertIn('sum(totalsleepduration)', a.last)
        self.assertIn('group by day', a.last)
        self.assertIn('avg(day_seconds)', a.last)

    def test_steps_still_uses_daily_max_then_period_average(self):
        a = _Capture(pd.DataFrame({'avg_curr': [1], 'avg_prev': [1]}))
        helper(a).movementSummary()
        self.assertIn('MAX(steps)', a.last)
        self.assertIn('GROUP BY day', a.last)

    def test_blood_pressure_query_has_no_daily_grouping(self):
        a = _Capture(pd.DataFrame({
            'reading_date': [dt.date(2026, 10, 1)], 'systolic': [120.0],
            'diastolic': [80.0], 'week_type': ['current']}))
        helper(a).bloodPressure()
        self.assertNotIn('with daily as', a.last)
        self.assertIn('omronbloodpressure', a.last)

    def test_symptoms_query_unchanged(self):
        a = _Capture(pd.DataFrame({'symptom': ['x'], 'total_count': ['1'], 'days': ['1']}))
        helper(a).topSymptomsRecorded()
        self.assertNotIn('with daily as', a.last)
        self.assertIn('projectdevicedata', a.last)


if __name__ == '__main__':
    unittest.main(verbosity=2)
