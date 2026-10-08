"""
Tests for the weekly report window produced by uh_jwt_coordinator.lambda_handler.

The coordinator fires SUN 07:00 UTC == SUN 00:00 America/Phoenix (UTC-7, no DST),
so at run time the last completed calendar day is Saturday. The report must cover
the 7 completed days ending Saturday.
"""
import datetime as dt
import sys
import unittest
from unittest import mock

sys.path.insert(0, '/home/duo/sensorfabric-uh')


def window_for(today):
    """Replicate the coordinator's auto-generated window for a given 'today'."""
    import ultrahuman.uh_jwt_coordinator as C
    captured = {}

    class FakeDate(dt.date):
        @classmethod
        def today(cls):
            return today

    def fake_start(self, start_date, end_date, participant_id=None):
        captured['start'] = start_date
        captured['end'] = end_date
        return {'success': True, 'participants_processed': 0}

    with mock.patch.object(C, 'get_secret', return_value={'REPORT_SECRET': 'x'}), \
         mock.patch.object(C.datetime, 'date', FakeDate), \
         mock.patch.object(C.UltrahumanJWTCoordinator, '__init__', lambda self, config: None), \
         mock.patch.object(C.UltrahumanJWTCoordinator, 'start_jwt_generation', fake_start):
        C.lambda_handler({}, None)
    return captured['start'], captured['end']


def explicit_window(payload):
    import ultrahuman.uh_jwt_coordinator as C
    captured = {}

    def fake_start(self, start_date, end_date, participant_id=None):
        captured['start'] = start_date
        captured['end'] = end_date
        return {'success': True, 'participants_processed': 0}

    with mock.patch.object(C, 'get_secret', return_value={'REPORT_SECRET': 'x'}), \
         mock.patch.object(C.UltrahumanJWTCoordinator, '__init__', lambda self, config: None), \
         mock.patch.object(C.UltrahumanJWTCoordinator, 'start_jwt_generation', fake_start):
        C.lambda_handler(payload, None)
    return captured['start'], captured['end']


def inclusive_days(start, end):
    s = dt.date.fromisoformat(start)
    e = dt.date.fromisoformat(end)
    return (e - s).days + 1


class SundayRunWindow(unittest.TestCase):
    SUNDAYS = [dt.date(2026, 10, 11), dt.date(2026, 10, 18),
               dt.date(2026, 1, 4), dt.date(2027, 2, 28)]

    def test_sunday_window_is_exactly_seven_inclusive_days(self):
        for sun in self.SUNDAYS:
            s, e = window_for(sun)
            self.assertEqual(inclusive_days(s, e), 7,
                             "run %s produced %s..%s" % (sun, s, e))

    def test_end_date_is_yesterday_a_saturday(self):
        for sun in self.SUNDAYS:
            s, e = window_for(sun)
            end = dt.date.fromisoformat(e)
            self.assertEqual(end, sun - dt.timedelta(days=1))
            self.assertEqual(end.strftime('%A'), 'Saturday')

    def test_start_date_is_today_minus_seven(self):
        for sun in self.SUNDAYS:
            s, e = window_for(sun)
            self.assertEqual(dt.date.fromisoformat(s), sun - dt.timedelta(days=7))

    def test_window_excludes_the_in_progress_day(self):
        for sun in self.SUNDAYS:
            s, e = window_for(sun)
            self.assertLess(dt.date.fromisoformat(e), sun,
                            "end_date must not include the run day")

    def test_known_example(self):
        s, e = window_for(dt.date(2026, 10, 11))
        self.assertEqual((s, e), ('2026-10-04', '2026-10-10'))


class ConsecutiveWeeks(unittest.TestCase):
    def test_consecutive_runs_are_contiguous_without_overlap_or_gap(self):
        runs = [dt.date(2026, 10, 4) + dt.timedelta(days=7 * i) for i in range(6)]
        windows = [window_for(r) for r in runs]
        for (s1, e1), (s2, e2) in zip(windows, windows[1:]):
            prev_end = dt.date.fromisoformat(e1)
            next_start = dt.date.fromisoformat(s2)
            self.assertEqual(next_start, prev_end + dt.timedelta(days=1),
                             "gap/overlap between %s..%s and %s..%s" % (s1, e1, s2, e2))

    def test_no_day_appears_in_two_consecutive_reports(self):
        runs = [dt.date(2026, 10, 4) + dt.timedelta(days=7 * i) for i in range(6)]
        seen = set()
        for r in runs:
            s, e = window_for(r)
            d = dt.date.fromisoformat(s)
            end = dt.date.fromisoformat(e)
            while d <= end:
                self.assertNotIn(d, seen, "%s counted in two reports" % d)
                seen.add(d)
                d += dt.timedelta(days=1)
        self.assertEqual(len(seen), 7 * len(runs))


class PreviousPeriodAlignment(unittest.TestCase):
    """The two previous-period conventions in helper.py must agree and not overlap."""

    def test_conventions_agree_and_abut(self):
        s, e = window_for(dt.date(2026, 10, 11))
        start = dt.date.fromisoformat(s)
        end = dt.date.fromisoformat(e)

        # bloodPressure / weightSummary: fixed -7 shift on both ends
        bp_prev = (start - dt.timedelta(days=7), end - dt.timedelta(days=7))
        # movementSummary: period-based, abutting
        period = (end - start).days + 1
        mv_prev_end = start - dt.timedelta(days=1)
        mv_prev = (mv_prev_end - dt.timedelta(days=period - 1), mv_prev_end)

        self.assertEqual(bp_prev, mv_prev, "previous-period conventions disagree")
        self.assertLess(bp_prev[1], start, "previous period overlaps current")
        self.assertEqual((bp_prev[1] - bp_prev[0]).days + 1, 7)


class ExplicitDatesPreserved(unittest.TestCase):
    def test_both_dates_passed_through_untouched(self):
        s, e = explicit_window({'start_date': '2025-03-01', 'end_date': '2025-03-09'})
        self.assertEqual((s, e), ('2025-03-01', '2025-03-09'))

    def test_backfill_of_arbitrary_length_is_not_rewritten(self):
        s, e = explicit_window({'start_date': '2025-01-01', 'end_date': '2025-01-31'})
        self.assertEqual(inclusive_days(s, e), 31)


class DisplayMatchesQueriedDates(unittest.TestCase):
    """uh_jwt_worker builds the header strings from the same dates it queries."""

    def test_header_strings_derive_from_the_queried_window(self):
        s, e = window_for(dt.date(2026, 10, 11))
        start_obj = dt.datetime.strptime(s, '%Y-%m-%d').date()
        end_obj = dt.datetime.strptime(e, '%Y-%m-%d').date()
        # Mirrors uh_jwt_worker.py:234-235
        start_str = start_obj.strftime("%B %d")
        end_str = end_obj.strftime("%B %d, %Y")
        self.assertEqual(start_str, 'October 04')
        self.assertEqual(end_str, 'October 10, 2026')
        # The displayed span must equal the queried span.
        self.assertEqual(inclusive_days(start_obj.isoformat(), end_obj.isoformat()), 7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
