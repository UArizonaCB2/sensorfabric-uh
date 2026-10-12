"""
Tests for weekly report participant selection via the MDH segment.

The report audience is the MDH "Active participants" segment, queried with
getAllParticipants(queryParam={'segmentID': ...}). MDH owns the rules.

Selection must fail closed. Every failure mode here asserts that an exception
is raised and that no participant list is returned, because the alternative --
falling back to all enrolled participants -- would send clinical reports to
people the study has excluded.

All participant dicts are fixtures local to this file.
"""
import sys
import unittest
from unittest import mock

sys.path.insert(0, '/home/duo/sensorfabric-uh')
import ultrahuman.uh_jwt_coordinator as C  # noqa: E402
from ultrahuman.error_handling import NonRetryableError  # noqa: E402

GUID = '5be2dbab-8328-47ab-a6f0-6ad71dc266b0'
KEY = C.ACTIVE_SEGMENT_ID_KEY


def member(n):
    return {'participantIdentifier': 'BB-0000-%04d' % n,
            'email': 'p%d@example.invalid' % n,
            'enrolledDate': '2026-01-01',
            'enrolled': True}


def coordinator(config=None, segment_members=None, project_total=778,
                total_override=None, raises=None):
    """Build a coordinator with a stubbed MDH client."""
    c = object.__new__(C.UltrahumanJWTCoordinator)
    c._UltrahumanJWTCoordinator__config = config if config is not None else {}
    c.mdh = mock.Mock()
    c.mdh.project_id = 'proj-123'

    if raises is not None:
        c.mdh.getAllParticipants.side_effect = raises
    else:
        members = segment_members if segment_members is not None else []
        total = total_override if total_override is not None else len(members)
        c.mdh.getAllParticipants.return_value = {
            'participants': members, 'totalParticipants': total}
    c.mdh.makeGetRequests.return_value = {'totalParticipants': project_total,
                                          'participants': []}
    return c


class ValidMembership(unittest.TestCase):
    def test_returns_the_segment_members(self):
        c = coordinator(segment_members=[member(i) for i in range(101)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            got = c._get_active_participants()
        self.assertEqual(len(got), 101)

    def test_queries_mdh_with_the_segment_id(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            c._get_active_participants()
        c.mdh.getAllParticipants.assert_called_once_with(
            queryParam={'segmentID': GUID})

    def test_returned_shape_is_unchanged(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            got = c._get_active_participants()
        self.assertEqual(sorted(got[0].keys()),
                         ['email', 'enrolled_date', 'participant_id'])
        self.assertEqual(got[0]['participant_id'], 'BB-0000-0001')

    def test_does_not_filter_on_enrolled_itself(self):
        # MDH already applies the segment rules; a member lacking the enrolled
        # flag in the payload must still be returned rather than re-filtered.
        m = member(1)
        del m['enrolled']
        c = coordinator(segment_members=[m])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            self.assertEqual(len(c._get_active_participants()), 1)

    def test_count_is_not_assumed_to_be_101(self):
        for n in (1, 7, 42, 250):
            c = coordinator(segment_members=[member(i) for i in range(n)])
            with mock.patch.dict('os.environ', {KEY: GUID}):
                self.assertEqual(len(c._get_active_participants()), n)


class FreshOnEveryRun(unittest.TestCase):
    def test_each_call_requeries_mdh(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            c._get_active_participants()
            c._get_active_participants()
            c._get_active_participants()
        self.assertEqual(c.mdh.getAllParticipants.call_count, 3)

    def test_membership_changes_between_runs_are_picked_up(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            first = c._get_active_participants()
            c.mdh.getAllParticipants.return_value = {
                'participants': [member(1), member(2)], 'totalParticipants': 2}
            second = c._get_active_participants()
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 2)

    def test_no_module_level_participant_cache(self):
        import inspect
        src = inspect.getsource(C)
        for bad in ('lru_cache', 'global active_participants', '_cached_participants'):
            self.assertNotIn(bad, src)


class MissingConfiguration(unittest.TestCase):
    def test_absent_env_and_config_raises(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(NonRetryableError) as cm:
                c._get_active_participants()
        self.assertIn(KEY, str(cm.exception))

    def test_empty_string_raises(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: '   '}):
            with self.assertRaises(NonRetryableError):
                c._get_active_participants()

    def test_mdh_is_never_queried_when_unconfigured(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(NonRetryableError):
                c._get_active_participants()
        c.mdh.getAllParticipants.assert_not_called()

    def test_value_may_come_from_the_secret_payload(self):
        c = coordinator(config={KEY: GUID}, segment_members=[member(1)])
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertEqual(len(c._get_active_participants()), 1)

    def test_environment_wins_over_the_secret(self):
        other = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        c = coordinator(config={KEY: other}, segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            c._get_active_participants()
        c.mdh.getAllParticipants.assert_called_once_with(
            queryParam={'segmentID': GUID})


class InvalidSegmentId(unittest.TestCase):
    BAD = ['Active participants', 'not-a-guid', '12345',
           '5be2dbab-8328-47ab-a6f0', '5be2dbab83284 7aba6f06ad71dc266b0',
           '5be2dbab-8328-47ab-a6f0-6ad71dc266b0-extra', 'zzzzzzzz-8328-47ab-a6f0-6ad71dc266b0']

    def test_non_guid_values_raise(self):
        for bad in self.BAD:
            c = coordinator(segment_members=[member(1)])
            with mock.patch.dict('os.environ', {KEY: bad}):
                with self.assertRaises(NonRetryableError, msg=bad):
                    c._get_active_participants()

    def test_mdh_is_never_queried_with_a_bad_id(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: 'Active participants'}):
            with self.assertRaises(NonRetryableError):
                c._get_active_participants()
        c.mdh.getAllParticipants.assert_not_called()

    def test_surrounding_whitespace_is_tolerated(self):
        c = coordinator(segment_members=[member(1)])
        with mock.patch.dict('os.environ', {KEY: '  %s  ' % GUID}):
            c._get_active_participants()
        c.mdh.getAllParticipants.assert_called_once_with(
            queryParam={'segmentID': GUID})


class IgnoredFilter(unittest.TestCase):
    """MDH returns the whole project when it does not recognise the parameter."""

    def test_result_equal_to_project_total_raises(self):
        everyone = [member(i) for i in range(778)]
        c = coordinator(segment_members=everyone, project_total=778)
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(NonRetryableError) as cm:
                c._get_active_participants()
        self.assertIn('not applied', str(cm.exception))

    def test_a_genuine_subset_is_accepted(self):
        c = coordinator(segment_members=[member(i) for i in range(101)],
                        project_total=778)
        with mock.patch.dict('os.environ', {KEY: GUID}):
            self.assertEqual(len(c._get_active_participants()), 101)

    def test_unknown_project_total_does_not_block(self):
        c = coordinator(segment_members=[member(i) for i in range(101)],
                        project_total=None)
        with mock.patch.dict('os.environ', {KEY: GUID}):
            self.assertEqual(len(c._get_active_participants()), 101)


class EmptyResult(unittest.TestCase):
    def test_zero_members_raises(self):
        c = coordinator(segment_members=[])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(NonRetryableError) as cm:
                c._get_active_participants()
        self.assertIn('no participants', str(cm.exception))

    def test_missing_participants_key_raises(self):
        c = coordinator(segment_members=[])
        c.mdh.getAllParticipants.return_value = {'totalParticipants': 0}
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(NonRetryableError):
                c._get_active_participants()


class IncompletePagination(unittest.TestCase):
    def test_short_page_against_reported_total_raises(self):
        c = coordinator(segment_members=[member(i) for i in range(50)],
                        total_override=101)
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(NonRetryableError) as cm:
                c._get_active_participants()
        self.assertIn('partial list', str(cm.exception))

    def test_matching_total_is_accepted(self):
        c = coordinator(segment_members=[member(i) for i in range(101)],
                        total_override=101)
        with mock.patch.dict('os.environ', {KEY: GUID}):
            self.assertEqual(len(c._get_active_participants()), 101)


class ApiErrors(unittest.TestCase):
    def test_api_exception_propagates(self):
        c = coordinator(raises=RuntimeError('MDH 503'))
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(Exception):
                c._get_active_participants()

    def test_api_failure_never_returns_a_list(self):
        c = coordinator(raises=RuntimeError('MDH 503'))
        with mock.patch.dict('os.environ', {KEY: GUID}):
            try:
                got = c._get_active_participants()
            except Exception:
                got = None
        self.assertIsNone(got)

    def test_validation_errors_are_not_relabelled_as_api_faults(self):
        c = coordinator(segment_members=[])
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with mock.patch.object(C, 'handle_api_error') as h:
                with self.assertRaises(NonRetryableError):
                    c._get_active_participants()
                h.assert_not_called()

    def test_project_total_probe_failure_propagates(self):
        c = coordinator(segment_members=[member(1)])
        c.mdh.makeGetRequests.side_effect = RuntimeError('probe failed')
        with mock.patch.dict('os.environ', {KEY: GUID}):
            with self.assertRaises(Exception):
                c._get_active_participants()


class NeverFallsBackToEnrolled(unittest.TestCase):
    def test_no_enrolled_only_fallback_path_exists(self):
        import inspect
        src = inspect.getsource(C.UltrahumanJWTCoordinator._get_active_participants)
        self.assertNotIn("participant.get('enrolled')", src)
        self.assertNotIn('getAllParticipants()', src)

    def test_every_failure_mode_raises_rather_than_returning(self):
        cases = [
            ('unconfigured', {}, coordinator(segment_members=[member(1)])),
            ('bad guid', {KEY: 'nope'}, coordinator(segment_members=[member(1)])),
            ('empty', {KEY: GUID}, coordinator(segment_members=[])),
            ('unfiltered', {KEY: GUID},
             coordinator(segment_members=[member(i) for i in range(778)],
                         project_total=778)),
            ('partial', {KEY: GUID},
             coordinator(segment_members=[member(1)], total_override=99)),
        ]
        for label, env, c in cases:
            with mock.patch.dict('os.environ', env, clear=True):
                with self.assertRaises(Exception, msg=label):
                    c._get_active_participants()


class OtherWorkflowsUntouched(unittest.TestCase):
    def test_publisher_keeps_its_own_filter(self):
        with open('/home/duo/sensorfabric-uh/ultrahuman/uh_publisher.py') as f:
            src = f.read()
        self.assertIn("participant.get('enrolled') and uh_enabled == 'yes'", src)
        self.assertNotIn('segmentID', src)

    def test_legacy_generator_untouched(self):
        with open('/home/duo/sensorfabric-uh/ultrahuman/uh_jwt_generator.py') as f:
            src = f.read()
        self.assertNotIn('segmentID', src)

    def test_worker_does_not_select_participants(self):
        with open('/home/duo/sensorfabric-uh/ultrahuman/uh_jwt_worker.py') as f:
            src = f.read()
        self.assertNotIn('getAllParticipants', src)
        self.assertNotIn('segmentID', src)


if __name__ == '__main__':
    unittest.main(verbosity=2)
