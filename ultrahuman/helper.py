from sensorfabric.mdh import MDH
from datetime import datetime, timezone, date, timedelta
import math
import inspect
import os
import random

class ParticipantNotEnrolled(Exception):
    """Raised when the participant is not enrolled in the study."""
    pass

class Helper:
    """ Helper class for reporting template"""
    def __init__(self, mdh: MDH,
                 participant_id: str,
                 end_date: date):
        """
        Paramters
        ---------
        1. mdh (sensorfabric.mdh.MDH) - A sensorfabric MDH object.
        2. participant_id (string) - Participant ID for which the report is being
           created.
        3. end_date (date) - Last date (inclusive) of the week you want to use for calculating the
            reporting metrics.

        Returns
        -------
        Helper object

        Exceptions
        -----
        ParticipantNotEnrolled - If the participant status is not enrolled
        """
        self.mdh: MDH = mdh
        self.participant_id: str = participant_id
        self.end_date: date = end_date
        # Weeks are assumed to be inclusive of start and end dates. If the end date is a Sat,
        # the start date will be the Sun previous to it. Hence 6 and not 7.
        self.start_date: date = end_date - timedelta(days=6)

        # Go ahead and get all the information for the participant from MDH
        self.participant = mdh.getParticipant(participant_id)

        # Make sure that this participant has enrolled
        if not self.participant['enrolled']:
            raise ParticipantNotEnrolled('Participant is not yet enrolled in the study')

    def enrolledDate(self) -> date:
        """
        Returns the enrollment date of the participant.
        """
        date_str = self.participant['enrollmentDate']
        if type(date_str) == str:
            return datetime.fromisoformat(date_str).date()
        else:
            return date_str

    def getParticipant(self) -> dict:
        """Returns the MDH participant dictionary"""
        return self.participant

    def weeksEnrolled(self) -> int:
        """
        Returns the total number of weeks (rounded up) the
        participant has enrolled in the study.
        """
        enrolled_on = datetime.fromisoformat(self.participant['enrollmentDate'])
        today = datetime.now(timezone.utc)
        delta = today - enrolled_on

        weeks = math.ceil(delta.days / 7)

        return weeks

    def weeksPregnant(self) -> int:
        """
        Returns the gestational age in weeks for the user.
        If we are not able to find it then it returns None.
        """
        if not 'customFields' in self.participant:
            return None

        customFields = self.participant['customFields']

        # Check to see if we have the gestational age field.
        # Some older study versions may not have this.
        if not 'ga_calculated_today_days' in customFields:
            return None

        ga_days = customFields['ga_calculated_today_days']
        try:
            ga_days_int = int(ga_days)
        except ValueError:
            return None

        # The current GA week is going to be the week they are in right now.
        return math.floor(ga_days_int / 7)

    def ringWearTime(self) -> int:
        """
        Returns the percentage of ring wear time. If there is no data
        on this then it returns a None.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Method not implemented')

    def emaCompleted(self) -> int:
        """
        Method which returns the total number of EMA completed in the given
        time range. Returns a None if EMA's are not supported in this study.
        """
        results = self.mdh.getSurveyResults(queryParam={
            'participantIdentifier': self.participant_id,
            'surveyName': 'EMA AM,EMA PM',
            'after': self.start_date.isoformat(),
            'before': self.end_date.isoformat(),
        })

        return len(results)

    def bloodPressure(self):
        """
        Method which returns the number of BP meassurements this week, trend
        compared to past week and number of points above thresholds.

        BP values come directly from the Omron connection
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        this_week = self.mdh.getDeviceDataPoints('Omron',
                                     ['BloodPressureSystolic', 'BloodPressureDiastolic'],
                                     queryParam={
                                         'participantIdentifier': self.participant_id,
                                         'observedAfter': self.start_date.isoformat(),
                                         'observedBefore': self.end_date.isoformat(),
                                     })
        # We are using start_date - 7d and end_date - 1d is for the previous week.
        # For example from if the current week is from (Sun - Sat) then the previous week
        # is the previous (Sun - Sat)
        previous_week = self.mdh.getDeviceDataPoints('Omron',
                                     ['BloodPressureSystolic', 'BloodPressureDiastolic'],
                                     queryParam={
                                         'participantIdentifier': self.participant_id,
                                         'observedAfter': (self.start_date - timedelta(days=7)).isoformat(),
                                         'observedBefore': (self.end_date - timedelta(days=1)).isoformat(),
                                     })

        bp_counts = 0
        above_threshold_counts = 0
        this_week_map = 0
        previous_week_map = 0
        # BP comparison between weeks is done using MAP (Mean Arterial Pressure)
        # `MAP = (2 x DBP + SBP) / 3`

        # TODO: Need to look at the raw data and figure out how to drop SBP and DBP values
        # belonging to the same meassurement together.

    def heartRateSummary(self):
        """
        Get the summary of HR values in the past week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def temperatureSummary(self):
        """
        Get the summary of temperature values in the past week,
        along with trend comparison to the last week and temperature values above
        the threhold.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def sleepSummary(self):
        """
        Get the summary of sleep values in the past week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def weightSummary(self):
        """
        Get weight summary values in the past week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def movementSummary(self):
        """
        Get movement summary values in the past week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def topSymptomsRecorded(self):
        """
        Get the top 3 symptoms that the users have recorded ordered in the list.
        If there were no symptoms recorded in the past week this function returns an empty list.
        In case of ties, symptom order is alphabetical.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def _debugOutputs(self):
        """
        This method is is used to simulate the behavior of this library until all sensor data
        can be tested and all methods have been fully implemented. The goal is to have the inteface
        ready and tested for the layers above this.
        """

        # Get the current stack frame so we travense back to get the function that
        # called it.
        current_stack_frame = inspect.currentframe()
        # Get the back pointer from this frame to the calling frame
        calling_frame = current_stack_frame.f_back
        # Make sure we have a calling frame, if not then just return.
        if not calling_frame:
            return None

        calling_function_name = calling_frame.f_code.co_name

        trends = ['higher', 'lower', 'steady']
        # Go through all the function names and return an example object.
        if calling_function_name == 'ringWearTime':
            return {
                # Percentage of ring wear time during the week.
                'ring_wear_percent': 97,
            }

        elif calling_function_name == 'bloodPressure':
            return {
                'counts': 6,
                'above_threshold_counts': 2,
                'trend': trends[random.randint(0, len(trends)-1)],
            }

        elif calling_function_name == 'heartRateSummary':
            return {
                'hr_counts': 12001600,
                'avg_rhr': 62,
            }

        elif calling_function_name == 'temperatureSummary':
            return {
                'counts': 12103,
                'above_threshold_counts': 3,
                'trend': trends[random.randint(0, len(trends)-1)],
            }

        elif calling_function_name == 'sleepSummary':
            return {
                'hours': 60,
                'average_per_night': 6.4,
            }

        elif calling_function_name == 'weightSummary':
            return {
                # Can return both positive or negative values.
                'change_in_weight': random.randint(0, 10) - 5,
            }

        elif calling_function_name == 'movementSummary':
            return {
                'total_movements_mins': 120,
                'average_steps_int': 4200,
                # Trend can return a positive or negative value.
                'trend': 5000 - random.randint(4500, 5500),
            }

        elif calling_function_name == 'topSymptomsRecorded':
            return ['Headaches', 'Indigestion', 'Nausea']
