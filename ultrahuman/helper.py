from sensorfabric.mdh import MDH
from sensorfabric.athena import athena
import pandas as pd
from datetime import datetime, timezone, date, timedelta
import math
import inspect
import os
import random

"""
Current Limitations
-------------------
1. We don't have any data inside GoogleFit or HealthConnect for Android phones. Hence we are not able to test
    out weight data going into it.
"""

class ParticipantNotEnrolled(Exception):
    """Raised when the participant is not enrolled in the study."""
    pass

class Helper:
    """ Helper class for reporting template"""
    def __init__(self, mdh: MDH,
                 athena_mdh: athena,
                 athena_uh: athena,
                 participant_id: str,
                 end_date: date):
        """
        Paramters
        ---------
        1. mdh (sensorfabric.mdh.MDH) - A sensorfabric MDH object.
        2. athena_mdh (sensorfabric.athena.athena) - Athena connection to MDH backend.
        3. athena_uh (sensorfabric.athena.athena) - Athena connection to our AWS UH backend.
        4. participant_id (string) - Participant ID for which the report is being
           created.
        5. end_date (date) - Last date (inclusive) of the week you want to use for calculating the
            reporting metrics.

        Returns
        -------
        Helper object

        Exceptions
        -----
        ParticipantNotEnrolled - If the participant status is not enrolled
        """
        self.mdh: MDH = mdh
        self.athena_mdh = athena_mdh
        self.athena_uh = athena_uh
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

        query = f"""
            -- Assuming that we get temperature values every 5 weeks, we calculate the wear time based on this
            -- metric.
            select
                    cast(ceil(count(*) * 100 / (288.0 * 7)) as int) "wear_percentage"
                from temp
                where pid = '{self.participant_id}'
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) <= date('{self.end_date.isoformat()}')
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) >= date('{self.end_date.isoformat()}') - interval '6' day
        """

        weartime = self.athena_uh.execQuery(query)

        if weartime.shape[0] <= 0:
            return None

        wear_percentage = None
        try:
            wear_percentage = int(weartime['wear_percentage'][0])
            # If somehow the wear percentage is above 100, we will restrict it to 100
            if wear_percentage > 100:
                wear_percentage = 100
        except:
            return None

        return {
                # Percentage of ring wear time during the week.
                'ring_wear_percent': wear_percentage
        }

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

        query_this_week = f"""
            select systolic, diastolic from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and datetimelocal >= date('{self.start_date.isoformat()}')
                and datetimelocal <= date('{self.end_date.isoformat()}')
        """
        # We are using start_date - 7d and end_date - 1d is for the previous week.
        # For example from if the current week is from (Sun - Sat) then the previous week
        # is the previous (Sun - Sat)
        query_prev_week = f"""
            select systolic, diastolic from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and datetimelocal >= date('{(self.start_date - timedelta(days=7)).isoformat()}')
                and datetimelocal <= date('{(self.end_date - timedelta(days=1)).isoformat()}')
        """

        this_week: pd.DataFrame = self.athena_mdh.execQuery(query_this_week)
        previous_week: pd.DataFrame = self.athena_mdh.execQuery(query_prev_week)

        # If we did not get any BP data for this week, we just return none.
        if this_week.shape[0] <= 0:
            return None

        this_week['systolic'] = pd.to_numeric(this_week['systolic'], errors='coerce')
        this_week['diastolic'] = pd.to_numeric(this_week['diastolic'], errors='coerce')
        previous_week['systolic'] = pd.to_numeric(previous_week['systolic'], errors='coerce')
        previous_week['diastolic'] = pd.to_numeric(previous_week['diastolic'], errors='coerce')

        high_values = 0
        # Check for values which are above the threshold.
        if this_week.shape[0] > 0:
            for sys, dia in zip(this_week['systolic'], this_week['diastolic']):
                try:
                    if sys > 140 or dia > 90:
                        high_values += 1
                except:
                    # If there are any errors then we can't do much here right now.
                    # Let's just move ahead for now.
                    continue

        # Not always gaurenteed that we will have data for this week and the past.
        trend = None
        if this_week.shape[0] > 0 and previous_week.shape[0] > 0:
            sys_curr = this_week['systolic'].mean()
            dia_curr = this_week['diastolic'].mean()
            map_curr = (2 * dia_curr + sys_curr) / 3

            sys_prev = previous_week['systolic'].mean()
            dia_prev = previous_week['diastolic'].mean()
            map_prev = (2 * dia_prev + sys_prev) / 3

            trend = 'Steady'
            if map_curr > map_prev:
                trend = 'Higher'
            elif map_curr < map_prev:
                trend = 'Lower'

        return {
                'counts': self._addCommas(this_week.shape[0]),
                'above_threshold_counts': high_values,
                'trend': trend,
        }

    def heartRateSummary(self):
        """
        Get the summary of HR values in the past week.
        Important - Do no use counts. Since it gives a single HR value every 5 minutes this is not
        and accurate representation of the total number of beats.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            with rhr as (
            select
                cast(floor(avg(object_values_value)) as int) avg_rhr
            from night_rhr
            where pid = '{self.participant_id}'
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) <= date('{self.end_date.isoformat()}')
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) >= date('{self.end_date.isoformat()}') - interval '6' day
            ),
            hrs as (
                select count(*) "hr_counts"
                from hr
            where pid = 'BB-3234-3734'
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) <= date('{self.end_date.isoformat()}')
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) >= date('{self.end_date.isoformat()}') - interval '6' day
            )
            select rhr.avg_rhr, hrs.hr_counts
                from rhr
            cross join hrs
        """

        hrsummary = self.athena_uh.execQuery(query)

        if hrsummary.shape[0] <= 0:
            return None

        return {
            'hr_counts': None,
            'avg_rhr': hrsummary['avg_rhr'][0],
        }

    def temperatureSummary(self):
        """
        Get the summary of temperature values in the past week,
        along with trend comparison to the last week and temperature values above
        the threhold.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            -- This does not do any outlier removal right now. Just the raw numbers.
            with wcurr as (
                select
                    cast(from_iso8601_timestamp(object_values_timestamp_iso8601_tz) as date) "date",
                    object_values_value "skin_temp"
                from temp
                where pid = '{self.participant_id}'
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) <= date('{self.end_date.isoformat()}')
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) >= date('{self.end_date.isoformat()}') - interval '6' day
            ),
            higher as (
                select count(*) threshold_counts
                    from wcurr
                    where (skin_temp + 32) * (9/5) > 100
            ),
            wprev as (
                select
                    cast(from_iso8601_timestamp(object_values_timestamp_iso8601_tz) as date) "date",
                    object_values_value "skin_temp"
                from temp
                where pid = 'BB-3234-3734'
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) <= date('{self.end_date.isoformat()}') - interval '7' day
                and from_iso8601_timestamp(object_values_timestamp_iso8601_tz) >= date('{self.end_date.isoformat()}') - interval '13' day
            ),
            s as (
            select avg(wcurr.skin_temp) "curr_avg_temp", count(wcurr.skin_temp) "curr_count",
                avg(wprev.skin_temp) "prev_avg_temp", count(wprev.skin_temp) "prev_count"
            from wcurr
                cross join wprev
            )

            -- TODO: Find out why adding higher.threshold_counts is breaking the library right now.
            select *,
                (curr_avg_temp + 32) * (9/5) "curr_avg_temp_f",
                case
                    when curr_avg_temp - prev_avg_temp < 0 then 'lower'
                    when curr_avg_temp - prev_avg_temp > 0 then 'higher'
                    else 'steady'
                end "trend"
                from s
                cross join higher
        """

        temperature = self.athena_uh.execQuery(query)

        if temperature.shape[0] <= 0:
            return None

        return {
                'counts': self._addCommas(temperature['curr_count'][0]),
                'above_threshold_counts': 0,
                'trend': self._capFirst(temperature['trend'][0]),
        }

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
        Since we don't really know if users are changing device or what health enclave our
        weight data is going to be, we have to unfortunately test the weight values accross all
        3 pools - HealthKit, GoogleFit, Healthconnect (Android's new thing).
        TODO: Add support here for Android devices which includes - GoogleFit and Healthconnect
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
                with wcurr as (
                SELECT
                    CASE units
                    WHEN 'lb' THEN cast(VALUE AS DOUBLE)
                    ELSE 2.2 * cast(VALUE AS DOUBLE)
                    END weight
                FROM
                    healthkitv2samples
                WHERE
                    TYPE = 'Weight'
                    AND participantidentifier = '{self.participant_id}'
                    and cast("date" as date) <= cast('{self.end_date.isoformat()}' as date)
                    and cast("date" as date) >= cast('{self.end_date.isoformat()}' as date) - interval '6' day
                ),
                wprev as (
                SELECT
                    CASE units
                    WHEN 'lb' THEN cast(VALUE AS DOUBLE)
                    ELSE 2.2 * cast(VALUE AS DOUBLE)
                    END weight
                FROM
                    healthkitv2samples
                WHERE
                    TYPE = 'Weight'
                    AND participantidentifier = 'BB-3234-3734'
                    and cast("date" as date) <= cast('{self.end_date.isoformat()}' as date) - interval '7' day
                    and cast("date" as date) >= cast('{self.end_date.isoformat()}' as date) - interval '13' day
                )

                select floor(avg(wcurr.weight)),
                        floor(avg(wprev.weight)),
                        floor(avg(wcurr.weight)) - floor(avg(wprev.weight)) "weight_changed" from wcurr
                cross join wprev
        """

        healthkit = self.athena_mdh.execQuery(query)
        change_in_weight = None
        try:
            change_in_weight = int(healthkit['weight_changed'][0])
        except:
            return None

        return {
            # Can return both positive or negative values.
            'change_in_weight': change_in_weight
        }

    def movementSummary(self):
        """
        Get movement summary values in the past week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        raise ('Function not implemented')

    def topSymptomsRecorded(self):
        """
        Get the top 5 symptoms that the users have recorded ordered in the list.
        If there were no symptoms recorded in the past week this function returns an empty list.
        In case of ties, symptom order is alphabetical.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            with r1 as (
            select cast(observationdate as date) "dates", value "symptom" from projectdevicedata
                where participantidentifier = '{self.participant_id}'
                and cast(observationdate as date) <= cast('{self.end_date.isoformat()}' as date)
                and cast(observationdate as date) >= cast('{self.end_date.isoformat()}' as date) - interval '6' day
                and type = 'symptom'
                and value != 'no_symptom'
            ),
            r2 as (
            select symptom, count(*) "total_count", array_agg(distinct(dates)) "days"
            from r1
            group by symptom
            )

            select symptom, total_count, cardinality("days") "days"
            from r2
            order by days desc
            limit 5
        """
        topsymptoms = self.athena_mdh.execQuery(query)

        if topsymptoms.shape[0] <=0:
            return None

        # For each symptom name, lets go ahead and replace '_' with ' '
        tsymptoms = []
        for name, count, days in zip(topsymptoms['symptom'], topsymptoms['total_count'], topsymptoms['days']):
            tsymptoms.append({
                'name': self._capFirst(strname),
                'count': count,
                'days': days,
            })

        return tsymptoms

    def _capFirst(self, value: str) -> str:
        """Capitalize the first letter of the string"""
        if len(value) <= 0:
            return value

        return value[0].upper() + value[min(1, len(value)):]

    def _addCommas(self, value: int) -> str:
        """Add commas in the correct place integer passed and then return a string for it."""
        buff: str = str(value)
        buff = buff[::-1]
        cbuff = ""
        for i in range(0, len(buff)):
            cbuff = cbuff + buff[i]
            if (i+1) % 3 == 0:
                cbuff = cbuff + ','
        cbuff = cbuff[::-1]

        return cbuff

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
                'ring_wear_percent': 65,
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
                'counts': self._addCommas(12103),
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
            return [{'name':'Headache',  'count': 4, 'days':4},
                    {'name':'Restless Legs', 'count':3, 'days': 2}]
