from sensorfabric.mdh import MDH
from sensorfabric.needle import Needle
import pandas as pd
import datetime
import math
import inspect
import os
import random
import hashlib
import json
import functools
from typing import Dict, Any, Optional, Tuple
import logging

"""
Current Limitations
-------------------
1. We don't have any data inside GoogleFit or HealthConnect for Android phones. Hence we are not able to test
    out weight data going into it.
"""
logger = logging.getLogger()
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', logging.INFO)

if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(DEFAULT_LOG_LEVEL)
else:
    logging.basicConfig(level=DEFAULT_LOG_LEVEL)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


class ParticipantNotEnrolled(Exception):
    """Raised when the participant is not enrolled in the study."""
    pass


class Helper:
    """ Helper class for reporting template"""
    def __init__(self, config: Dict[str, Any]):
        """
        Paramters
        ---------
        1. config (Dict[str, Any]) - Configuration dictionary containing the following keys:
            - MDH_SECRET_KEY: MDH secret key
            - MDH_ACCOUNT_NAME: MDH account name
            - MDH_PROJECT_ID: MDH project ID
            - MDH_PROJECT_NAME: MDH projetc name.
            - UH_DATABASE: AWS database name for UH data.
            - UH_WORKGROUP: AWS workgroup name for UH data.
            - UH_S3_LOCATION: AWS S3 location for query results for UH.
            - participant_id: Participant ID
            - end_date (datetime.date): End date of the week
            - start_date (datetime.date): Start date of the week

        Returns
        -------
        Helper object

        Exceptions
        -----
        ParticipantNotEnrolled - If the participant status is not enrolled
        """
        self.__config = config
        mdh_configuration = {
            'account_secret': self.__config.get('MDH_SECRET_KEY'),
            'account_name': self.__config.get('MDH_ACCOUNT_NAME'),
            'project_id': self.__config.get('MDH_PROJECT_ID'),
        }
        # Used to access MDH API such as surveys, custom variables etc.
        self.mdh = MDH(**mdh_configuration)

        self.athena_mdh = Needle(method='mdh', mdh_configuration={
            'account_secret': self.__config.get('MDH_SECRET_KEY'),
            'account_name': self.__config.get('MDH_ACCOUNT_NAME'),
            'project_id': self.__config.get('MDH_PROJECT_ID'),
            'project_name': self.__config.get('MDH_PROJECT_NAME')
        })

        # Maintains an Athena SQL connection to UA databases.
        # ACCESS and SECRET keys are used from system defaults.
        self.athena_uh = Needle(method='aws', aws_configuration={
            'database': self.__config.get('UH_DATABASE'),
            'workgroup': self.__config.get('UH_WORKGROUP'),
            's3_location': self.__config.get('UH_S3_LOCATION')
        })
        self.participant_id = self.__config.get('participant_id')
        
        # Convert date strings to datetime.date objects if they're strings
        end_date = self.__config.get('end_date')
        if isinstance(end_date, str):
            self.end_date = datetime.datetime.fromisoformat(end_date).date()
        else:
            self.end_date = end_date
            
        start_date = self.__config.get('start_date')
        if isinstance(start_date, str):
            self.start_date = datetime.datetime.fromisoformat(start_date).date()
        else:
            self.start_date = start_date

        # Go ahead and get all the information for the participant from MDH
        self.participant = self.mdh.getParticipant(self.participant_id)

        # Make sure that this participant has enrolled
        if not self.participant['enrolled']:
            raise ParticipantNotEnrolled('Participant is not yet enrolled in the study')

    def enrolledDate(self) -> datetime.date:
        """
        Returns the enrollment date of the participant.
        """
        date_str = self.participant['enrollmentDate']
        if type(date_str) == str:
            return datetime.datetime.fromisoformat(date_str).date()
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
        enrolled_on = datetime.datetime.fromisoformat(self.participant['enrollmentDate'])
        today = datetime.datetime.now(datetime.timezone.utc)
        delta = today - enrolled_on

        weeks = math.ceil(delta.days / 7)

        return weeks

    def _get_utc_timestamp_range(self, start_date: datetime.date, end_date: datetime.date) -> Tuple[int, int]:
        """
        Convert date range to UTC timestamp range in milliseconds.
        
        Parameters:
        -----------
        start_date : datetime.date
            Start date (inclusive)
        end_date : datetime.date  
            End date (inclusive)
            
        Returns:
        --------
        Tuple[int, int]
            (start_timestamp_ms, end_timestamp_ms) where end is exclusive
        """
        # Convert dates to UTC datetime at start of day
        start_datetime = datetime.datetime.combine(start_date, datetime.time.min)
        end_datetime = datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min)
        
        # Convert to UTC timestamps in seconds (not milliseconds)
        start_ts = int(start_datetime.replace(tzinfo=datetime.timezone.utc).timestamp())
        end_ts = int(end_datetime.replace(tzinfo=datetime.timezone.utc).timestamp())
        
        return start_ts, end_ts

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

        # Calculate UTC timestamp range for the past 7 days (including end_date)
        start_ts, end_ts = self._get_utc_timestamp_range(self.start_date, self.end_date)
        
        query = f"""
            -- Assuming that we get temperature values every 5 minutes, we calculate the wear time based on this
            -- metric. Using fast integer timestamp comparison instead of expensive string parsing.
            select
                    cast(ceil(count(*) * 100 / 2016.0) as int) "wear_percentage"
                from temp
                where pid = '{self.participant_id}'
                and object_values_timestamp >= {start_ts}
                and object_values_timestamp < {end_ts}
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

        # Calculate date strings for current and previous weeks
        start_date_str = self.start_date.strftime('%Y-%m-%d')
        end_date_str = self.end_date.strftime('%Y-%m-%d')
        prev_start_date = self.start_date - datetime.timedelta(days=7)
        prev_end_date = self.end_date - datetime.timedelta(days=7)
        prev_start_str = prev_start_date.strftime('%Y-%m-%d')
        prev_end_str = prev_end_date.strftime('%Y-%m-%d')

        # Combined query to get both weeks' data in one database call
        combined_query = f"""
            with current_week as (
                select cast(coalesce(datetimelocal, datetime, inserteddate) as date) reading_date,
                       cast(systolic as double) systolic, cast(diastolic as double) diastolic,
                       'current' as week_type
                from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and cast(coalesce(datetimelocal, datetime, inserteddate) as date) between date('{start_date_str}') and date('{end_date_str}')
            ),
            previous_week as (
                select cast(coalesce(datetimelocal, datetime, inserteddate) as date) reading_date,
                       cast(systolic as double) systolic, cast(diastolic as double) diastolic,
                       'previous' as week_type
                from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and cast(coalesce(datetimelocal, datetime, inserteddate) as date) between date('{prev_start_str}') and date('{prev_end_str}')
            )
            select * from current_week
            union all
            select * from previous_week
        """
        combined_data: pd.DataFrame = self.athena_mdh.execQuery(combined_query)

        if combined_data is None or combined_data.empty:
            return None
        combined_data['systolic'] = pd.to_numeric(combined_data['systolic'], errors='coerce', downcast='float')
        combined_data['diastolic'] = pd.to_numeric(combined_data['diastolic'], errors='coerce', downcast='float')
        # Split the results back into current and previous weeks
        this_week = combined_data[combined_data['week_type'] == 'current'][['reading_date', 'systolic', 'diastolic']]
        previous_week = combined_data[combined_data['week_type'] == 'previous'][['systolic', 'diastolic']]

        # If we did not get any BP data for this week, we just return none.
        if this_week.shape[0] <= 0:
            return None

        # Data is already cast to double in SQL, no need for additional pandas conversion
        high_values = 0
        high_readings = []
        # Check for values which are above the threshold.
        if this_week.shape[0] > 0:
            for when, sys, dia in zip(this_week['reading_date'], this_week['systolic'], this_week['diastolic']):
                # Evaluate each side independently and NULL-safely. A reading
                # that is high on one value while the other is missing is still
                # a high reading and must not be dropped. int() is only applied
                # to a value that is actually present.
                sys_high = pd.notna(sys) and sys > 140
                dia_high = pd.notna(dia) and dia > 90

                if sys_high or dia_high:
                    # Build the entry first so the count and the list can never disagree.
                    reading = {
                        'date': self._formatReadingDate(when),
                        'systolic': int(sys) if pd.notna(sys) else None,
                        'diastolic': int(dia) if pd.notna(dia) else None,
                    }
                    high_values += 1
                    high_readings.append(reading)

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
                # Raw integer count. The template needs this for singular/plural wording,
                # since 'counts' above is a formatted string.
                'counts_int': this_week.shape[0],
                'above_threshold_counts': high_values,
                # The individual readings which crossed the threshold. Empty when none did.
                'high_readings': high_readings,
                'is_high': high_values > 0,
                'trend': trend,
        }

    def heartRateSummary(self):
        """
        Get resting heart rate from MDH Oura sleep data.

        ourasleep holds one row per sleep session, so a nap is its own row.
        The sessions are averaged within each calendar day first and only then
        averaged across the days that have data, so every day carries equal
        weight no matter how many sessions it contains. Days with no valid
        reading drop out of the average rather than counting as zero.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            with daily as (
                select day, avg(lowestheartrate) as day_rhr
                from ourasleep
                where participantidentifier = '{self.participant_id}'
                  and day between '{self.start_date}' and '{self.end_date}'
                  and lowestheartrate is not null
                group by day
            )
            select
                cast(floor(avg(day_rhr)) as int) as avg_rhr
            from daily
        """

        hrsummary = self.athena_mdh.execQuery(query)

        if hrsummary.shape[0] <= 0:
            return None

        avg_rhr = hrsummary['avg_rhr'][0]

        if pd.isna(avg_rhr):
            return None

        return {
            'hr_counts': None,
            'avg_rhr': int(avg_rhr),
        }

    def hrvSummary(self):
        """
        Get the average HRV for this week from MDH Oura sleep data.

        averagehrv is Oura's per-session mean of its 5 minute rMSSD samples,
        reported in milliseconds.

        ourasleep holds one row per sleep session, so a nap is its own row.
        The sessions are averaged within each calendar day first and only then
        averaged across the days that have data, so every day carries equal
        weight no matter how many sessions it contains. Days with no valid
        reading drop out of the average rather than counting as zero.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            with daily as (
                select day, avg(averagehrv) as day_hrv
                from ourasleep
                where participantidentifier = '{self.participant_id}'
                  and day between '{self.start_date}' and '{self.end_date}'
                  and averagehrv is not null
                group by day
            )
            select
                cast(floor(avg(day_hrv)) as int) as avg_hrv
            from daily
        """

        hrvsummary = self.athena_mdh.execQuery(query)

        if hrvsummary is None or hrvsummary.shape[0] <= 0:
            return None

        avg_hrv = hrvsummary['avg_hrv'][0]

        if pd.isna(avg_hrv):
            return None

        return {
            'avg_hrv': int(avg_hrv),
        }

    def temperatureSummary(self):
        """
        Get the summary of temperature values in the past week,
        along with trend comparison to the last week and temperature values above
        the threhold.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        # Calculate UTC timestamp ranges for current and previous weeks
        curr_start_ts, curr_end_ts = self._get_utc_timestamp_range(self.start_date, self.end_date)
        prev_start_date = self.start_date - datetime.timedelta(days=7)
        prev_end_date = self.end_date - datetime.timedelta(days=7)
        prev_start_ts, prev_end_ts = self._get_utc_timestamp_range(prev_start_date, prev_end_date)

        query = f"""
            -- Using conditional aggregation to eliminate cross joins
            select
                avg(case when object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts} 
                    then object_values_value end) curr_avg_temp,
                count(case when object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts} 
                    then object_values_value end) curr_count,
                avg(case when object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts} 
                    then object_values_value end) prev_avg_temp,
                count(case when object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts} 
                    then object_values_value end) prev_count,
                count(case when object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts}
                    and object_values_value * 1.8 + 32 > 100 then 1 end) threshold_counts
            from temp
            where pid = '{self.participant_id}'
                and (
                    (object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts}) or
                    (object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts})
                )
        """

        temperature = self.athena_uh.execQuery(query)

        if temperature.shape[0] <= 0:
            return None

        # Calculate trend here since we removed the CASE statement from SQL
        curr_avg_raw = temperature['curr_avg_temp'][0]
        prev_avg_raw = temperature['prev_avg_temp'][0]

        curr_avg = float(curr_avg_raw) if curr_avg_raw is not None else None
        prev_avg = float(prev_avg_raw) if prev_avg_raw is not None else None

        if curr_avg is None or prev_avg is None:
            return None

        trend = 'steady'
        if curr_avg and prev_avg:
            if curr_avg - prev_avg < -0.1:
                trend = 'lower'
            elif curr_avg - prev_avg > 0.1:
                trend = 'higher'

        return {
                'counts': self._addCommas(temperature['curr_count'][0]),
                'above_threshold_counts': temperature['threshold_counts'][0],
                'trend': self._capFirst(trend),
        }

    def sleepSummary(self):
        """
        Get the sleep summary values for this week from MDH Oura sleep data.

        totalsleepduration is in seconds, per the MDH Oura Sleep export docs.

        ourasleep holds one row per sleep session, so a nap is its own row. The
        sessions are summed per day first and only then averaged across the days
        that actually have data, otherwise a short nap would drag the per-night
        average down.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        query = f"""
            with nightly as (
                select day, sum(totalsleepduration) as day_seconds
                from ourasleep
                where participantidentifier = '{self.participant_id}'
                  and day between '{self.start_date}' and '{self.end_date}'
                  and totalsleepduration is not null
                group by day
            )
            select
                sum(day_seconds) as total_seconds,
                avg(day_seconds) as avg_day_seconds
            from nightly
        """

        sleep = self.athena_mdh.execQuery(query)

        if sleep is None or sleep.shape[0] <= 0:
            return None

        total_seconds = sleep['total_seconds'][0]
        avg_day_seconds = sleep['avg_day_seconds'][0]

        if pd.isna(total_seconds) or pd.isna(avg_day_seconds):
            return None

        return {
            # Total sleep across the whole report period, in hours.
            'hours': int(round(float(total_seconds) / 3600)),
            # Average sleep per night, across the nights that have data.
            'average_per_night': round(float(avg_day_seconds) / 3600, 1),
        }

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

        end_date_str = self.end_date.strftime('%Y-%m-%d')
        curr_start_str = self.start_date.strftime('%Y-%m-%d')
        prev_end_str = (self.end_date - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        prev_start_str = (self.start_date - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

        query = f"""
                select 
                    cast(floor(avg(case when cast(startdate as date) between date('{curr_start_str}') and date('{end_date_str}')
                        then case units when 'lb' then cast(value as double) else cast(value as double) * 2.20462 end 
                        end)) as int) curr_avg_weight,
                    cast(floor(avg(case when cast(startdate as date) between date('{prev_start_str}') and date('{prev_end_str}')
                        then case units when 'lb' then cast(value as double) else cast(value as double) * 2.20462 end 
                        end)) as int) prev_avg_weight
                from healthkitv2samples
                where type in ('BodyMass', 'Weight')
                    and participantidentifier = '{self.participant_id}'
                    and (
                        cast(startdate as date) between date('{curr_start_str}') and date('{end_date_str}') or
                        cast(startdate as date) between date('{prev_start_str}') and date('{prev_end_str}')
                    )
        """

        healthkit = self.athena_mdh.execQuery(query)
        change_in_weight = None
        try:
            curr_weight = healthkit['curr_avg_weight'][0]
            prev_weight = healthkit['prev_avg_weight'][0]
            if curr_weight is not None and prev_weight is not None:
                change_in_weight = int(curr_weight) - int(prev_weight)
            else:
                return None
        except:
            return None

        return {
            # Can return both positive or negative values.
            'change_in_weight': change_in_weight
        }

    def movementSummary(self):
        """
        Get average daily steps from MDH Oura activity data.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        period_days = (self.end_date - self.start_date).days + 1
        prev_end_date = self.start_date - datetime.timedelta(days=1)
        prev_start_date = prev_end_date - datetime.timedelta(days=period_days - 1)

        query = f"""
            WITH daily_steps AS (
                SELECT
                    day,
                    MAX(steps) AS total_steps,
                    CASE
                        WHEN day BETWEEN '{self.start_date}' AND '{self.end_date}'
                            THEN 'current'
                        WHEN day BETWEEN '{prev_start_date}' AND '{prev_end_date}'
                            THEN 'previous'
                    END AS period_type
                FROM ouradailyactivity
                WHERE participantidentifier = '{self.participant_id}'
                  AND day BETWEEN '{prev_start_date}' AND '{self.end_date}'
                  AND steps IS NOT NULL
                GROUP BY day,
                    CASE
                        WHEN day BETWEEN '{self.start_date}' AND '{self.end_date}'
                            THEN 'current'
                        WHEN day BETWEEN '{prev_start_date}' AND '{prev_end_date}'
                            THEN 'previous'
                    END
            )
            SELECT
                CAST(FLOOR(AVG(
                    CASE WHEN period_type = 'current'
                         THEN total_steps END
                )) AS INT) AS avg_curr,
                CAST(FLOOR(AVG(
                    CASE WHEN period_type = 'previous'
                         THEN total_steps END
                )) AS INT) AS avg_prev
            FROM daily_steps
        """

        movement = self.athena_mdh.execQuery(query)

        if movement.shape[0] <= 0:
            return None

        avg_steps = None
        steps_changed = None

        # No try/except here on purpose. NULL aggregates are handled by the
        # pd.isna checks below and correctly yield None, meaning "no records".
        # Anything else (a missing column, a type the cast did not produce) is a
        # real failure and must surface rather than be reported as "No Data".
        avg_curr = movement['avg_curr'][0]
        avg_prev = movement['avg_prev'][0]

        if not pd.isna(avg_curr):
            avg_steps = int(avg_curr)

        if not pd.isna(avg_curr) and not pd.isna(avg_prev):
            steps_changed = int(avg_curr) - int(avg_prev)

        if avg_steps is None:
            return None

        return {
            'total_movements_mins': None,
            'average_steps_int': self._addCommas(avg_steps),
            'trend': self._addCommas(steps_changed) if steps_changed is not None else None,
        }

    def topSymptomsRecorded(self):
        """
        Get the top 5 symptoms that the users have recorded ordered in the list.
        If there were no symptoms recorded in the past week this function returns an empty list.
        In case of ties, symptom order is alphabetical.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        end_date_str = self.end_date.strftime('%Y-%m-%d')
        start_date_str = self.start_date.strftime('%Y-%m-%d')

        query = f"""
            with date_range as (
                select date('{start_date_str}') as start_date, date('{end_date_str}') as end_date
            ),
            r1 as (
            select cast(observationdate as date) "dates", value "symptom" 
            from projectdevicedata, date_range
                where participantidentifier = '{self.participant_id}'
                and cast(observationdate as date) between date_range.start_date and date_range.end_date
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
            order by days desc, symptom asc
            limit 5
        """
        topsymptoms = self.athena_mdh.execQuery(query)

        if topsymptoms.shape[0] <=0:
            return None

        # For each symptom name, lets go ahead and replace '_' with ' '
        tsymptoms = []
        for name, count, days in zip(topsymptoms['symptom'], topsymptoms['total_count'], topsymptoms['days']):
            tsymptoms.append({
                'name': self._capFirst(name.replace('_', ' ')),
                # Athena hands these back as strings. The template compares
                # days against the integer 1 to pick "day" vs "days", and the
                # count-up script parses data-value as a number, so cast here
                # rather than leaving every symptom reading "1 days".
                'count': int(count),
                'days': int(days),
            })

        return tsymptoms

    def _capFirst(self, value: str) -> str:
        """Capitalize the first letter of the string"""
        if len(value) <= 0:
            return value

        return value[0].upper() + value[min(1, len(value)):]

    def _formatReadingDate(self, value) -> str:
        """
        Format a single reading's date for display in the report (eg. 'Sep 24').
        Falls back to the raw value if we can't make sense of it, since a missing
        date should never take down the whole report.
        """
        if value is None:
            return ''

        try:
            if pd.isna(value):
                return ''
        except (TypeError, ValueError):
            pass

        if isinstance(value, str):
            try:
                value = datetime.datetime.fromisoformat(value)
            except ValueError:
                return value

        try:
            return value.strftime('%b %d')
        except AttributeError:
            return str(value)

    def _addCommas(self, value: int) -> str:
        """Add commas in the correct place integer passed and then return a string for it."""
        buff: str = str(value)
        # Hold the sign aside. Grouping the digits with the '-' still attached
        # puts a comma straight after it whenever the digit count is a multiple
        # of three, which produced strings like '-,512'.
        sign = ''
        if buff.startswith('-'):
            sign = '-'
            buff = buff[1:]
        buff = buff[::-1]
        cbuff = ""
        for i in range(0, len(buff)):
            cbuff = cbuff + buff[i]
            if i < len(buff)-1 and (i+1) % 3 == 0:
                cbuff = cbuff + ','
        cbuff = cbuff[::-1]

        return sign + cbuff

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
                'counts_int': 6,
                'above_threshold_counts': 2,
                'high_readings': [
                    {'date': 'Sep 24', 'systolic': 148, 'diastolic': 92},
                    {'date': 'Sep 27', 'systolic': 143, 'diastolic': 88},
                ],
                'is_high': True,
                'trend': trends[random.randint(0, len(trends)-1)],
            }

        elif calling_function_name == 'heartRateSummary':
            return {
                'hr_counts': 12001600,
                'avg_rhr': 62,
            }

        elif calling_function_name == 'hrvSummary':
            return {
                'avg_hrv': 48,
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
                'total_movements_mins': self._addCommas(120),
                'average_steps_int': self._addCommas(4200),
                # Trend can return a positive or negative value.
                'trend': 5000 - random.randint(4500, 5500),
            }

        elif calling_function_name == 'topSymptomsRecorded':
            return [{'name':'Headache',  'count': 4, 'days':4},
                    {'name':'Restless Legs', 'count':3, 'days': 2}]
