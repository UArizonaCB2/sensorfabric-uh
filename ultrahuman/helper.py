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
                select cast(systolic as double) systolic, cast(diastolic as double) diastolic,
                       'current' as week_type
                from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and cast(datetimelocal as date) between date('{start_date_str}') and date('{end_date_str}')
            ),
            previous_week as (
                select cast(systolic as double) systolic, cast(diastolic as double) diastolic,
                       'previous' as week_type
                from omronbloodpressure
                where participantIdentifier = '{self.participant_id}'
                and cast(datetimelocal as date) between date('{prev_start_str}') and date('{prev_end_str}')
            )
            select * from current_week
            union all
            select * from previous_week
        """

        combined_data: pd.DataFrame = self.athena_mdh.execQuery(combined_query)
        
        # Split the results back into current and previous weeks
        this_week = combined_data[combined_data['week_type'] == 'current'][['systolic', 'diastolic']]
        previous_week = combined_data[combined_data['week_type'] == 'previous'][['systolic', 'diastolic']]

        # If we did not get any BP data for this week, we just return none.
        if this_week.shape[0] <= 0:
            return None

        # Data is already cast to double in SQL, no need for additional pandas conversion
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

        # Calculate UTC timestamp range for the current week
        start_ts, end_ts = self._get_utc_timestamp_range(self.start_date, self.end_date)

        query = f"""
            select
                cast(floor(avg(rhr.object_values_value)) as int) avg_rhr,
                (select count(*) from hr 
                 where pid = '{self.participant_id}'
                 and object_values_timestamp >= {start_ts}
                 and object_values_timestamp < {end_ts}) hr_counts
            from night_rhr rhr
            where rhr.pid = '{self.participant_id}'
                and rhr.object_values_timestamp >= {start_ts}
                and rhr.object_values_timestamp < {end_ts}
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
        Get the sleep summary values for this week.
        """
        if os.getenv('TEMPLATE_MODE', 'PRODUCTION') == 'PRESENT':
            return self._debugOutputs()

        return None

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
                    cast(floor(avg(case when cast("date" as date) between date('{curr_start_str}') and date('{end_date_str}') 
                        then case units when 'lb' then cast(value as double) else cast(value as double) * 2.20462 end 
                        end)) as int) curr_avg_weight,
                    cast(floor(avg(case when cast("date" as date) between date('{prev_start_str}') and date('{prev_end_str}') 
                        then case units when 'lb' then cast(value as double) else cast(value as double) * 2.20462 end 
                        end)) as int) prev_avg_weight
                from healthkitv2samples
                where type = 'Weight'
                    and participantidentifier = '{self.participant_id}'
                    and (
                        cast("date" as date) between date('{curr_start_str}') and date('{end_date_str}') or
                        cast("date" as date) between date('{prev_start_str}') and date('{prev_end_str}')
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
        Get movement summary values in the past week.
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
        with daily_steps as (
            select 
                date(from_unixtime(object_values_timestamp)) step_date,
                sum(object_values_value) total_steps,
                case 
                    when object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts} then 'current'
                    when object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts} then 'previous'
                    else 'unknown'
                end period_type
            from steps
            where pid = '{self.participant_id}'
                and (
                    (object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts}) or
                    (object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts})
                )
            group by date(from_unixtime(object_values_timestamp)), 
                case 
                    when object_values_timestamp >= {curr_start_ts} and object_values_timestamp < {curr_end_ts} then 'current'
                    when object_values_timestamp >= {prev_start_ts} and object_values_timestamp < {prev_end_ts} then 'previous'
                    else 'unknown'
                end
        )
        select 
            cast(floor(avg(case when period_type = 'current' then total_steps end)) as int) avg_curr,
            cast(floor(avg(case when period_type = 'previous' then total_steps end)) as int) avg_prev
        from daily_steps
        """

        movement = self.athena_uh.execQuery(query)
        if movement.shape[0] <= 0:
            return None

        avg_steps = None
        steps_changed = None
        try:
            avg_curr = movement['avg_curr'][0]
            avg_prev = movement['avg_prev'][0]
            if avg_curr is not None:
                avg_steps = int(avg_curr)
            if avg_curr is not None and avg_prev is not None:
                steps_changed = int(avg_curr) - int(avg_prev)
        except:
            return None

        return {
                'total_movements_mins': None,
                'average_steps_int': self._addCommas(avg_steps),
                # Trend can return a positive or negative value.
                'trend': self._addCommas(steps_changed),
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
            if i < len(buff)-1 and (i+1) % 3 == 0:
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
                'total_movements_mins': self._addCommas(120),
                'average_steps_int': self._addCommas(4200),
                # Trend can return a positive or negative value.
                'trend': 5000 - random.randint(4500, 5500),
            }

        elif calling_function_name == 'topSymptomsRecorded':
            return [{'name':'Headache',  'count': 4, 'days':4},
                    {'name':'Restless Legs', 'count':3, 'days': 2}]
