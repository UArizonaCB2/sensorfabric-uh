from jinja2 import Template
from datetime import datetime
from typing import Dict, List, Any, Optional
from sensorfabric.mdh import MDH


class TemplateGenerator:
    """
    AWS Lambda function for generating weekly health reports from MDH / UltraHuman API data.
    
    This class handles:
    1. Connecting to MDH/Athena
    2. Running SQL queries to gather data
    3. Generating Jinja2 HTML template
    """
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        self.mdh = None

    def _initialize_connections(self):
        """Initialize MDH connection"""
        try:
            # Initialize MDH connection from env vars
            mdh_configuration = {
                'account_secret': self.__config.get('MDH_SECRET_KEY'),
                'account_name': self.__config.get('MDH_ACCOUNT_NAME'),
                'project_id': self.__config.get('MDH_PROJECT_ID'),
            }
            self.mdh = MDH(**mdh_configuration)
            logger.info("MDH connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MDH connection: {str(e)}")
            raise


    def generate_weekly_report_template(
        participant_id: str,
        target_week: Optional[str] = None,
    ) -> str:
        """
        Generate a Jinja2 HTML template for weekly health reports from Ultrahuman API data.
        
        Args:
            participant_id: ID of the participant to generate the report for
            target_week: Optional week to generate the report for

        Returns:
            HTML string with populated template
            
        Raises:
            Exception: If MDH connection fails
        """
        # initialize connections if not already done
        if self.mdh is None:
            self._initialize_connections()
        # TODO awswrangler.
        # TODO pull data and compute on the fly from participant id.
        # weeks enrolled comes from MDH participant record
        participant = self.mdh.getParticipant(participant_id)
        if target_week is None:
            last_week_utc_timestamp = datetime.datetime.now() - datetime.timedelta(days=7)
            last_week_utc_timestamp_int = int(last_week_utc_timestamp.timestamp())
        else:
            # TODO make target_week processing more robust.
            # TODO need to use participant's timezone? not sure.
            last_week_utc_timestamp = datetime.datetime.strptime(target_week, '%Y-%m-%d') - datetime.timedelta(days=7)
            last_week_utc_timestamp_int = int(last_week_utc_timestamp.timestamp())
        # current_pregnancy_week = math.floor(ga_calculated_today_days / 7)
        # enrolled_date
        # MDH task API:
        # total_surveys = 
        # surveys_completed = 

        # MDH athena SQL:
        # bp_count
        # -
        # bp_trend
        # -
        # bp_high_readings
        # -

        # TODO get mdh participant-based states
        # uh data from s3/athena based data - SQL queries:
        # awsrangler here pointing to correct path.
        # ring_wear_percentage
        # - 
        # heart_rate_total_beats
        # - SELECT COUNT(*)
        #   FROM hr
        #   WHERE pid = <participant_id>
        #     AND object_values_timestamp >= <last_week_utc_timestamp>;

        # heart_rate_avg_resting
        # - SELECT avg(object_values_value)
        #   FROM hr
        #   WHERE pid = <participant_id>
        #     AND object_values_timestamp >= <last_week_utc_timestamp>;

        # temperature_total_readings
        # - SELECT COUNT(*)
        #   FROM temp
        #   WHERE pid = <participant_id>
        #     AND object_values_timestamp >= <last_week_utc_timestamp>;
        # temperature_fever_readings
        # - SELECT COUNT(*)
        #   FROM temp
        #   WHERE pid = <participant_id>
        #     AND object_values_timestamp >= <last_week_utc_timestamp>
        #     AND object_values_value >= FEVER_THRESHOLD;
        # sleep_total_hours
        # -
        # sleep_avg_per_night
        # -
        # weight_weekly_change
        # -
        # weight_total_change
        # -
        # movement_total_minutes
        # -
        # movement_avg_steps_per_day
        # -
        template_str = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Weekly Health Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .metric { margin: 10px 0; padding: 10px; background-color: #f5f5f5; border-radius: 5px; }
                .trend-positive { color: green; }
                .trend-negative { color: red; }
                .trend-neutral { color: #666; }
            </style>
        </head>
        <body>
            <h1>Weekly Health Report</h1>
            
            <div class="metric">
                <strong>Enrollment Status:</strong> {{ weeks_enrolled }} weeks enrolled
            </div>
            
            <div class="metric">
                <strong>Pregnancy Progress:</strong> {{ current_pregnancy_week }} weeks - {{ current_pregnancy_week + 1 }} weeks pregnant
            </div>
            
            <div class="metric">
                <strong>Device Usage:</strong> {{ ring_wear_percentage }}% ring wear time
            </div>
            
            <div class="metric">
                <strong>Survey Completion:</strong> {{ surveys_completed }} of {{ total_surveys }} surveys completed
            </div>
            
            {% if blood_pressure_enabled %}
            <div class="metric">
                <strong>Blood Pressure:</strong> You recorded {{ bp_count }} blood pressures this week. 
                Blood pressure trend: {{ bp_trend }} as last week. 
                Number of blood pressures over 140/90 = {{ bp_high_readings or "none" }}
            </div>
            {% endif %}
            
            {% if heart_rate_enabled %}
            <div class="metric">
                <strong>Heart Rate:</strong> {{ heart_rate_total_beats }} heart beats recorded. 
                Average resting heart rate of {{ heart_rate_avg_resting }}
            </div>
            {% endif %}
            
            {% if temperature_enabled %}
            <div class="metric">
                <strong>Temperature:</strong> Total temperature readings = {{ temperature_total_readings }}. 
                Trending = {{ metrics.temperature.trend }}
                <br>
                {{ temperature_fever_readings or "No" }} temperatures over 100.0°F recorded
            </div>
            {% endif %}
            
            {% if sleep_enabled %}
            <div class="metric">
                <strong>Sleep:</strong> {{ sleep_total_hours }} hours of sleep this week. 
                Average {{ sleep_avg_per_night }} per night.
            </div>
            {% endif %}
            
            {% if weight_enabled %}
            <div class="metric">
                <strong>Weight:</strong> Change in weight this week = {{ weight_weekly_change }} lbs. 
                Total change {{ weight_total_change }} lbs since {{ enrolled_date }}
            </div>
            {% endif %}
            
            {% if movement_enabled %}
            <div class="metric">
                <strong>Movement:</strong> Total movement this week = {{ movement_total_minutes }} minutes. 
                Average steps per day = {{ movement_avg_steps_per_day }}
                <br>
                Trend - {{ movement_step_trend }} 
                {{ "fewer" if movement_step_trend < 0 else "more" }} than last week
            </div>
            {% endif %}
            
            <div class="metric">
                <small>Report generated on {{ report_date }}</small>
            </div>
        </body>
        </html>
        """
        
        template = Template(template_str)
        
        return template.render(
            weeks_enrolled=weeks_enrolled,
            current_pregnancy_week=current_pregnancy_week,
            ring_wear_percentage=ring_wear_percentage,
            surveys_completed=surveys_completed,
            total_surveys=total_surveys,
            bp_count=bp_count,
            bp_trend=bp_trend,
            bp_high_readings=bp_high_readings,
            enrolled_date=enrolled_date,
            sleep_total_hours=sleep_total_hours,
            sleep_avg_per_night=sleep_avg_per_night,
            movement_total_minutes=movement_total_minutes,
            movement_avg_steps_per_day=movement_avg_steps_per_day,
            movement_step_trend=movement_step_trend,
            # enabled flags
            blood_pressure_enabled=True,
            heart_rate_enabled=True,
            temperature_enabled=True,
            sleep_enabled=True,
            weight_enabled=True,
            movement_enabled=True,
            report_date=datetime.now().strftime("%Y-%m-%d %H:%M")
        )
