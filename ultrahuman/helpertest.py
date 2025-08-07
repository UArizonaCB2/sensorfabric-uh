#!/usr/bin/env python3

from sensorfabric.mdh import MDH
from sensorfabric.athena import athena
import dotenv
from helper import Helper
from datetime import datetime, timedelta, date

dev_id = 'ba244766-7bd6-408d-8208-0a29c8949321'
prod_id = '8c08c4c6-f56d-408c-9db7-706d3a9f730e'

def setup():
    config = dotenv.dotenv_values('/Users/shravan/Amahealth/mdh-ema/.env')
    mdh = MDH(account_secret=config['RKS_PRIVATE_KEY'],
              account_name=config['RKS_SERVICE_ACCOUNT'],
              project_id=prod_id)

    #participant = mdh.getParticipant('BB-4194-0806')
    #customFields = participant['customFields']
    #print(customFields['uh_sync_timestamp'])
    #print(participant)
    amdh = athena(database='mdh_export_database_rk_033fa2f7_flinn_study_prod',
                  workgroup='mdh_export_database_external_prod',
                  s3_location='s3://pep-mdh-export-database-prod/execution/rk_033fa2f7_flinn_study')

    # Going to create a simple athena connection to connect to all the UH things.
    auh = athena(database='uh-biobayb-prod',
                 workgroup='biobayb-uh',
                 s3_location='s3://uoa-biobayb-uh-dev/results/')

    helper = Helper(mdh, amdh, auh, 'BB-3234-3734', date.fromisoformat('2025-08-05'))

    return helper

myhelper = setup()

hr = myhelper.heartRateSummary()
print(hr)
exit()
ringwear = myhelper.ringWearTime()
print(ringwear)
temp = myhelper.temperatureSummary()
print(temp)
topsymptoms = myhelper.topSymptomsRecorded()
print(topsymptoms)
weight = myhelper.weightSummary()
print(weight)
bp = myhelper.bloodPressure()
print(bp)
movement = myhelper.movementSummary()
print(movement)
sleep = myhelper.sleepSummary()
print(sleep)
hr = myhelper.heartRateSummary()
print(hr)
nrolled_weeks = myhelper.weeksEnrolled()
print(enrolled_weeks)
ga_weeks = myhelper.weeksPregnant()
print(ga_weeks)
# Dates passed are inclusive.
ema_count: int = myhelper.emaCompleted()
print(ema_count)
