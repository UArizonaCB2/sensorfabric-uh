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
              project_id=dev_id)

    #participant = mdh.getParticipant('BB-4194-0806')
    #customFields = participant['customFields']
    #print(customFields['uh_sync_timestamp'])
    #print(participant)
    amdh = athena(database='mdh_export_database_rk_033fa2f7_flinn_study_prod',
                  workgroup='mdh_export_database_external_prod',
                  s3_location='s3://pep-mdh-export-database-prod/execution/rk_033fa2f7_flinn_study')

    helper = Helper(mdh, amdh, None, 'MDH-9064-8651', date.fromisoformat('2025-07-10'))

    return helper

myhelper = setup()
topsymptoms = myhelper.topSymptomsRecorded()
print(topsymptoms)
exit()
weight = myhelper.weightSummary()
print(weight)
bp = myhelper.bloodPressure()
print(bp)
ringwear = myhelper.ringWearTime()
print(ringwear)
movement = myhelper.movementSummary()
print(movement)
sleep = myhelper.sleepSummary()
print(sleep)
temp = myhelper.temperatureSummary()
print(temp)
hr = myhelper.heartRateSummary()
print(hr)
nrolled_weeks = myhelper.weeksEnrolled()
print(enrolled_weeks)
ga_weeks = myhelper.weeksPregnant()
print(ga_weeks)
# Dates passed are inclusive.
ema_count: int = myhelper.emaCompleted()
print(ema_count)
