#!/usr/bin/env python3

from sensorfabric.mdh import MDH
import dotenv
from helper import Helper
from datetime import datetime, timedelta, date

def setup():
    config = dotenv.dotenv_values('/Users/shravan/Amahealth/mdh-ema/.env')
    mdh = MDH(account_secret=config['RKS_PRIVATE_KEY'],
              account_name=config['RKS_SERVICE_ACCOUNT'],
              project_id='ba244766-7bd6-408d-8208-0a29c8949321')

    #participant = mdh.getParticipant('BB-4194-0806')
    #customFields = participant['customFields']
    #print(customFields['uh_sync_timestamp'])
    #print(participant)

    helper = Helper(mdh, 'BB-3234-3734', date.fromisoformat('2025-07-26'))

    return helper

myhelper = setup()
ringwear = myhelper.ringWearTime()
print(ringwear)
weight = myhelper.weightSummary()
print(weight)
movement = myhelper.movementSummary()
print(movement)
symptoms = myhelper.topSymptomsRecorded()
print(symptoms)
sleep = myhelper.sleepSummary()
print(sleep)
temp = myhelper.temperatureSummary()
print(temp)
hr = myhelper.heartRateSummary()
print(hr)
bp = myhelper.bloodPressure()
print(bp)
enrolled_weeks = myhelper.weeksEnrolled()
print(enrolled_weeks)
ga_weeks = myhelper.weeksPregnant()
print(ga_weeks)
# Dates passed are inclusive.
ema_count: int = myhelper.emaCompleted()
print(ema_count)
