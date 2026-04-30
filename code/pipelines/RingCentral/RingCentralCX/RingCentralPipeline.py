import os
import sys

from ringcentral import SDK
from dotenv import load_dotenv
import pandas as pd

env_dir = os.path.dirname(__file__)
env_path = os.path.join(env_dir, '.env')
load_dotenv(env_path)

CALLER = "+19156211063"
RECIPIENT = ""

JWT_TOKEN = "eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiMTE2MjgxNjA0OCIsImlzcyI6Imh0dHBzOi8vcGxhdGZvcm0ucmluZ2NlbnRyYWwuY29tIiwiZXhwIjozODg0MjkzNjgwLCJpYXQiOjE3MzY4MTAwMzMsImp0aSI6IkpxQVNrNnM5Ui1pOER1VUtsVHdXV1EifQ.BlvfD-CUAYn09w-9i89kORQpaoKZ7WDHKnkXMZloM9zAdhdPBeB2GjBncUNxNZtN181TXafNSTlBas9OoGDoE4mPiSCiQK9Gl9zzX_A0TJtquPBlxyLZ2mRMFHdrl1XMGyngf4OgO2IVtGVUAGNVQFq5MTDUNF1WrFiOfY12Tv6gJnc3JVF90xtEiPCKa_FZ51h37eVLcsNMQpe1as3dYbzBxsVUmNUCCeXqo0M862KJ-8LsRwrRuJ2jgjcsJbVCzYXgfl32P8hiLYXULwA4vGqAHKsDjYfmRMvnV_CQQb4iyS_nfgrg6I1uTIon6wK7LjugDDBIFYAa4mB92YnbCQ"

RC_SERVER_URL = os.getenv('RC_SERVER_URL')
RC_APP_CLIENT_ID = os.getenv('RC_APP_CLIENT_ID')
RC_APP_CLIENT_SECRET = os.getenv('RC_APP_CLIENT_SECRET')
RC_USER_JWT = os.getenv('RC_USER_JWT')


rcsdk = SDK( RC_APP_CLIENT_ID,
             RC_APP_CLIENT_SECRET,
             RC_SERVER_URL )
platform = rcsdk.platform()

try:
  platform.login( jwt=os.environ.get('RC_USER_JWT') )
except Exception as e:
  sys.exit("Unable to authenticate to platform. Check credentials." + str(e))


def get_Ids(queryParam:str):
  Ids = {
    'id':[],
    'name':[]
    }
  try:
    queryParams = {
        'type':[queryParam],
        'perPage':10_000,
        
    }
    endpoint = "/restapi/v1.0/account/~/extension/"
    resp = platform.get(endpoint, queryParams)
    jsonObj = resp.json()
    
    for record in jsonObj.records:
        name = record.__dict__.get('name', '')
        id = record.__dict__.get('id', '')
        
        Ids['id'].append(str(id))
        Ids['name'].append(name)
    
    return Ids
  
  except Exception as e:
    print('Unable to get users: ' + str(e))

def read_analytics_timeline_grouped_by_users():
  userIds = get_Ids("Department")
  try:
    bodyParams = {
      'grouping': {
        'groupBy': "Queues",
        'keys': userIds['id']
      },
      'timeSettings':{
        'timeZone': "America/Los_Angeles",
        'timeRange': {
          # Change the "timeFrom" value accordingly so that it does not exceed 184 days from the current date and time
          # The specified time is UTC time. If you want the timeFrom and timeTo your local time, you have to convert
          # your local time to UTC time!
          'timeFrom': "2025-01-01T00:00:00.000Z",
          'timeTo': "2025-01-15T23:59:59.999Z"
        },
        'advancedTimeSettings': {
          'includeDays': [ "Sunday" ],
          'includeHours': [
            {
              'from': "00:00",
              'to': "23:59"
            }
          ]
        }
      },
      'responseOptions': {
        'counters': {
          'allCalls': True
        }
      }
    }

    queryParams = {
      'interval': "Day",
      'perPage': 10
    }

    endpoint = '/analytics/calls/v1/accounts/~/timeline/fetch'
    resp = platform.post(endpoint, bodyParams, queryParams)
    print(resp.json().data.__dict__)
    # records = resp.json().data.records
    # for row in records:
    #     for i in row.points:
    #       print(i.__dict__)
    
  except Exception as e:
    print ("Unable to read analytics timeline. ", str(e))
    
print(len(get_Ids("Department")['id']))