import requests, yaml, os
import pickle
import json
import os.path
from collections import OrderedDict
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
from fake_useragent import UserAgent
import datetime
import pytz
import random
import pandas as pd

ua = UserAgent()
ua.update()
tz_india = pytz.timezone('Asia/Kolkata')

def fetch_states():
    r = requests.get('https://cdn-api.co-vin.in/api/v2/admin/location/states')
    return pd.DataFrame(r.json()['states']).set_index('state_name')['state_id'].to_dict()

def fetch_disticts(state_id=16):
    r = requests.get('https://cdn-api.co-vin.in/api/v2/admin/location/districts/%d'%state_id)
    return pd.DataFrame(r.json()['districts'])

def _get_slots_by_pincode(pincode="600096"):
    today = datetime.datetime.now(tz_india).strftime("%d-%m-%Y")
    URL = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=%s&date=%s'%(pincode, today)
    headers = random.choice(headers_list)
    headers['User-Agent'] = ua.random
    r = requests.Session()
    r.headers = headers
    resp = r.get(URL)
    return resp

def _get_slots_by_district(district="571"):
    today = datetime.datetime.now(tz_india).strftime("%d-%m-%Y")
    URL = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=%s&date=%s'%(district, today)
    headers = random.choice(headers_list)
    headers['User-Agent'] = ua.random
    r = requests.Session()
    r.headers = headers
    resp = r.get(URL)
    return resp

def get_district(pincode="6000096"):
    resp = _get_slots_by_pincode(pincode)
    if resp.json()['centers']:
        return resp.json()['centers'][0]['district_name']
    else:
        return None

def gen_dist_map():
    states = fetch_states()
    dist_map = []
    for state_name, state_id in states.items():
        districts_dict = fetch_disticts(state_id)
        districts_dict['state_name'] = state_name
        districts_dict['state_id'] = state_id
        dist_map.append(districts_dict)
    dist_map_df = pd.concat(dist_map)
    dist_map_df['district_name'] = dist_map_df['district_name'].str.upper()
    dist_map_df.to_csv('districts.csv')

def load_yaml(fname):
    with open(fname) as creds_file:
        creds = yaml.load(creds_file.read(),Loader=yaml.FullLoader)
    return creds

def send_mail(to_list, subject, message, domain_name, api_key):
    if len(to_list)==0:return
    return requests.post(
        "https://api.mailgun.net/v3/%s/messages"%domain_name,
        auth=("api", api_key),
        data={"from": "COWIN Alerts <noreply@%s>"%domain_name,
              "to": ['vishstar88@gmail.com'],
              "bcc": to_list,
              "subject": subject,
              "html": message})

def load_sheets_creds():
    creds = None
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def read_vals(sheet_id, sheet_range="Main"):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = load_sheets_creds()
    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
    values = result.get('values', [])
    return values

def save_users(creds, cache_path='dist_map.json'):
    sheet_vals = read_vals(sheet_id=creds['sheets']['sheet_id'])
    users_df = pd.DataFrame(sheet_vals[1:],columns=sheet_vals[0])
    users_df.to_csv('users.csv')
    districts_df = pd.read_csv('districts.csv')
    dist_name_to_code = districts_df.set_index('district_name')['district_id'].to_dict()

    dist_map = {}
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            dist_map = json.load(f)

    for pincode in users_df['Pincode'].astype(str).unique():
        if pincode not in dist_map:
            dist_name = get_district(pincode)
            dist_map[pincode] = {'district_name':dist_name, 'district_id':dist_name_to_code.get(str(dist_name).upper())}
    with open(cache_path,'w') as f:
        json.dump(dist_map,f)


with open ('headers.json') as f:
    headers_list = json.load(f)

# Create ordered dict from Headers above
ordered_headers_list = []
for headers in headers_list:
    h = OrderedDict()
    for header,value in headers.items():
        h[header]=value
    ordered_headers_list.append(h)

if __name__ == "__main__":
    print(read_vals())