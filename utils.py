import requests, yaml, os
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
import pandas as pd

def load_yaml(fname):
    with open(fname) as creds_file:
        creds = yaml.load(creds_file.read(),Loader=yaml.FullLoader)
    return creds

def send_mail(to_list, message, domain_name, api_key):
    return requests.post(
        "https://api.mailgun.net/v3/%s/messages"%domain_name,
        auth=("api", api_key),
        data={"from": "COWIN Alerts <noreply@%s>"%domain_name,
              "to": ['vishstar88@gmail.com'],
              "bcc": to_list,
              "subject": "Vaccine Slots found!",
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

def save_users(creds):
    sheet_vals = read_vals(sheet_id=creds['sheets']['sheet_id'])
    pd.DataFrame(sheet_vals[1:],columns=sheet_vals[0]).to_csv('users.csv')

if __name__ == "__main__":
    print(read_vals())