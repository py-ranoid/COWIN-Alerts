from collections import Counter
import requests, datetime, time
import pandas as pd
import utils
import random
import json

REFRESH_INTERVAL = 15
SEND_ALL_INTERVAL = 250
WAIT_INTERVAL = 120
NUM_PINGS = {}
MAX_PINGS = 6
def get_slots_by_pincode(pincode="600096"):
    resp = utils._get_slots_by_pincode(pincode)
    if not resp.status_code == 200: return pd.DataFrame([]),pd.DataFrame([]),resp.status_code
    all_sessions = [(center['name'], session['date'], session['available_capacity']) for center in resp.json()['centers'] for session in center['sessions'] if session['min_age_limit']==18]
    all_sessions = pd.DataFrame(all_sessions,columns=['CenterName','Date', 'Slots'])
    return all_sessions, all_sessions[all_sessions['Slots']>0],200

def get_slots_by_district(district="571"):
    resp = utils._get_slots_by_district(district)
    if not resp.status_code == 200: return pd.DataFrame([]),pd.DataFrame([]),resp.status_code

    all_sessions = [(center['name'], session['date'], session['available_capacity'], center['pincode']) for center in resp.json()['centers'] for session in center['sessions'] if session['min_age_limit']==18]
    all_sessions = pd.DataFrame(all_sessions,columns=['CenterName','Date','Slots','Pincode'])
    return all_sessions, all_sessions[all_sessions['Slots']>0], 200

def update_users(creds):
    utils.save_users(creds)
    users_df = pd.read_csv('users.csv')
    new_users = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(utils.tz_india)>datetime.datetime.now(utils.tz_india)-datetime.timedelta(seconds=REFRESH_INTERVAL*WAIT_INTERVAL)]
    print (new_users)

    utils.send_mail(to_list=new_users['Email Address'].tolist(), subject = "Subscribed to COWIN-Alerts.",
                    message = str("Thanks for signing up. COWIN-Alerts will check for slots (18-44yrs) in your pincode every minute and send you an email if any centers have slots free. These alerts will be live for one week, post which you'll need to register again. <br> Form link : bit.ly/cowin-alerts <br> Source code : github.com/py-ranoid/COWIN-Alerts "),
                    domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])

def main(creds,send_all=False):
    #Get all live users
    users_df = pd.read_csv('users.csv')
    NOW = datetime.datetime.now(utils.tz_india)
    users_df = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(utils.tz_india)>NOW-datetime.timedelta(days=7)]
    status_count = []

    # Iterate over all pincodes
    print ("%s - Polling pincodes. Num unique : %r"%(str(NOW)[:16], users_df.Pincode.nunique()))
    for pincode, df in users_df.groupby('Pincode'):

        # Get sessions available in given pincode
        pincode = str(pincode)
        all_sessions, available_sessions, status_code = get_slots_by_pincode(pincode)
        status_count.append(status_code)
        if not status_code == 200:
            continue

        if send_all:
            message = all_sessions.to_html() if len(all_sessions)>0 else "No hospitals/PHCs found in pincode with open or booked slots. <br> You can also sign up for slot alerts in neighboring pincodes here : bit.ly/cowin-alerts "
            subject = "Pincode Centers Summary (%s)"%pincode
        else:
            if (len(available_sessions)==0 or sum(all_sessions['Slots']<2)):
                # print ("%s | %d | PINCODE :%s\t- NO SLOTS AVAILABLE"%(str(NOW)[:16], status_code, pincode))
                continue
            print ("%s | %d | PINCODE :%s\t- SLOTS FOUND"%(str(NOW)[:16], status_code, pincode))
            message = available_sessions.to_html()
            subject = "Vaccine Slots found! (%s)"%pincode

        # Mails summary if send_all else sends available sessions
        resp = utils.send_mail(to_list=df['Email Address'].tolist(), subject=subject, message = message,
                        domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])
    print ("%s - Response Summary : %r"%(str(NOW)[:16],pd.Series(status_count).value_counts().to_dict()))

def main_alt(creds,send_all=False):
    #Get all live users
    users_df = pd.read_csv('users.csv')
    NOW = datetime.datetime.now(utils.tz_india)
    users_df = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(utils.tz_india)>NOW-datetime.timedelta(days=7)]
    status_count = []

    with open('dist_map.json') as f:
        dist_map = json.load(f)
    users_df['district_id'] = users_df['Pincode'].fillna(0).astype(int).astype(str).apply(lambda x:dist_map.get(x,{'district_id':None})['district_id'])
    users_df['district_name'] = users_df['Pincode'].fillna(0).astype(int).astype(str).apply(lambda x:dist_map.get(x,{'district_name':None})['district_name'])

    # Iterate over all pincodes
    print ("%s - Polling district codes. Num unique : %r"%(str(NOW)[:16], users_df.district_id.nunique()))
    print (users_df.shape)
    users_df = users_df.dropna()
    print (users_df.shape)
    for district, df in users_df.groupby('district_id'):
        # Get sessions available in given district
        district = str(int(district))
        district_name = df['district_name'].iloc[0]
        try:all_sessions, available_sessions, status_code = get_slots_by_district(district)
        except: status_code = 666
        status_count.append(status_code)
        if not status_code == 200:
            continue

        if send_all:
            message = all_sessions.to_html() if len(all_sessions)>0 else "No hospitals/PHCs found in your district with open or booked slots. <br> You can also sign up for slot alerts in neighboring pincodes here : bit.ly/cowin-alerts "
            subject = "District Centers Summary (%s)"%district_name
        else:
            if (len(available_sessions)==0 or sum(all_sessions['Slots']<2)):
                print ("%s | %d | DISTRICT : %15s - NO SLOTS AVAILABLE"%(str(NOW)[:16], status_code, district_name[:15]))
                continue
            print ("%s | %d | DISTRICT : %15s - SLOTS FOUND"%(str(NOW)[:16], status_code, district_name[:15]))
            NUM_PINGS[district_name] = NUM_PINGS.get(district_name,0)+1
            message = available_sessions.to_html()
            subject = "Vaccine Slots in your district! (%s)"%district_name
            if NUM_PINGS.get(district_name,0)>MAX_PINGS:
                print ("MAX PINGS REACHED. Skipping Notification")
                continue

        # Mails summary if send_all else sends available sessions
        resp = utils.send_mail(to_list=df['Email Address'].tolist(), subject=subject, message = message,
                        domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])
    print ("%s - Response Summary : %r"%(str(NOW)[:16],pd.Series(status_count).value_counts().to_dict()))

if __name__ == "__main__":
    iter_num = 0
    creds = utils.load_yaml('creds.yaml')
    utils.save_users(creds)
    print ("Saved Users")

    while True:
        main_alt(creds)
        time.sleep(WAIT_INTERVAL)
        iter_num+=1
        print ("ITER : %d"%iter_num)
        if iter_num%REFRESH_INTERVAL==0:
            utils.save_users(creds)
            update_users(creds)
            NUM_PINGS = {}
        if iter_num%SEND_ALL_INTERVAL==0:
            main_alt(creds,True)