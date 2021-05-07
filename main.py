from collections import Counter
import requests, datetime, time
import pandas as pd
import utils
import random

REFRESH_INTERVAL = 10
SEND_ALL_INTERVAL = 180
WAIT_INTERVAL = 150

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
    users_df = pd.read_csv('users.csv')
    users_df = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(utils.tz_india)>datetime.datetime.now(utils.tz_india)-datetime.timedelta(days=7)]
    status_count = []
    for pincode, df in users_df.groupby('Pincode'):
        pincode = str(pincode)
        all_sessions, available_sessions, status_code = get_slots_by_pincode(pincode)
        status_count.append(status_code)
        if not send_all and (len(available_sessions)==0 or sum(all_sessions['Slots']<2)):
            print ("%s | %d | PINCODE :%s\t- NO SLOTS AVAILABLE"%(str(datetime.datetime.now(utils.tz_india))[:16], status_code, pincode))
            continue
        if not status_code == 200: continue
        if send_all:
            message = all_sessions.to_html() if len(all_sessions)>0 else "No hospitals/PHCs found in pincode with open or booked slots. <br> You can also sign up for slot alerts in neighboring pincodes here : bit.ly/cowin-alerts "
            subject = "Pincode Centers Summary (%s)"%pincode
        else:
            print ("%s | %d | PINCODE :%s\t- SLOTS FOUND"%(str(datetime.datetime.now(utils.tz_india))[:16], status_code, pincode))
            message = available_sessions.to_html()
            subject = "Vaccine Slots found! (%s)"%pincode
        resp = utils.send_mail(to_list=df['Email Address'].tolist(), subject=subject, message = message,
                        domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])
    print (Counter(status_count))

if __name__ == "__main__":
    iter_num = 0
    creds = utils.load_yaml('creds.yaml')
    utils.save_users(creds)
    print ("Saved Users")

    while True:
        main(creds)
        time.sleep(WAIT_INTERVAL)
        iter_num+=1
        print ("ITER : %d"%iter_num)
        if iter_num%REFRESH_INTERVAL==0:
            utils.save_users(creds)
            update_users(creds)
        if iter_num%SEND_ALL_INTERVAL==0:
            main(creds,True)