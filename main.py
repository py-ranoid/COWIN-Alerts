import requests, datetime, time
import pytz
import pandas as pd
import utils
import random

REFRESH_INTERVAL = 15
SEND_ALL_INTERVAL = 240
WAIT_INTERVAL = 60
tz_india = pytz.timezone('Asia/Kolkata')

def get_slots_by_pincode(pincode="600096"):
    URL = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=%s&date=%s'%(pincode,datetime.datetime.now(tz_india).strftime("%d-%m-%Y"))
    headers = random.choice(utils.headers_list)
    r = requests.Session()
    r.headers = headers
    resp = r.get(URL)
    if not resp.status_code == 200: return pd.DataFrame([]),pd.DataFrame([]),resp.status_code
    all_sessions = [(center['name'], session['date'], session['available_capacity']) for center in resp.json()['centers'] for session in center['sessions'] if session['min_age_limit']==18]
    all_sessions = pd.DataFrame(all_sessions,columns=['CenterName','Date','Slots'])
    return all_sessions, all_sessions[all_sessions['Slots']>0],200

def update_users(creds):
    utils.save_users(creds)
    users_df = pd.read_csv('users.csv')
    new_users = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(tz_india)>datetime.datetime.now(tz_india)-datetime.timedelta(minutes=REFRESH_INTERVAL)]
    print (new_users)

    utils.send_mail(to_list=new_users['Email Address'].tolist(), subject = "Subscribed to COWIN-Alerts.",
                    message = str("Thanks for signing up. COWIN-Alerts will check for slots (18-44yrs) in your pincode every minute and send you an email if any centers have slots free. These alerts will be live for one week, post which you'll need to register again. <br> Form link : bit.ly/cowin-alerts <br> Source code : github.com/py-ranoid/COWIN-Alerts "),
                    domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])

def main(creds,send_all=False):
    users_df = pd.read_csv('users.csv')
    users_df = users_df[pd.to_datetime(users_df['Timestamp']).dt.tz_localize(tz_india)>datetime.datetime.now(tz_india)-datetime.timedelta(days=7)]
    for pincode, df in users_df.groupby('Pincode'):
        pincode = str(pincode)
        all_sessions, available_sessions, status_code = get_slots_by_pincode(pincode)
        if not send_all and len(available_sessions)==0:
            print ("%s | %d | PINCODE :%s\t- NO SLOTS AVAILABLE"%(str(datetime.datetime.now(tz_india))[:16], status_code, pincode))
            continue
        message = all_sessions.to_html() if send_all else available_sessions.to_html()
        subject = "Pincode Centers Summary (%s)"%pincode if send_all else "Vaccine Slots found! (%s)"%pincode
        resp = utils.send_mail(to_list=df['Email Address'].tolist(), subject=subject, message = message,
                        domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])
        print (resp)

if __name__ == "__main__":
    iter_num = 0
    creds = utils.load_yaml('creds.yaml')
    update_users(creds)
    print ("Saved Users")

    while True:
        main(creds)
        time.sleep(WAIT_INTERVAL)
        iter_num+=1
        if iter_num%REFRESH_INTERVAL==0:
            utils.save_users(creds)
            update_users(creds)
        if iter_num%SEND_ALL_INTERVAL==0:
            main(creds,True)