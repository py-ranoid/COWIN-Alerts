import requests, datetime, time
import pandas as pd
import utils
REFRESH_INTERVAL = 15
SEND_ALL_INTERVAL = 120
WAIT_INTERVAL = 60

def get_slots_by_pincode(pincode="600096",):
    URL = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=%s&date=%s'%(pincode,datetime.datetime.now().strftime("%d-%m-%Y"))
    resp = requests.get(URL)
    if not resp.status_code == 200: return []
    all_sessions = [(center['name'], session['date'], session['available_capacity']) for center in resp.json()['centers'] for session in center['sessions'] if session['min_age_limit']==18]
    all_sessions = pd.DataFrame(all_sessions,columns=['CenterName','Date','Slots'])
    return all_sessions, all_sessions[all_sessions['Slots']>0]

def update_users(creds):
    utils.save_users(creds)
    users_df = pd.read_csv('users.csv')
    new_users = users_df[pd.to_datetime(users_df['Timestamp'])>datetime.datetime.now()-datetime.timedelta(minutes=REFRESH_INTERVAL)]
    utils.send_mail(to_list=new_users['Email Address'].tolist(), message = str("Thanks for signing up to COWIN alerts"),
                    domain_name=creds['mailgun']['domain'], api_key=creds['mailgun']['key'])

def main(creds,send_all=False):
    users_df = pd.read_csv('users.csv')
    users_df = users_df[pd.to_datetime(users_df['Timestamp'])>datetime.datetime.now()-datetime.timedelta(days=7)]
    for pincode, df in users_df.groupby('Pincode'):
        all_sessions, available_sessions = get_slots_by_pincode(str(pincode))
        if not send_all and len(available_sessions)==0:
            print ("PINCODE :%r\t- NO SLOTS AVAILABLE"%pincode)
            continue
        message = all_sessions.to_html() if send_all else available_sessions.to_html()
        resp = utils.send_mail(to_list=df['Email Address'].tolist(), message = message,
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
            utils.save_users()
            update_users()
        if iter_num%SEND_ALL_INTERVAL==0:
            main(creds,True)