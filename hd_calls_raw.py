import civis
import requests
login_url = 'https://www.hubdialer.com/hq/'
root_url = 'https://www.hubdialer.com/hq/index.php/campaignhq/reports_cli/'
call_attempts_url ="{root_url}households_report/call_attempts/{campaign_id}/{start_date}/{end_date}/{start_time}/{end_time}"
auth_data = { 'email': 'olga@jbforilgov.com',
              'password': '*****',
              'login_attempt': '1' }
url_params = { 'root_url': root_url,
 'campaign_id': '14558',
 'start_date': '2018-11-04',
 'end_date': '2018-11-07',
 'start_time': '09:00',
 'end_time': '22:00' }
s = requests.Session()
s.post(login_url, data=auth_data)
report = s.get(call_attempts_url.format(**url_params),stream=True)
with open('/tmp/call_attempts_14558.csv','wb') as ef:
    for chunk in report.iter_content(chunk_size=1024):
        if chunk:
            ef.write(chunk)
fut = civis.io.csv_to_civis('/tmp/call_attempts_14558.csv',
                           'JBPritzker',
                           'jb4gov.raw_14558',
                            headers=True,
                           existing_table_rows='truncate')
fut.result() 
print("raw_14558 updated")
