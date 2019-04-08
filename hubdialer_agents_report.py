import civis
import requests
from datetime import date, timedelta
login_url = 'https://www.hubdialer.com/hq/'
root_url = 'https://www.hubdialer.com/hq/index.php/campaignhq/reports_cli/'
agent_report_url = "{root_url}agent_report/0/{start_date}/{end_date}/{start_time}/{end_time}/0/0/{client_id}"
auth_data = { 'email': 'olga@jbforilgov.com',
              'password': '*****',
              'login_attempt': '1' }
yesterday = date.today() - timedelta(1)
url_params = { 'root_url': root_url,
 'start_date': yesterday.strftime('%Y-%m-%d'),
 'end_date': yesterday.strftime('%Y-%m-%d'),
 'start_time': '09:00',
 'end_time': '22:00', 
 'client_id': '1314'}
s = requests.Session()
s.post(login_url, data=auth_data)
report = s.get(agent_report_url.format(**url_params),stream=True)
with open('/tmp/agents.csv','wb') as ef:
    for chunk in report.iter_content(chunk_size=1024):
        if chunk:
            ef.write(chunk)
fut = civis.io.csv_to_civis('/tmp/agents.csv',
                           'JBPritzker',
                           'jb4gov.hubdialer_agents_raw',
                            headers=True,
                           existing_table_rows='append')
fut.result() 
print("hubdialer_agents_raw updated")
sql = ('drop table if exists jb4gov.hubdialer_minutes_report; '
'create table jb4gov.hubdialer_minutes_report as '
'select orgcode.pod '
',orgcode.region '
',orgcode.org_code '
',date_trunc(\'week\',hdagents.logged_in)::date reporting_week '
',sum(hdagents._minutes_logged_in) minutes_logged '
'from jb4gov.hubdialer_agents_raw hdagents '
'join jb4gov.fo_code_pod_turf_type orgcode on orgcode.org_code = upper(right(hdagents.name,3)) '
'group by 1,2,3,4') 
query_fut = civis.io.query_civis(sql,
                            'JBPritzker')
query_fut.result()
print("I'm done!")
