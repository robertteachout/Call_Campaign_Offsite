import server.secret
import server.query
import server.queries.reschedule
import server.queries.mastersiteID
import server.queries.last_call_search
import server.queries.project_breakdown
import log.log as log
from datetime import date
from pipeline.etc import daily_piv, time_check, next_business_day, x_Bus_Day_ago, Next_N_BD, date_list_split, last_business_day
import pandas as pd
servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
today = date.today()
tomorrow = next_business_day(today)
yesterday = last_business_day(today)
yesterday_str = yesterday.strftime(date_format)

### Add to query
project_breakdown = server.queries.project_breakdown.sql(yesterday_str)
project_breakdown = server.query.query(servername, database,  project_breakdown, 'project_breakdown')

cc = project_breakdown[['OutreachId','ProjectType']]
cc_agg = cc.drop_duplicates().groupby('ProjectType')['OutreachId'].count()

nic = project_breakdown[['OutreachId','ProjectType','disp_count']]
nic_agg = nic.drop_duplicates().groupby(['ProjectType'])['disp_count'].count() #, 'Disp_Name'

cf = project_breakdown[['OutreachId','ProjectType', 'NoteType', 'CallNoteFlag']]
cf_agg = cf.drop_duplicates().groupby(['ProjectType'])['CallNoteFlag'].sum() #, 'Disp_Name'
final = (pd.concat([cc_agg, nic_agg, cf_agg], axis=1))

final['nic_conversion'] = final.disp_count / final.OutreachId 
final['cf_conversion'] = final.CallNoteFlag / final.OutreachId 
print(final.sort_values(by='OutreachId',ascending=False))
