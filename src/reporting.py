import reports.optum_care

report = reports.optum_care.main()
import pandas as pd

df1 = pd.read_csv('docs/report_oc.csv',index_col='Total')
c= pd.merge(df1, report, how='left', left_index=True,right_index=True)

c.to_csv('docs/report_oc.csv')

