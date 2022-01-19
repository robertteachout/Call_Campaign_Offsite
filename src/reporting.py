import reports.optum_care
import reports.optum_aca

optum_care = reports.optum_care.main()
optum_aca = reports.optum_aca.main()
import pandas as pd

report_oc_full = pd.read_csv('docs/report_oc.csv',index_col='Total')
report_aca_full = pd.read_csv('docs/report_aca.csv',index_col='Total')
oc= pd.merge(report_oc_full, optum_care, how='left', left_index=True, right_index=True)
aca= pd.merge(report_aca_full, optum_aca, how='left', left_index=True, right_index=True)

oc.to_csv('docs/report_oc.csv')
aca.to_csv('docs/report_aca.csv')