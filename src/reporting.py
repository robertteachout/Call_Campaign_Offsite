import pandas as pd

import reports.project_breakdown
import reports.optum_aca
import reports.project_breakdown_full

def main():
    oc_name  = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']
    aca_name = ['ACA-HospitalCR','ACA-PhysicianCR']

    optum_care = reports.project_breakdown.main(oc_name)
    optum_aca  = reports.project_breakdown.main(aca_name)

    report_oc_full = pd.read_csv('docs/report_oc.csv',index_col='Total')
    report_aca_full = pd.read_csv('docs/report_aca.csv',index_col='Total')
    oc= pd.merge(report_oc_full, optum_care, how='left', left_index=True, right_index=True)
    aca= pd.merge(report_aca_full, optum_aca, how='left', left_index=True, right_index=True)

    oc.to_csv('docs/report_oc.csv')
    aca.to_csv('docs/report_aca.csv')

if __name__ == '__main__':
    reports.project_breakdown_full.main()
    # main()