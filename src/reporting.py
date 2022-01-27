import pandas as pd

import reports.project_breakdown
import reports.project_breakdown_full
import reports.campaign_project_score
import reports.campaign_disp

def main():
    oc_name  = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']
    aca_name = ['ACA-HospitalCR','ACA-PhysicianCR']

    # reports.project_breakdown.main(oc_name, 'docs/report_oc.csv')
    # reports.project_breakdown.main(aca_name, 'docs/report_aca.csv')
    # reports.campaign_project_score.main('2022-01-27')
    reports.campaign_disp.main(projects=aca_name)

if __name__ == '__main__':
    main()