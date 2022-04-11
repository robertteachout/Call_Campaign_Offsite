import numpy as np
import pandas as pd


def special_handling(df):
    f1 = df["Agent"] == "Special Handling"
    df["Skill"] = np.where(f1, "PC_Special_Handling", df["Skill"])
    return df


def CC_Genpact_Scheduling(df):
    f1 = df.Project_Type.isin(
        [
            "Oscar",
            "ACA-HospitalCR",
            "Aetna Commercial",
        ]
    )
    f2 = df.Retrieval_Team == "Genpact Offshore"
    df["Skill"] = np.where((f1 & f2), "CC_Genpact_Scheduling", df["Skill"])
    return df


def fire_flag(df, skill_name):
    filer1 = df["Score"].str[:1]
    df["Skill"] = np.where(filer1, skill_name, df["Skill"])
    return df


def CC_Pend_Eligible(df):
    filter1 = df["CallCount"] >= 5
    filter2 = df["Outreach_Status"] == "Unscheduled"
    df["Skill"] = np.where(filter1 & filter2, "CC_Pend_Eligible", df["Skill"])
    return df


def emr_rm(df):
    f1 = df["Retrieval_Group"] == "EMR Remote"
    df["Skill"] = np.where(f1, "EMR_Remote_removed", df["Skill"])
    return df


def HIH_rm(df):
    f1 = df["Retrieval_Group"] == "HIH - Other"
    df["Skill"] = np.where(f1, "HIH_removed", df["Skill"])
    return df


def Onsite_rm(df):
    f1 = df["Retrieval_Group"] == "Onsite"
    df["Skill"] = np.where(f1, "Onsite_removed", df["Skill"])
    return df


def Osprey(df):
    f1 = df["Project_Type"] == "Osprey"
    f2 = df["Outreach_Status"] != "Scheduled"
    f3 = df["Outreach_Status"] != "Escalated"

    f4 = df["Outreach_Status"] == "Escalated"
    f5 = df["Outreach_Status"] == "PNP Released"
    # removeing inventory
    df["Skill"] = np.where(f1, "CC_Osprey_Outbound", df["Skill"])
    df["Skill"] = np.where(f1 & (f4 | f5), "Osprey_Escalation", df["Skill"])
    return df


def rm_schedule(df, ls):
    f1 = df["Outreach_Status"] == "Scheduled"
    f2 = df["Last_Call"].notna()
    f3 = df["Project_Type"].isin(ls)
    df["Skill"] = np.where(f1 & f2 & ~f3, "schedule_pull", df["Skill"])
    return df


def adhoc1(df):
    f1 = df["Project_Type"].isin(["Aetna Commercial", "Aetna Medicare"])
    ls = df[f1].sort_values("age", ascending=False)["OutreachID"][:3000]

    f6 = df["OutreachID"].isin(ls)
    df["Skill"] = np.where(f6, "CC_Adhoc1", df["Skill"])
    return df


def research_pull(df):
    f0 = df["Project_Type"] == "Osprey"
    f1 = df["PhoneNumber"] == "9999999999"
    df["Skill"] = np.where(f1, "Research_Pull ", df["Skill"])
    df["Skill"] = np.where(f1 & f0, "Osprey_research", df["Skill"])
    return df


def last_call(df, nbd):
    # convert CF last call date to child org / child ORG's won't be affected
    df["Last_Call"] = pd.to_datetime(df["Last_Call"], errors="coerce").dt.date
    filter1 = df["Last_Call"] >= nbd  # x_Bus_Day_ago(3)
    filter2 = df["Skill"] != "CC_Pend_Eligible"
    df["Skill"] = np.where(filter1 & filter2, "Child_ORG", df["Skill"])
    return df


def Re_Skill_Tier(df):
    f1 = df["OutreachID_Count"] == 1
    f2 = df["OutreachID_Count"] >= 1
    f3 = df["OutreachID_Count"] <= 4
    f4 = df["OutreachID_Count"] >= 5  #### ?

    df["Skill"] = np.where(f4, "CC_Tier1", df["Skill"])
    df["Skill"] = np.where(f2 & f3, "CC_Tier2", df["Skill"])
    return df


def fill(df):
    f1 = df["Skill"].isnull()
    f2 = df["Skill"] == "NaN"
    df["Skill"] = np.where(f1 | f2, "CC_Tier2", df["Skill"])
    return df


def escalations(df):
    f1 = df["Outreach_Status"] == "Escalated"
    f2 = df["Outreach_Status"] == "PNP Released"
    df["Skill"] = np.where(f1 | f2, "CC_Escalation", df["Skill"])
    return df


def wellmed(df):
    f1 = df["Project_Type"] == "WellMed"
    df["Skill"] = np.where(f1, "CC_Wellmed_Sub15_UNS", df["Skill"])
    return df


def MasterSiteId(df):
    f1 = df["MasterSiteId"].notna()
    f2 = df["MasterSiteId"] != 1000838
    df["Skill"] = np.where(f1 & f2, "CC_Cross_Reference", df["Skill"])
    return df


def quicklist(df):
    nocall_list = pd.read_csv("data/table_drop/nocalllist.csv")
    f1 = nocall_list.OutreachID.tolist()
    f2 = df.OutreachID.isin(f1)
    df["Skill"] = np.where(f2, "CC_Tier2", df["Skill"])
    df["quicklist"] = np.where(f2, 1, 0)
    return df


def chartfinder(df):
    df["Skill"] = "CC_ChartFinder"
    return df


def Cross_Reference_SPI(df, ls):
    f1 = df["SPI"] == True
    f2 = df["Project_Type"].isin(ls)
    df["Skill"] = np.where(f1 & ~f2, "CC_Cross_Reference_SPI", df["Skill"])
    return df


def mv_projects(df, ls):
    f1 = df["Project_Type"].isin(ls)
    df["Skill"] = np.where(f1, "CC_ChartFinder", df["Skill"])
    return df


def optum_assigned(df):
    f1 = df["optum_assigned"] == 1
    df["Skill"] = np.where(f1, "optum_assigned", df["Skill"])
    return df


def complex_skills(df):
    ls = [
        "UHC HEDIS",
        "HEDIS",
        "ACA-PhysicianCR",
        "RADV",
        "Chart Review",
    ]  # 'Chart Review'
    # 'Oscar',
    commerical = [
        "Centene ACA",
        "Centene HEDIS",
        "WellCare HEDIS",
        "Highmark ACA",
        "Advantasure ACA",
        "Anthem ACA",
        "HealthSpring HEDIS",
        "OptimaHealth HEDIS",
        "Med Mutual of Ohio HEDIS",
        "Inovalon",
        "BCBS TN ACA",
        "Cigna HEDIS",
        "Anthem Hedis",
        "Anthem Comm HEDIS",
        "Devoted Health HEDIS",
        "Aetna HEDIS",
        "Advantasure_HEDIS_WA",
        "IBX Hedis",
        "Molina HEDIS Region 6-FL-SC",
        "Aetna MEDICAID HEDIS",
        "Inovalon Hedis",
        "Advantmed HEDIS",
        "Excellus CRA",
        "Oscar HF ACA",
        "Priority Health ACA",
        "Molina HEDIS Region 5-IL-MI-WI",
        "Advantasure_HEDIS_NE",
        "Aetna Commercial",
        "Gateway HEDIS",
        "Molina HEDIS Region 4-NY-OH",
        "Advantasure_HEDIS_VT",
        "Arizona BlueCross BlueShield",
        "Med Mutual of Ohio ACA",
        "Highmark NY ACA",
        "Molina HEDIS Region 3-MS-NM-TX",
        "Advantasure_HEDIS_ND",
        "Advantasure_HEDIS_OOA_Anthem",
        "Optima Health Commercial",
        "Highmark HEDIS",
        "Centauri",
        "BCBSTN HEDIS",
        "Molina HEDIS Supplemental Region 5- IL-MI-WI",
        "Molina HEDIS Region 2-ID-UT-WA",
        "Premera",
        "Humana HEDIS",
        "Molina HEDIS Supplemental Region 4-NY-OH",
        "Molina HEDIS Supplemental Region 6-FL-SC",
        "ABCBS",
        "Molina HEDIS Region 1-CA",
        "Reveleer HEDIS",
        "Centene HEDIS-WI",
        "Molina HEDIS Supplemental Region 3-MS-NM-TX",
        "Change Healthcare",
        "Alliant Health Plans HEDIS",
        "BCBS TN HEDIS OOA",
        "Humana",
    ]
    commerical = [
        x for x in commerical if (x.__contains__("ACA")) or (x.__contains__("HEDIS"))
    ]
    ls += commerical
    f = df
    f = chartfinder(f)
    f = MasterSiteId(f)
    # f = adhoc1(f)

    f = CC_Genpact_Scheduling(f)
    f = mv_projects(f, ls)
    f = rm_schedule(f, ls)
    f = escalations(f)
    f = Osprey(f)
    f = research_pull(f)
    f = Cross_Reference_SPI(f, ls)
    f = emr_rm(f)
    f = HIH_rm(f)
    f = Onsite_rm(f)
    # f = optum_assigned(f)
    return f
