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


def rm_schedule(df):
    f1 = df["Outreach_Status"] == "Scheduled"
    f2 = df["Last_Call"].notna()
    df["Skill"] = np.where(f1 & f2, "schedule_pull", df["Skill"])
    return df


def adhoc1(df):
    f1 = df["super_age"] == 1
    df["Skill"] = np.where(f1, "CC_Adhoc1", df["Skill"])
    return df


def osprey_research(df):
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
    # f1 = df["MasterSiteId"].notna()
    # f2 = df["MasterSiteId"] != 1000838
    f1 = df["Outreach_Status"] != "Scheduled"
    f2 = df["SPI"] == True
    msid = df[f1 & ~f2].sort_values("age").reset_index().MasterSiteId.unique()[:8000]
    msid = [int(i) for i in list(msid)]
    msid.remove(1000838)
    f3 = df["MasterSiteId"].isin(msid)
    df["Skill"] = np.where(f3, "CC_Cross_Reference", df["Skill"])
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


def Cross_Reference_SPI(df):
    f1 = df["SPI"] == True
    df["Skill"] = np.where(f1, "CC_Cross_Reference_SPI", df["Skill"])
    return df


def mv_projects(df, ls):
    f1 = df["Project_Type"].isin(ls)
    df["Skill"] = np.where(f1, "CC_ChartFinder", df["Skill"])
    return df


def optum_assigned(df):
    f1 = df["optum_assigned"] == 1
    df["Skill"] = np.where(f1, "optum_assigned", df["Skill"])
    return df

def no_call(df):
    f1 = df["Last_Call"].isna()
    df["Skill"] = np.where(f1, "CC_ChartFinder", df["Skill"])
    return df

def complex_skills(df):

    f = df
    f = chartfinder(f)
    f = MasterSiteId(f)
    f = adhoc1(f)

    f = CC_Genpact_Scheduling(f)
    f = rm_schedule(f)
    f = escalations(f)
    # f = Cross_Reference_SPI(f)

    f = no_call(f)
    f = Osprey(f)
    f = osprey_research(f)
    f = emr_rm(f)
    f = HIH_rm(f)
    f = Onsite_rm(f)
    # f = optum_assigned(f)
    return f
