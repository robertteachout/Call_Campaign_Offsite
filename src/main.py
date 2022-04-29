import pandas as pd

import log.log as log
import pipeline.clean
import pipeline.score
import pipeline.skills
import server.connections
import server.insert
import server.queries.call_campaign_insert
import server.queries.MasterSiteId
import server.queries.fax_date
import server.queries.reschedule
from pipeline.utils import Business_Days, daily_piv, time_check, x_Bus_Day_ago
from pipeline.tables import (compressed_files, contact_counts,
                             extract_file_name, tables)
from server.insert import before_insert, clean_for_insert, sql_insert

Bus_day = Business_Days()


def main(test="n", msid="n", sample="n"):
    server_name = "EUS1PCFSNAPDB01"
    database = "DWWorking"
    table = "Call_Campaign"
    dwworking = server.connections.MSSQL(server_name, database)
    dw_engine = dwworking.create_engine()

    ### load & transform
    extract, filename = extract_file_name(test)
    load = compressed_files(filename, path=extract, sep="|")
    log.df_len("load", load)

    ### test last call date = today
    tested, test = pipeline.clean.check_load(load, Bus_day.today)
    log.df_len("tested", tested)
    time_check(Bus_day.now, f"File Load \t{test}")

    # ## Reschedules
    # reschedule_sql = server.queries.reschedule.sql()
    # reSchedule = pd.read_sql(reschedule_sql, dw_engine)
    # log.df_len('reSchedule', reSchedule)

    # df_full0 = tested.append(reSchedule, ignore_index = True)
    # log.df_len('df_full', df_full0)

    ### Master Site ID
    if msid == "y":
        mastersite_sql = server.queries.MasterSiteId.sql()
        mastersite = pd.read_sql(mastersite_sql, dw_engine)
        tables("push", mastersite, "mastersite.csv")

    mastersite = tables("pull", "na", "mastersite.csv")
    mapped = pd.merge(tested, mastersite, how="left", on="OutreachID")
    mapped.MasterSiteId = mapped.MasterSiteId.fillna(1000838)
    log.df_len("MasterSiteId", mapped)
    time_check(Bus_day.now, "msid map")

    # add fax date
    fax_sql = server.queries.fax_date.sql()
    fax = pd.read_sql(fax_sql, dw_engine)
    mapped = pd.merge(mapped, fax, how="left", on="OutreachID")
    mapped.LastFaxDate = pd.to_datetime(mapped.LastFaxDate, format="%Y%m%d").dt.date

    ### fix & add columns
    clean = pipeline.clean.clean(mapped, Bus_day.tomorrow_str)
    log.df_len("clean", clean)
    time_check(Bus_day.now, "clean")

    ### reskill inventory
    skilled = pipeline.skills.complex_skills(clean)
    log.df_len("skilled", skilled)
    time_check(Bus_day.now, "skill")

    ### score inventory per skill
    scored = pipeline.score.scored_inventory(skilled)
    log.df_len("scored", scored)
    time_check(Bus_day.now, "Split, Score, & Parent/Child Relationship")

    def Save():
        ### save file
        compressed_files(f"{Bus_day.tomorrow_str}.zip", table=scored)
        # compressed_files(f"{Bus_day.tomorrow_str}.csv.gz", table=scored)
        ### get column name & types ~ collect unique phone script
        tables("push", scored.dtypes.reset_index(), "columns.csv")
        ### reporting
        time_check(Bus_day.now, "Save files")
        ### insert into server ###
        load = clean_for_insert(scored)
        load_date = "".join(scored.Load_Date.unique())
        remove, lookup = server.queries.call_campaign_insert.sql(
            x_Bus_Day_ago(10), load_date
        )
        before_insert(dw_engine, remove, lookup)
        sql_insert(load, dw_engine, table)

        contact_counts(scored)
        time_check(Bus_day.now, "batch_insert")

    ### create campaign pivot
    daily_piv(scored)
    time_check(Bus_day.now, "Create Pivot Table")

    if sample == "y":
        compressed_files(f"{Bus_day.tomorrow_str}.zip", table=scored)

    if test == "Pass":
        Save()


if __name__ == "__main__":

    def question(q):
        return input(f"\n{q}(y/n): ")

    if question("questions") == "y":
        answer = question("test, msid, save (yyy): ")
        test, msid, sample = [a for a in answer]
        main(test, msid, sample)
    else:
        main()
