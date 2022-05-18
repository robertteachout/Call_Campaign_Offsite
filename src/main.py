import pandas as pd

import log.log as log
import pipeline.clean
import pipeline.score
import pipeline.skills
import server.connections
import server.insert
import server.queries.fax_date
import server.queries.MasterSiteId
from pipeline.tables import (asm_fall_out, compressed_files, extract_file_name,
                             get_sql_data, save_locally, tables)
from pipeline.utils import Business_Days, daily_piv, time_check, x_Bus_Day_ago

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

    # merge new element 
    fax_sql = server.queries.fax_date.sql()
    fax = get_sql_data("fax_data", fax_sql, dw_engine)
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

    ### create campaign pivot
    daily_piv(scored)
    time_check(Bus_day.now, "Create Pivot Table")

    if sample == "y":
        save_locally(scored, log_contact='n')
        asm_fall_out(load, filename)

    if test == "Pass":
        asm_fall_out(load, filename)
        save_locally(scored)
        time_check(Bus_day.now, "Save files")
        server.insert.server_insert(scored, table, dw_engine, x_Bus_Day_ago(10))
        time_check(Bus_day.now, "batch_insert")

if __name__ == "__main__":

    def question(q):
        return input(f"\n{q}(y/n): ")

    if question("questions") == "y":
        answer = question("test, msid, save (yyy): ")
        test, msid, sample = [a for a in answer]
        main(test, msid, sample)
    else:
        main()
