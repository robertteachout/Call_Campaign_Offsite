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

bus_day = Business_Days()


def main(test="n", msid="n", save_test="n"):
    """ main executable to run daily call campaign
    Args:
        test: y/n -> extract_file_name(test) 
            get previous days file name
        msid: y/n -> server.queries.MasterSiteId.sql()
            updates MastersiteID & saved under data/table_drops 
        save_test: y/n ->  save_locally(), asm_fall_out()
            save local file and check file records to match whats in production
    """
    ### Server information & creating connection
    server_name = "EUS1PCFSNAPDB01"
    database = "DWWorking"
    table = "Call_Campaign"
    dwworking = server.connections.MSSQL(server_name, database)
    dw_engine = dwworking.create_engine()

    ### load & transformation
    # get correct filename based on main input variable `test`
    # options: today's or yesterdays
    extract, filename = extract_file_name(test)
    # read compressed zip file
    load = compressed_files(filename, path=extract, sep="|")
    log.df_len("load", load)

    ### check -> if `last call` column = today then `Pass` else `Fail`
    check_load, check = pipeline.clean.check_load(load, bus_day.today_str)
    log.df_len("tested", check_load)
    time_check(bus_day.now, f"File Load \t{check}")

    ### Master Site ID
    # main(msid='y') -> update msids
    if msid == "y":
        # run sql query for [OutreachID, MSID] (many to one) & save locally
        mastersite_sql = server.queries.MasterSiteId.sql()
        mastersite = pd.read_sql(mastersite_sql, dw_engine)
        tables("push", mastersite, "mastersite.csv")
    # pull locally saved MastersiteIDs from data/table_drop
    # join msid to file & fill NA with default value of 1000838
    mastersite = tables("pull", "na", "mastersite.csv")
    mapped = pd.merge(check_load, mastersite, how="left", on="OutreachID")
    mapped.MasterSiteId = mapped.MasterSiteId.fillna(1000838)
    log.df_len("MasterSiteId", mapped)
    time_check(bus_day.now, "msid map")

    # sql lookup on fax date & left join to file
    fax_sql = server.queries.fax_date.sql()
    fax = get_sql_data("fax_data", fax_sql, dw_engine)
    mapped = pd.merge(mapped, fax, how="left", on="OutreachID")
    mapped.LastFaxDate = pd.to_datetime(mapped.LastFaxDate, format="%Y%m%d").dt.date

    ### clean file -> format data types, clean bad numbers, fix age calculation
    clean = pipeline.clean.clean(mapped, bus_day.tomorrow_str)
    log.df_len("clean", clean)
    time_check(bus_day.now, "clean")

    ### create default skills
    skilled = pipeline.skills.complex_skills(clean)
    log.df_len("skilled", skilled)
    time_check(bus_day.now, "skill")

    ### score inventory per skill
    scored = pipeline.score.scored_inventory(skilled)
    log.df_len("scored", scored)
    time_check(bus_day.now, "Split, Score, & Parent/Child Relationship")

    ### create campaign pivot
    daily_piv(scored)
    time_check(bus_day.now, "Create Pivot Table")

    if save_test == "y":
        save_locally(scored, log_contact='n')
        asm_fall_out(load, filename)

    if check == "Pass":
        asm_fall_out(load, filename)
        save_locally(scored)
        time_check(bus_day.now, "Save files")
        server.insert.server_insert(scored, table, dw_engine, x_Bus_Day_ago(10))
        time_check(bus_day.now, "batch_insert")

if __name__ == "__main__":

    def question(q):
        return input(f"\n{q}(y/n): ")

    if question("questions") == "y":
        answer = question("test, msid, save (yyy): ")
        test, msid, save_test = [a for a in answer]
        main(test, msid, save_test)
    else:
        main()
