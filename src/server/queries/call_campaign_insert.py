def sql(campaign_history, load_date):
    ### Remove campaign_history's file ###
    remove = f"""
            DELETE
            FROM [DWWorking].[dbo].[Call_Campaign]
            WHERE Load_Date < '{campaign_history}'
            OR Load_Date = '{load_date}'
            """
    ### Remove yesterday's file ###
    # pull whats in the server
    lookup = """
            SELECT
                [Load_Date] 
                ,count([Load_Date]) AS Count
            FROM [DWWorking].[dbo].[Call_Campaign]
            GROUP BY [Load_Date]
            ORDER BY [Load_Date];
            """
    return remove, lookup
