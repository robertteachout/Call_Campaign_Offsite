def sql(CF, NIC, lbd):
    sql = f"""
            SELECT
            c.[Skill]
            ,c.[OutreachID]
            ,c.[PhoneNumber]
            ,c.[Daily_Groups]
            ,n.NIC_Last_Call
            ,CAST (od.lastcalldate AS DATE) as 'CF_Last_Call'
            ,c.[Unique_Phone]
            ,c.[Load_Date]

            FROM [DWWorking].[dbo].[Call_Campaign] AS c
            LEFT JOIN [DW_Operations].[dbo].[DimOutreach] AS cf
                ON c.OutreachID = cf.OutreachId
            LEFT JOIN (
                SELECT DISTINCT
                [Skill_Name],
                Contact_Name,
                CAST (MAX([Start_Date]) AS DATE) As NIC_Last_Call
                FROM [DWWorking].[Prod].[nicagentdata]
                GROUP BY 
                    [Skill_Name],
                    Contact_Name
                ) AS n
                    ON c.[PhoneNumber] = n.[Contact_Name]
                    AND c.[Skill] = n.[Skill_Name]
            LEFT JOIN Chartfinder_Snap.dbo.OutreachDates as od
                ON c.OutreachID = od.OutreachID
            WHERE   c.[Load_Date] = '{lbd}'
            AND     c.[Daily_Groups] = '{lbd}'
            AND     od.lastcalldate <= '{CF}'
            AND     n.[NIC_Last_Call] <= '{NIC}'
            -- AND     c.[Unique_Phone] = 1
            """
    return sql
