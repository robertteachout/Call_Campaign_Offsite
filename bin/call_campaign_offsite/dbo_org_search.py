from dbo_query import Query

def cd_last_call():
    query = ('''
                SELECT
                c.[Skill]
                ,c.[OutreachID]
                ,c.[PhoneNumber]
                ,c.[Daily_Groups]
                ,n.[Start_Date] As NIC_Last_Call
                ,CAST (od.lastcalldate AS DATE) as 'CF_Last_Call'
                ,c.[Unique_Phone]
                ,c.[Load_Date]

                FROM [DWWorking].[dbo].[Call_Campaign] AS c
                LEFT JOIN [DW_Operations].[dbo].[DimOutreach] AS cf
                    ON c.OutreachID = cf.OutreachId
                LEFT JOIN (
                    SELECT
                    [Skill_Name],
                    Contact_Name,
                    MAX([Start_Date]) AS [Start_Date]
                    FROM [DWWorking].[Prod].[nicagentdata]
                    GROUP BY Skill_Name, Contact_Name
                    ) AS n
                        ON c.[PhoneNumber] = n.[Contact_Name]
                        AND c.[Skill] = n.[Skill_Name]
                LEFT JOIN Chartfinder_Snap.dbo.OutreachDates as od
                    ON c.OutreachID = od.OutreachID
                WHERE c.[Daily_Groups] = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
                ''')
    return Query('DWWorking', query, 'Last Call Check')