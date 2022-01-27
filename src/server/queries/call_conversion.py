def sql(lbd):
    return f"""
        with 
        project AS (
            SELECT DISTINCT 
                PS.OutreachID
                ,PT.Name ProjectType
            FROM ChartFinder.dbo.ProjectSite PS 
            JOIN ChartFinder.dbo.Project P 
                ON PS.ProjectId = P.ProjectId
            JOIN ChartFinder.dbo.List PT 
                ON P.ProjectType = PT.Value
                    AND PT.ListType = 'ProjectType'
        ),
        nic_full as (
            SELECT DISTINCT
                Contact_Name PhoneNumber
                ,Skill_Name
                ,Disp_Name 
                ,ROW_NUMBER() OVER(PARTITION BY Skill_Name, Contact_Name 
                            ORDER BY Skill_Name, Contact_Name) top_disp
            FROM [DWWorking].[Prod].[nicagentdata]
            WHERE [Start_Date] = '{lbd}'
            ), 
        nic AS (
            SELECT * 
            FROM nic_full
            WHERE top_disp = 1
        ),

        cf AS(
            SELECT 
                OutreachId
                ,CAST(lastcalldate AS DATE) cf_last_call
            FROM Chartfinder_Snap.dbo.OutreachDates 
            -- WHERE CAST (lastcalldate AS DATE) = '{lbd}'
            )

        SELECT
        c.[OutreachID]
        ,p.ProjectType
        ,c.[PhoneNumber]
        ,nic.Disp_Name
        ,cf.cf_last_call

        FROM [DWWorking].[dbo].[Call_Campaign] AS c
        LEFT JOIN project p
            ON p.OutreachId = c.OutreachID 
        LEFT JOIN nic
            ON nic.Skill_Name = c.Skill
            AND nic.PhoneNumber = c.PhoneNumber
        LEFT JOIN cf
            ON c.OutreachID = cf.OutreachID
        WHERE   c.[Load_Date] = '{lbd}'
                    """