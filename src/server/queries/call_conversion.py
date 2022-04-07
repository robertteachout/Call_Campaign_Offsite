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
        daysince AS (
            SELECT
                OM.OutreachID
                ,(DATEDIFF(dd, OM.InsertDate, GETDATE()) + 1) - (DATEDIFF(wk, OM.InsertDate, GETDATE()) * 2) - (
                CASE
                    WHEN DATENAME(dw, OM.InsertDate) = 'Sunday' THEN 1
                    ELSE 0
                END) - (
                CASE
                    WHEN DATENAME(dw, GETDATE()) = 'Saturday' THEN 1
                    ELSE 0
                END) AS 'DaysSinceCreation' 
            FROM ChartFinder.dbo.OutreachMaster OM
        ),
        retrieval_group AS (
            SELECT 
                OM.OutreachID
                ,ISNULL(RM.Name, 'Off Site Pending') AS 'Retrieval Method'
                ,(CASE
                    WHEN Name IN ('Off Site Pending',
                        'Fax Outreach',
                        'Provider Portal',
                        'Mail',
                        'Fax',
                        'Email',
                        'Mail - CD',
                        'Mail - FD',
                        'sFTP',
                        'Targeted Fax') THEN 'Offsite'
                    WHEN Name IN ('HIH - IOD',
                        'Z-HIH-HP-Sent',
                        'CIOX - Onsite',
                        'CIOX - Remote',
                        'Digital Direct - Ciox',
                        'Self Retrieval',
                        'Digital Direct',
                        'UHC Onsite',
                        'CIOX',
                        'LI - Remote',
                        'CIOX - Partner') THEN 'ROI & Digital Direct'
                    WHEN Name IN ('Paper Scan',
                        'EMR - Print to Scan',
                        'EMR - Flash Drive',
                        'LI - Onsite',
                        'On Site Pending') THEN 'Onsite'
                    WHEN Name IN ('EMR - Remote',
                        'EMR - Remote Queued'
                        ) THEN 'EMR Remote'
                    WHEN Name = 'HIH - Other' THEN 'HIH'
                    ELSE 'HIH'
                END) AS [Retrieval Group]
            FROM ChartFinder.dbo.OutreachMaster OM WITH (NOLOCK)
            LEFT JOIN ChartFinder.dbo.List RM WITH (NOLOCK)
                    ON OM.RetrievalMethod = RM.Value
                    AND RM.ListType = 'RetrievalMethod'
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
        ,rg.[Retrieval Method]
        ,rg.[Retrieval Group]
        ,p.ProjectType
        ,c.[PhoneNumber]
        ,nic.Disp_Name
        ,cf.cf_last_call
        ,ds.DaysSinceCreation

        FROM [DWWorking].[dbo].[Call_Campaign] AS c
        LEFT JOIN project p
            ON p.OutreachId = c.OutreachID 
        LEFT JOIN nic
            ON nic.Skill_Name = c.Skill
            AND nic.PhoneNumber = c.PhoneNumber
        LEFT JOIN cf
            ON c.OutreachID = cf.OutreachID
        LEFT JOIN retrieval_group rg
            ON rg.OutreachID = c.OutreachID
        LEFT JOIN daysince ds
            ON ds.OutreachID = c.OutreachID 
        WHERE   c.[Load_Date] = '{lbd}'
                    """
