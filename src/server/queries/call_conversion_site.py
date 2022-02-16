def sql(lbd):
    return f"""
    DECLARE @date AS VARCHAR(10) = '{lbd}';
    WITH cc AS (
        SELECT *
        FROM [DWWorking].[dbo].[Call_Campaign]
        WHERE Unique_Phone = 1
        AND Load_Date = @date
        AND Skill in ( 'CC_Cross_Reference','CC_ChartFinder') 

    ),

    nic as (
        SELECT DISTINCT
            PhoneNumber
            ,Skill_Name
            ,Disp_Name 
        FROM (
                SELECT DISTINCT
                    Contact_Name PhoneNumber
                    ,Skill_Name
                    ,Disp_Name 
                    ,[Start_Date]
                    ,ROW_NUMBER() OVER(PARTITION BY Skill_Name, Contact_Name 
                                ORDER BY Skill_Name, Contact_Name, start_time) top_disp
                FROM [DWWorking].[Prod].[nicagentdata]
                WHERE CAST([Start_Date] AS  DATE) = @date
            ) as nic_raw
        WHERE top_disp = 1
        AND Skill_Name in ('CC_Chart_Finder', 'CC_Cross_Reference')
    ),

    project AS (
        SELECT DISTINCT
        OutreachID
        ,ProjectType
        FROM (
            SELECT DISTINCT
                cc.OutreachID
                ,pt.ProjectTypeDescription ProjectType
                ,ROW_NUMBER() OVER(PARTITION BY cc.OutreachID 
                    ORDER BY cc.OutreachID) project_top
            FROM cc
            LEFT JOIN [DW_Operations].[dbo].[FactChart] fc
                ON fc.OutreachID = cc.OutreachID
            LEFT JOIN [DW_Operations].[dbo].[DimProjectType] pt
                ON pt.ProjectTypeId = fc.ProjectTypeId 
            ) AS project_raw
        WHERE project_top = 1
    ),

    cf AS (
        SELECT DISTINCT
            cc.OutreachId
            ,CAST(cf.lastcalldate AS DATE) cf_last_call
        FROM cc
        LEFT JOIN Chartfinder_Snap.dbo.OutreachDates cf
            ON cc.OutreachID = cf.OutreachID
        WHERE CAST(cf.lastcalldate AS DATE) = @date
    )

    SELECT DISTINCT
        cc.[OutreachID]
        ,cc.[PhoneNumber]
        ,p.ProjectType
        ,cc.Unique_Phone
        ,cc.Skill
        -- ,cc.Score
        --,nic.Skill_Name
        ,nic.Disp_Name
        ,cf.cf_last_call
        -- ,cc.[Load_Date]

    FROM cc
    LEFT JOIN project p
        ON p.OutreachId = cc.OutreachID 
    LEFT JOIN nic
        ON nic.Skill_Name = cc.Skill
        AND nic.PhoneNumber = cc.PhoneNumber
    LEFT JOIN cf
        ON cc.OutreachID = cf.OutreachID

                    """