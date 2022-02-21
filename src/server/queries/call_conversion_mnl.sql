DECLARE @date AS VARCHAR(10) = '2022-02-15';

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
            WHERE CAST([Start_Date] AS  DATE) = '2022-02-15'  --@date
        ) as nic_raw
    WHERE top_disp = 1
    AND Skill_Name in ( 'MNL_Out_Offsite'
                        ,'MNL_Out_Escalations'
                        ,'MNL_Out_Optum Broom Wagons'
                        ,'MNL_Out_OnDemand'
                        ,'MNL_Out_OnsitePilot'
                        ,'MNL_Out_Genpact_S15 to PRV')
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
)

SELECT DISTINCT
     cc.[OutreachID]
    ,cc.[PhoneNumber]
    ,p.ProjectType
    ,nic.Skill_Name
    ,nic.Disp_Name

FROM cc
LEFT JOIN project p
    ON p.OutreachId = cc.OutreachID 
LEFT JOIN nic
    ON nic.PhoneNumber = cc.PhoneNumber