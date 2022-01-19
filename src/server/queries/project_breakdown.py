def sql(date):
    sql = f'''
    With cc as (
      SELECT
        c.OutreachID
        ,c.PhoneNumber
        ,case
          when c.Daily_Groups = '{date}' then 1 else 0
          end as daily_target_flag
      FROM
        DWWorking.dbo.Call_Campaign c
      WHERE
        c.Load_Date = '{date}'
    )
    , nic AS (
      SELECT DISTINCT
        OutreachID
        --,Disp_Name
        ,count(Disp_Name) disp_count
      FROM cc 
      LEFT JOIN [DWWorking].[Prod].[nicagentdata] nic on nic.Contact_Name = cc.PhoneNumber
      WHERE [Start_Date] = '{date}'
      -- AND Skill_Name like 'CC%' 
      -- OR Skill_Name like 'MNL_Out%'
      GROUP BY OutreachID --,Disp_Name
    )
    , cf_flag AS (
      SELECT DISTINCT 
          cc.OutreachID
          ,mn.NoteType
          ,case
              when mn.NoteType IN (
              2,4,7,19,20,21,22,24,25,26,29,30,47,50,63,71,72,74,78,80,96,105,107,108,120,124,130,131,132,140,143,144,160,161
              ) then 1
              else 0
          end as 'CallNoteFlag'
      FROM cc 
      LEFT JOIN (
        SELECT 
            OutreachID
            ,NoteDate
            ,NoteType
        FROM ChartFinder_Snap.dbo.MasterNotes 
        WHERE CAST(NoteDate AS DATE) = '{date}'
        ) as mn
            ON mn.OutreachID = cc.OutreachID
    )

    SELECT DISTINCT 
      cc.OutreachID
      ,cc.daily_target_flag
      ,pt.ProjectTypeDescription	AS  'ProjectType'
      ,nic.disp_count
      ,cf_flag.CallNoteFlag
    FROM cc
    LEFT JOIN DW_Operations.dbo.FactChart fc ON fc.OutreachID = cc.OutreachID
    LEFT JOIN DW_Operations.dbo.DimProjectType pt ON pt.ProjectTypeId = fc.ProjectTypeId
    LEFT JOIN nic ON nic.OutreachID = cc.OutreachID
    LEFT JOIN cf_flag ON cf_flag.OutreachID = cc.OutreachID
    
    -- WHERE
      -- pt.ProjectTypeDescription in (
      --   'AHN',
      --   'CDQI HCR',
      --   'NAMMCA',
      --   'OC-AZ',
      --   'OC-NV',
      --   'OCN-WA',
      --   'OC-UT',
      --   'Reliant',
      --   'Riverside',
      --   'WellMed'
      -- )
      -- AND 
      
    '''
    return sql