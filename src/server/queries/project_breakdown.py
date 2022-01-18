def sql():
    sql = '''
    With cc as (
  SELECT
     c.OutreachID
    ,c.PhoneNumber
  FROM
    DWWorking.dbo.Call_Campaign c
  WHERE
    c.Load_Date = DATEADD(day, -1, CAST(GETDATE() AS date))
)
, nic AS (
  SELECT DISTINCT
        OutreachID
        ,Disp_Name
        ,count(Disp_Name) disp_count
        FROM cc 
        LEFT JOIN [DWWorking].[Prod].[nicagentdata] nic on nic.Contact_Name = cc.PhoneNumber
        WHERE [Start_Date] =  DATEADD(day, -1, CAST(GETDATE() AS date))
        GROUP BY OutreachID
              ,Disp_Name
),
cc_flag AS (
SELECT DISTINCT 
    o.OutreachId
    -- ,o.PhoneNumber
    ,pt.ProjectTypeDescription	AS  'ProjectType'
    ,nic.Disp_Name
    ,nic.disp_count
    ,mn.NoteType
    ,case
        when mn.NoteType IN (
        2,4,7,19,20,21,22,24,25,26,29,30,47,50,63,71,72,74,78,80,96,105,107,108,120,124,130,131,132,140,143,144,160,161
        ) then 1
        else 0
    end as 'CallNoteFlag'
FROM
  cc o
  LEFT JOIN nic ON nic.OutreachID = o.OutreachID
  LEFT JOIN ChartFinder_Snap.dbo.MasterNotes mn ON mn.OutreachId = o.OutreachId
  LEFT JOIN DW_Operations.dbo.FactChart fc ON fc.OutreachId = o.OutreachId
  LEFT JOIN DW_Operations.dbo.DimProjectType pt ON pt.ProjectTypeId = fc.ProjectTypeId

WHERE
  pt.ProjectTypeDescription in (
    'AHN',
    'CDQI HCR',
    'NAMMCA',
    'OC-AZ',
    'OC-NV',
    'OCN-WA',
    'OC-UT',
    'Reliant',
    'Riverside',
    'WellMed'
  )
  AND CAST(mn.NoteDate AS DATE) = DATEADD(day, -1, CAST(GETDATE() AS date))
)
SELECT DISTINCT *
FROM cc_flag
'''