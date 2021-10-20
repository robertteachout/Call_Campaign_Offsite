sql = """
SELECT DISTINCT
	 m.[Outreach ID]										AS [OutreachID]
	,m.[Audit Type]
	,m.[Project Due Date]
	,m.[Outreach status]
	, lcd.Date										        As  [Last Call Date]  
	, s.Phone1                                              AS  [PhoneNumber]
	, csid.SiteCleanId                                      AS  [Site Clean ID]
	, CONCAT(S.Address1, ' ', S.Address2)                   AS  [Address]
	, S.City												AS	[City]
	, S.State												AS  [State]
	, S.Zip	 
FROM DWWorking.Prod.Master_Reporting_Table                  AS m
LEFT JOIN DW_Operations.dbo.DimOutreach			    		AS	om
    ON m.[Outreach ID] = om.OutreachId
JOIN DW_Operations.dbo.DimDate						        AS lcd
    ON om.LastCallDateId = lcd.DateKey
JOIN [ChartFinder_Snap].[dbo].[Site]                        AS	csid
    ON om.PrimarySiteId = csid.ID
JOIN DW_Operations.dbo.DimSite					            AS	s
    ON om.PrimarySiteId=s.SiteId
    
WHERE   m.[Outreach status] = 'ReSchedule'
    AND m.[Project Status] <> 'Completed'
    AND m.[Retrieval Group] = 'Offsite'
    AND m.[Project Due Date] > GETDATE()
"""