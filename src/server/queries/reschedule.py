def sql():
    sql = """
	SELECT DISTINCT
         m.[Outreach ID]										AS [OutreachID]
		,m.[Audit Type] 										
		,m.[Project Type] 										
		,m.[Project Due Date] 									
		,m.[Outreach Status] 	                                							
        ,ISNULL(CONVERT(VARCHAR(10), OD.LastCallDate, 1), '')   AS [Last Call]
        ,SC.PHONENUM                                            AS [PhoneNumer]

	FROM DWWorking.Prod.Master_Reporting_Table                  AS m
	LEFT JOIN DW_Operations.dbo.DimOutreach			    		AS	om
		ON m.[Outreach ID] = om.OutreachId
	LEFT JOIN ChartFinder.dbo.ProjectSite PS
        ON OM.PrimarySiteId = PS.SiteId
            AND PS.OutreachID = OM.OutreachID
    LEFT JOIN ChartFinder.dbo.SiteContact SC
        ON sc.id = PS.PrimaryContactId
	LEFT JOIN ChartFinder.dbo.OutreachDates OD 
        ON OM.OutreachID = OD.OutreachID
	WHERE   m.[Outreach status] = 'ReSchedule'
		AND m.[Project Type] <> 'On Demand'
		AND m.[Project Status] <> 'Completed'
		AND m.[Retrieval Group] = 'Offsite'
		AND m.[Project Due Date] > GETDATE()
        AND sc.PrimaryFlag = 1
	"""
    return sql
