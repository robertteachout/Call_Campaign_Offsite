
def sql():
    ### Pull from db
    # SELECT DISTINCT
    #     msi.[MSId]
    #     ,c.OutreachID c
    # --  ,count(c.OutreachID) c
    # FROM [DWWorking].[dbo].[Call_Campaign] c
    # LEFT JOIN [MasterSiteID].[dbo].[MSIOutreach] msi
    #     ON c.OutreachID = msi.OutreachId
    # WHERE c.[Load_Date] = CAST(GETDATE() AS DATE)
    # AND  msi.MSId = 1000838
    # OR  msi.MSId IS NULL
    
    sql = '''
    SELECT DISTINCT
         msi.[MSId] mastersiteID
        ,msi.outreachid OutreachID
    FROM MasterSiteID.dbo.MSIOutreach MSI
    '''
    return sql
