def sql():
    ### Pull from db
    # SELECT DISTINCT
    #     msi.[MSId]
    #     ,c.OutreachID c
    # --  ,count(c.OutreachID) c
    # FROM [DWWorking].[dbo].[Call_Campaign] c
    # LEFT JOIN [MasterSiteId].[dbo].[MSIOutreach] msi
    #     ON c.OutreachID = msi.OutreachId
    # WHERE c.[Load_Date] = CAST(GETDATE() AS DATE)
    # AND  msi.MSId = 1000838
    # OR  msi.MSId IS NULL

    sql = """
    WITH offsite AS (
        SELECT 
            fc.OutreachId
        FROM [DW_Operations].[dbo].[FactChart]              fc
        LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
            ON fc.RetrievalMethodId = rm.RetrievalMethodId
        LEFT JOIN [DW_Operations].[dbo].[DimProjectStatus]      ps
            ON fc.ProjectStatusId = ps.ProjectStatusId
        WHERE   rm.RetrievalGroup = 'Offsite'
        AND     ps.ProjectStatusId IN (1, 3)
    )
    , offsite_msid AS (
        SELECT DISTINCT
            msi.[MSId] [MasterSiteId]
            ,msi.outreachid [OutreachID]
            ,spi.[SPI]
        FROM MasterSiteID.dbo.MSIOutreach   msi
        JOIN offsite                        fc
            ON msi.outreachid = fc.outreachid
        LEFT JOIN MasterSiteID.dbo.MSI      spi
            ON msi.MSId = spi.Id    
        WHERE msi.MSId <> 1000838 
    )
    , msic AS (
        SELECT DISTINCT
            MSId
            ,Phone MSI_Phone
        FROM [MasterSiteID].[dbo].[MSIContact] msic
        WHERE msic.[Primary] = 1
    )

    SELECT DISTINCT
        MasterSiteId
        ,OutreachID
        ,SPI
        ,MSI_Phone
    FROM offsite_msid                  msi
    LEFT JOIN msic
        ON msi.MasterSiteId = msic.MSId
    """
    return sql
