def sql():
    return """
    SELECT DISTINCT
        o.OutreachID
        ,[LastFaxDateId] LastFaxDate
    FROM [DW_Operations].[dbo].[DimOutreach] o

    JOIN DW_Operations.dbo.DimRetrievalMethod rm
        ON o.RetrievalMethod = o.RetrievalMethod
    JOIN DW_Operations.dbo.DimProjectSite PS
        ON o.OutreachId = PS.OutreachId

    LEFT JOIN DW_Operations.dbo.DimProject P 
            ON PS.ProjectId = P.ProjectId

    WHERE rm.RetrievalGroup = 'OffSite'
    AND [FirstFaxDateId] IS NOT NULL
    AND [FirstFaxDateId] <> -1
    AND P.ProjectStatusId = 3
    """