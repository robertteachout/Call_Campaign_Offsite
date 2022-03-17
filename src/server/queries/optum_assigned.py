
def sql():
    return """
    select distinct
    [outreach id],
    [outreach status],
    [outreach retrieval group],
    [Outreach Retrieval Method],
    COUNT(DISTINCT([uhc lob])) as 'LOB Count'

    into #a

    from (

    select distinct
    [outreach id],
    (case when [outreach status] = 'scheduled' and cast(getdate() as date) > cast([scheduled complete day] as date)
    then 'past due' else [outreach status] end) as [outreach status],
    (CASE WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Digital Direct - Ciox', 'Digital Direct') THEN 'Digital Direct'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('CIOX - Remote', 'CIOX', 'CIOX - Onsite', 'CIOX - Partner') THEN 'Embedded'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('EMR - Remote Queued', 'EMR - Remote') THEN 'EMR Remote'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Fax', 'Mail', 'Provider Portal', 'Email', 'Fax Outreach', '', 'Off Site Pending', 'sFTP', 'Mail - FD', 'Mail - CD') THEN 'Offsite'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Paper Scan', 'EMR - Flash Drive', 'On Site Pending', 'EMR - Print To Scan') THEN 'Onsite'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') = 'Self Retrieval' THEN 'Self Retrieval'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') LIKE 'HIH%' THEN 'HIH'
    ELSE '' END) as [outreach retrieval group],
    ISNULL([Outreach Retrieval Method],'Off Site Pending') as [Outreach Retrieval Method],
    (CASE WHEN LOB IN ('Medicare', 'Medicaid DSNP') THEN 'M&R' 
    WHEN LOB IN ('Medicaid', 'MMP') THEN 'C&S' 
    WHEN LOB IN ('Commercial','IFP Exchange') THEN 'E&I' END) AS [UHC LOB]

    from dwworking.prod.optum_extract
    where [project type] = 'uhc hedis'
    and [project year] = 2022
    and [project status] IN ('new','in process')
    and (case when [outreach status] = 'scheduled' and cast(getdate() as date) > cast([scheduled complete day] as date)
    then 'past due' else [outreach status] end) = 'past due'
    and (CASE WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Digital Direct - Ciox', 'Digital Direct') THEN 'Digital Direct'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('CIOX - Remote', 'CIOX', 'CIOX - Onsite', 'CIOX - Partner') THEN 'Embedded'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('EMR - Remote Queued', 'EMR - Remote') THEN 'EMR Remote'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Fax', 'Mail', 'Provider Portal', 'Email', 'Fax Outreach', '', 'Off Site Pending', 'sFTP', 'Mail - FD', 'Mail - CD') THEN 'Offsite'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') IN ('Paper Scan', 'EMR - Flash Drive', 'On Site Pending', 'EMR - Print To Scan') THEN 'Onsite'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') = 'Self Retrieval' THEN 'Self Retrieval'
    WHEN ISNULL([Outreach Retrieval Method],'Off Site Pending') LIKE 'HIH%' THEN 'HIH'
    ELSE '' END)  = 'Offsite'

    ) as a

    group by
    [outreach id],
    [outreach status],
    [outreach retrieval group],
    [Outreach Retrieval Method]

    having COUNT(DISTINCT([uhc lob])) = 1

    select distinct a.*, (CASE WHEN LOB IN ('Medicare', 'Medicaid DSNP') THEN 'M&R' 
    WHEN LOB IN ('Medicaid', 'MMP') THEN 'C&S' 
    WHEN LOB IN ('Commercial','IFP Exchange') THEN 'E&I' END) AS [UHC LOB]

    from #a a
    join dwworking.prod.optum_extract oe on a.[outreach id] = oe.[outreach id]
    """