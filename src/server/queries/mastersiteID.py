
def sql():
    sql = '''
    SELECT DISTINCT
         msi.[MSId] mastersiteID
        ,msi.outreachid OutreachID
    FROM MasterSiteID.dbo.MSIOutreach MSI
    '''
    return sql
