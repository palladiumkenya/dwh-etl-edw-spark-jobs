SELECT
    ROW_NUMBER() OVER (PARTITION BY PatientPK, Sitecode ORDER BY OrderedbyDate DESC) AS RowNum,
    PatientPKHash,
    PatientPk,
    SiteCode,
    OrderedbyDate,
    TestName,
    TestResult
FROM ODS.dbo.CT_PatientLabs
WHERE TestName like '%CD4%'
