SELECT
    PatientPkHash,
    sitecode,
    ROW_NUMBER()OVER (PARTITION by SiteCode,PatientPK  ORDER BY VisitDate Desc ) As NUM,
    PHQ_9_rating
FROM ODS.dbo.CT_DepressionScreening