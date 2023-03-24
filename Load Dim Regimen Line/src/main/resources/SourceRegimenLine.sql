SELECT
    DISTINCT StartRegimenLine AS RegimenLine
FROM ODS.dbo.CT_ARTPatients
UNION ALL
SELECT
    DISTINCT LTRIM(RTRIM(LastRegimenLine)) AS RegimenLine
FROM ODS.dbo.CT_ARTPatients