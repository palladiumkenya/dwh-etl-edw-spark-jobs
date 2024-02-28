SELECT
    PatientPk,
    SiteCode,
    TestResult,
    orderedbydate as SecondLatestVLDate
FROM Intermediate_OrderedViralLoads
WHERE rank = 2
  AND (
    TRY_CAST(REPLACE(TestResult, ',', '') AS FLOAT) >= 200.00
    )