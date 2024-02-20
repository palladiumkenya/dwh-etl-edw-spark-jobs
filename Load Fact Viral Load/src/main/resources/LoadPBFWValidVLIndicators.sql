SELECT
    PatientPK,
    SiteCode,
    TestResult AS ValidVLResult,
    CASE
        WHEN isnumericTestResult = 1 THEN
            CASE
                WHEN CAST(REPLACE(TestResult, ',', '') AS FLOAT) < 200.00 THEN 1
                ELSE 0
                END
        ELSE
            CASE
                WHEN TestResult IN ('undetectable', 'NOT DETECTED', '0 copies/ml', 'LDL', 'Less than Low Detectable Level') THEN 1
                ELSE 0
                END
        END AS ValidVLSup,
    OrderedbyDate AS ValidVLDate,
    CASE
        WHEN isnumericTestResult = 1 THEN
            CASE
                WHEN CAST(REPLACE(TestResult, ',', '') AS FLOAT) >= 1000.00 THEN '>1000'
                WHEN CAST(REPLACE(TestResult, ',', '') AS FLOAT) BETWEEN 200.00 AND 999.00 THEN '200-999'
                WHEN CAST(REPLACE(TestResult, ',', '') AS FLOAT) BETWEEN 50.00 AND 199.00 THEN '51-199'
                WHEN CAST(REPLACE(TestResult, ',', '') AS FLOAT) < 50 THEN '<50'
                END
        ELSE
            CASE
                WHEN TestResult IN ('undetectable', 'NOT DETECTED', '0 copies/ml', 'LDL', 'Less than Low Detectable Level') THEN 'Undetectable'
                END
        END AS ValidVLResultCategory
FROM PBFW_valid_vl