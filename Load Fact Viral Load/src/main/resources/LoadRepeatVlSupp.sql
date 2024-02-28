Select
    Patientpk,
    Sitecode
from  RepeatVL
WHERE
    (TRY_CAST(REPLACE(TestResult, ',', '') AS FLOAT) < 200.00)
   OR
    (TestResult IN ('Undetectable', 'NOT DETECTED', '0 copies/ml', 'LDL', 'Less than Low Detectable Level'))