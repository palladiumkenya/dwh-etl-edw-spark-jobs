SELECT
    Patientpk,
    Sitecode
from  RepeatVL
    where  try_Cast(Replace(TestResult,',','') AS FLOAT) >= 200.00