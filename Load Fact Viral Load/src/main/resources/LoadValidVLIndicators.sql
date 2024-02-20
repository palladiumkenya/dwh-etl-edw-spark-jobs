select
    PatientPK,
    SiteCode,
    TestResult as ValidVLResult,
    case
        when TRY_CAST(TestResult AS INT) IS NOT NULL THEN
            case
                when cast(replace(TestResult, ',', '') as  float) < 200.00 then 1
                else 0
                end
        else
            case
                when TestResult in ('undetectable','NOT DETECTED','0 copies/ml','LDL','Less than Low Detectable Level') then 1
                else 0
                end
        end as ValidVLSup,
    OrderedbyDate as ValidVLDate,
    case
        when TRY_CAST(TestResult AS INT) IS NOT NULL THEN
            case
                when cast(replace(TestResult,',','') AS float) >= 1000.00 then '>1000'
                when cast(replace(TestResult,',','') as float) between 200.00 and 999.00  then '200-999'
                when cast(replace(TestResult,',','') as float) between 50.00 and 199.00 then 'Low Risk LLV'
                when cast(replace(TestResult,',','') as float) < 50 then '<50'
                end
        else
            case
                when TestResult  in ('undetectable','NOT DETECTED','0 copies/ml','LDL','Less than Low Detectable Level') then 'Undetectable'
                end
        end as ValidVLResultCategory1,
    case
        when TRY_CAST(TestResult AS INT) IS NOT NULL THEN
            case
                when cast(replace(TestResult,',','') AS float) >= 1000.00 then 'UNSUPPRESSED'
                when cast(replace(TestResult,',','') as float) between 200.00 and 999.00  then 'High Risk LLV '
                when cast(replace(TestResult,',','') as float) between 50.00 and 199.00 then 'Low Risk LLV'
                when cast(replace(TestResult,',','') as float) < 50 then 'LDL'
                end
        else
            case
                when TestResult IN ('Undetectable', 'NOT DETECTED', '0 copies/ml', 'LDL', 'Less than Low Detectable Level') then 'LDL'
                else null
                end
        end as ValidVLResultCategory2
from valid_vl