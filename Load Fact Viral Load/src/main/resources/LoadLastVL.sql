select
    PatientPK,
    SiteCode,
    replace(TestResult, ',', '') as LastVL,
    OrderedbyDate as LastVLDate
from dbo.Intermediate_LatestViralLoads