select
    PatientPK,
    SiteCode,
    TestResult as LatestVL1,
    OrderedbyDate as LatestVLDate1,
    rank
from dbo.Intermediate_OrderedViralLoads
where rank = 1