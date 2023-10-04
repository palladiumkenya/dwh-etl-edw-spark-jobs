select
    PatientPK,
    SiteCode,
    TestResult as LatestVL2,
    OrderedbyDate as LatestVLDate2,
    rank
from dbo.Intermediate_OrderedViralLoads
where rank = 2