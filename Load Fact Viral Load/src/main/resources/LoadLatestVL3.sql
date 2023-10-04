select
    PatientPK,
    SiteCode,
    TestResult as LatestVL3,
    OrderedbyDate as LatestVLDate3,
    rank
from dbo.Intermediate_OrderedViralLoads
where rank = 3