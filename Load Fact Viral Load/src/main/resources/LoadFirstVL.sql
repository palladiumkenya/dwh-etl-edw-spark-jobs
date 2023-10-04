select
    PatientPK,
    SiteCode,
    replace(TestResult, ',', '') as FirstVL,
    OrderedbyDate as FirstVLDate
from dbo.Intermediate_BaseLineViralLoads