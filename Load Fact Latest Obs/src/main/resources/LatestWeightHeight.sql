select
    PatientID,
    PatientPK,
    SiteCode,
    Weight as LatestWeight,
    Height as LatestHeight
from dbo.Intermediate_LastestWeightHeight
