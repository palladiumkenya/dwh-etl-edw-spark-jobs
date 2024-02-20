Select
    vls.PatientPk,
    vls.SiteCode,
    vls.TestResult,
    SecondLatestVLDate
from SecondLatestVL
    inner join Intermediate_OrderedViralLoads vls  on SecondLatestVL.patientpk=vls.PatientPK and SecondLatestVL.Sitecode=Vls.SiteCode
where rank=1  AND DATEDIFF(MONTH, orderedbydate, SecondLatestVLDate) <= 6