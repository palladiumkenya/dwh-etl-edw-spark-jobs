select
    distinct HTSNumberHash,
             PatientPKHash,
             PatientPK,
             SiteCode,
             cast(DOB as date) as DOB,
             Gender,
             MaritalStatus,
             NupiHash
from dbo.HTS_clients as clients