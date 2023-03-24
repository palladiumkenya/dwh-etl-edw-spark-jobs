select
    distinct HTSNumberHash,
             PatientPKHash,
             PatientPK,
             SiteCode,
             cast(DOB as date) as DOB,
             Gender,
             MaritalStatus,
             NUPI
from dbo.HTS_clients as clients