select
    distinct PatientPkHash,
             PatientPk,
             PrepNumber,
             SiteCode,
             PrepEnrollmentDate,
             Sex,
             DateofBirth,
             ClientType,
             MaritalStatus
from ODS.dbo.PrEP_Patient