select
    distinct PatientPkHash,
             PatientPk,
             PrepNumber,
             SiteCode,
             PrepEnrollmentDate,
             Sex,
             DateofBirth,
             ClientType,
             MaritalStatus,
             voided
from ODS.dbo.PrEP_Patient