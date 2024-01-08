select
    PatientPKHash,
    PatientPK,
    SiteCode,
    VisitDate,
    row_number() over (partition by PatientPK, Sitecode order by VisitDate desc) as rank
from ODS.dbo.CT_AllergiesChronicIllness as chronic
where chronic.voided = 0