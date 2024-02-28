select
    PatientPKHash,
    PatientPK,
    SiteCode,
    VisitDate,
    ChronicIllness,
    row_number() over (partition by PatientPK, Sitecode order by VisitDate asc) as rank
from ODS.dbo.CT_AllergiesChronicIllness as chronic
where chronic.voided = 0
  and ChronicIllness like '%Hypertension%' 