select
    PatientPk,
    SiteCode,
    VisitDate,
    InfantFeeding,
    MedicationGiven,
    row_number() over (partition by SiteCode,PatientPK order by VisitDate desc) as num
from dbo.MNCH_CwcVisits as visits