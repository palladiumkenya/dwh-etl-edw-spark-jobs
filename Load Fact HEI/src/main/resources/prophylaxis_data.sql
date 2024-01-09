select
    heis.PatientPk,
    heis.SiteCode
from MNCH_HEIs as heis
    inner join latest_cwc_visit on latest_cwc_visit.PatientPK = heis.PatientPK
    and latest_cwc_visit.SiteCode = heis.Sitecode
where MedicationGiven in ('AZT', 'NVP', 'CTX')