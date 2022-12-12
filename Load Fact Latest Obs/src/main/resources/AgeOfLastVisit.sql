select
    last_encounter.PatientPK,
    last_encounter.SiteCode,
    datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
from ODS.dbo.CT_Patient as patient
         left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = patient.PatientPK
    and last_encounter.SiteCode = patient.SiteCode