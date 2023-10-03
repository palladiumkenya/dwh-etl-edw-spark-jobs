select
    distinct adverse_events.Patientpk,
    adverse_events.PatientPKHash,
    adverse_events.SiteCode,
    AdverseEvent,
    AdverseEventStartDate,
    AdverseEventEndDate,
    Severity,
    VisitDate,
    adverse_events.EMR,
    AdverseEventCause,
    AdverseEventRegimen,
    AdverseEventActionTaken,
    AdverseEventClinicalOutcome,
    AdverseEventIsPregnant,
    datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
from ODS.dbo.CT_AdverseEvents as adverse_events
left join ODS.dbo.CT_Patient as patient on patient.PatientPK = adverse_events.PatientPK and patient.SiteCode = adverse_events.SiteCode
left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = adverse_events.PatientPK and last_encounter.SiteCode = adverse_events.SiteCode