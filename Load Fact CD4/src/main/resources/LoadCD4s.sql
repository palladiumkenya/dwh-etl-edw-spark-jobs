select
    distinct baselines.PatientIDHash,
    baselines.PatientPKHash,
    baselines.SiteCode,
    CD4atEnrollment,
    CD4atEnrollment_Date as CD4atEnrollmentDate,
    bCD4 as BaselineCD4,
    bCD4Date as BaselineCD4Date,
    lastCD4 AS LastCD4,
    LastCD4AfterARTStart_Date as LastCD4Date,
    datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
from ODS.dbo.CT_PatientBaselines as baselines
left join ODS.dbo.CT_Patient as patient on patient.PatientPK = baselines.PatientPK and patient.SiteCode = baselines.SiteCode
left join ODS.dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = baselines.PatientPK and last_encounter.SiteCode = baselines.SiteCode