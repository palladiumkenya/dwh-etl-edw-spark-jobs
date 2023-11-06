select
    otz.PatientIDHash,
    otz.PatientPKHash,
    otz.SiteCode,
    otz.OTZEnrollmentDate,
    otz.LastVisitDate,
    otz.TransferInStatus,
    otz.TransitionAttritionReason,
    otz.ModulesPreviouslyCovered,
    otz.ModulesCompletedToday_OTZ_Orientation,
    otz.ModulesCompletedToday_OTZ_Participation,
    otz.ModulesCompletedToday_OTZ_Leadership,
    otz.ModulesCompletedToday_OTZ_MakingDecisions,
    otz.ModulesCompletedToday_OTZ_Transition,
    otz.ModulesCompletedToday_OTZ_TreatmentLiteracy,
    otz.ModulesCompletedToday_OTZ_SRH,
    otz.ModulesCompletedToday_OTZ_Beyond,
    datediff(yy, patient.DOB, last_encounter.LastEncounterDate) as AgeLastVisit
from dbo.Intermediate_LastOTZVisit as otz
left join dbo.Intermediate_LastPatientEncounter as last_encounter on last_encounter.PatientPK = otz.PatientPK and last_encounter.SiteCode = otz.SiteCode
left join dbo.CT_Patient as patient on patient.SiteCode = otz.SiteCode and patient.PatientPK = otz.PatientPK