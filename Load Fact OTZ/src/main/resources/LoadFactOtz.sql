select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    otz_enrollment.DateKey as OTZEnrollmentDateKey,
    last_visit.DateKey as LastVisitDateKey,
    otz_and_last_encounter_combined.TransferInStatus,
    otz_and_last_encounter_combined.ModulesPreviouslyCovered,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_Orientation,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_Participation,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_Leadership,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_MakingDecisions,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_Transition,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_TreatmentLiteracy,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_SRH,
    otz_and_last_encounter_combined.ModulesCompletedToday_OTZ_Beyond,
    current_date() as LoadDate
from otz_and_last_encounter_combined
    left join DimPatient as patient on patient.PatientPK = otz_and_last_encounter_combined.PatientPK
    and patient.SiteCode = otz_and_last_encounter_combined.SiteCode
    left join DimFacility as facility on facility.MFLCode = otz_and_last_encounter_combined.SiteCode
    left join DimDate as otz_enrollment on otz_enrollment.Date = otz_and_last_encounter_combined.OTZEnrollmentDate
    left join DimDate as last_visit on last_visit.Date = otz_and_last_encounter_combined.OTZEnrollmentDate
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = otz_and_last_encounter_combined.SiteCode
    left join DimPartner as partner on partner.PartnerName = upper(MFL_partner_agency_combination.SDP)
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = otz_and_last_encounter_combined.AgeLastVisit