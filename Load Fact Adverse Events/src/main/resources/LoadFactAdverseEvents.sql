select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    adverse_event_start.DateKey as AdverseEventStartDateKey,
    adverse_event_end.DateKey as AdverseEventEndDateKey,
    visit.DateKey as VisitDateKey,
    AdverseEvent,
    Severity,
    AdverseEventCause,
    AdverseEventRegimen,
    AdverseEventActionTaken,
    AdverseEventClinicalOutcome,
    AdverseEventIsPregnant,
    current_date() as LoadDate
from AdverseEvents source_data
     left join facility on facility.MFLCode  = source_data.SiteCode
     left join patient on patient.PatientPKHash = source_data.PatientPKHash and patient.SiteCode = source_data.SiteCode
     left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
     left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
     left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
     left join DimDate as adverse_event_start on adverse_event_start.Date = source_data.AdverseEventStartDate
     left join DimDate as adverse_event_end on adverse_event_end.Date = source_data.AdverseEventEndDate
     left join DimDate as visit on visit.Date = source_data.VisitDate
     left join age_group on age_group.Age = source_data.AgeLastVisit
WHERE patient.voided =0;
