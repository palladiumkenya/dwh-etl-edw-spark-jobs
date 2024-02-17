Select
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    patient.PatientKey,
    as_of.DateKey as AsOfDateKey,
    LastEncounterDate,
    ExpectedNextAppointmentDate,
    AppointmentStatus,
    DiffExpectedTCADateLastEncounter,
    age_group.AgeGroupKey,
    AsofDate,
    cast(current_date() as date) as LoadDate

from apt
left join facility on facility.MFLCode = apt.MFLCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=apt.MFLCode
left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join patient on patient.PatientPKHash = apt.PatientPKhash and patient.SiteCode=apt.MFLCode
left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join age_group on age_group.AgeGroupKey = DATEDIFF(year,patient.DOB,apt.AsOfDate)
left join as_of on as_of.Date = apt.AsOfDate
WHERE patient.voided =0;