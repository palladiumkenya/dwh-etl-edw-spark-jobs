select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    referral.DateKey as ReferralDateKey,
    enrolled.DateKey as DateEnrolledKey,
    preferred.DateKey as DatePrefferedToBeEnrolledKey,
    FacilityReferredTo,
    HandedOverTo,
    HandedOverToCadre,
    ReportedCCCNumber,
    current_date() as LoadDate
from source_data
    left join DimPatient as patient on patient.PatientPKHash = source_data.PatientPKHash and patient.SiteCode = source_data.SiteCode
    left join DimFacility as facility on facility.MFLCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimDate as referral on referral.Date = source_data.ReferralDate
    left join DimDate as enrolled on enrolled.Date = source_data.DateEnrolled
    left join DimDate as preferred on preferred.Date = source_data.DatePrefferedToBeEnrolled
where row_num = 1 and patient.voided =0;