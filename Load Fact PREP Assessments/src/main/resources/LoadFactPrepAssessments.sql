select
    distinct
    patient.PatientKey,
    facility.FacilityKey,
    agency.AgencyKey,
    partner.PartnerKey,
    age_group.AgeGroupKey,
    patient.Gender,
    VisitID,
    SexPartnerHIVStatus,
    IsHIVPositivePartnerCurrentonART,
    IsPartnerHighrisk,
    PartnerARTRisk,
    ClientAssessments,
    ClientWillingToTakePrep,
    PrEPDeclineReason,
    RiskReductionEducationOffered,
    ReferralToOtherPrevServices,
    FirstEstablishPartnerStatus,
    PartnerEnrolledtoCCC,
    HIVPartnerCCCnumber,
    HIVPartnerARTStartDate,
    MonthsknownHIVSerodiscordant,
    SexWithoutCondom,
    NumberofchildrenWithPartner,
    ClientRisk,
    assessment_date.DateKey As AssessmentVisitDateKey,
    EligiblePrep,
    ScreenedPrep,
    current_date() as LoadDate
from source_data
    left join patient on patient.PatientPKHash = source_data.PatientPKHash and patient.SiteCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join facility on facility.MFLCode = source_data.SiteCode
    left join age_group on age_group.Age = round((months_between(coalesce(source_data.AssessmentVisitDate, current_date()),  patient.DOB)/12),0)
    left join DimDate as assessment_date on assessment_date.Date = source_data.AssessmentVisitDate
WHERE patient.voided =0;
