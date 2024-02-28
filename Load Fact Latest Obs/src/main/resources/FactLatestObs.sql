select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    diff_care.DifferentiatedCareKey,
    obs.LatestHeight,
    obs.LatestWeight,
    obs.AgeLastVisit,
    obs.Adherence,
    obs.DifferentiatedCare,
    obs.onMMD,
    obs.StabilityAssessment,
    obs.Pregnant,
    breastfeeding,
    TBScreening,
    current_date() as LoadDate
from obs
    left join DimPatient as patient on patient.PatientPKHash = obs.PatientPKHash and patient.SiteCode = obs.SiteCode
    left join DimFacility as facility on facility.MFLCode = obs.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = obs.SiteCode
    left join DimPartner as partner on partner.PartnerName = upper(MFL_partner_agency_combination.SDP)
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = obs.AgeLastVisit
    left join DimDifferentiatedCare as diff_care on diff_care.DifferentiatedCare = obs.DifferentiatedCare
WHERE patient.voided =0;