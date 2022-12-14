select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    diff_care.DifferentiatedCareKey,
    combined_table.LatestHeight,
    combined_table.LatestWeight,
    combined_table.AgeLastVisit,
    combined_table.Adherence,
    combined_table.DifferentiatedCare,
    combined_table.onMMD,
    combined_table.StabilityAssessment,
    combined_table.Pregnant,
    current_date() as LoadDate
from combined_table
    left join DimPatient as patient on patient.PatientPK = combined_table.PatientPK and patient.SiteCode = combined_table.SiteCode
    left join DimFacility as facility on facility.MFLCode = combined_table.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = combined_table.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = combined_table.AgeLastVisit
    left join DimDifferentiatedCare as diff_care on diff_care.DifferentiatedCare = combined_table.DifferentiatedCare