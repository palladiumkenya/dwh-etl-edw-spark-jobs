select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    source_CD4.CD4atEnrollment,
    source_CD4.CD4atEnrollmentDate,
    source_CD4.BaselineCD4,
    source_CD4.BaselineCD4Date,
    source_CD4.LastCD4,
    source_CD4.LastCD4Date,
    source_CD4.LastCD4Percentage,
    current_date() as LoadDate
from source_CD4
left join patient on patient.PatientPKHash = source_CD4.PatientPKHash and patient.SiteCode = source_CD4.SiteCode
left join facility on facility.MFLCode = source_CD4.SiteCode
left join DimDate as cd4_enrollment on cd4_enrollment.Date = source_CD4.CD4atEnrollmentDate
left join DimDate as last_cd4 on last_cd4.Date = source_CD4.LastCD4Date
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_CD4.SiteCode
left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join age_group on age_group.Age = source_CD4.AgeLastVisit
WHERE patient.voided =0;