select
    patient.PatientKey,
    facility.FacilityKey,
    agency.AgencyKey,
    partner.PartnerKey,
    age_group.AgeGroupKey,
    Discontinuation.DateKey as ExitDateKey,
    PrepDiscontinuation.ExitDate,
    PrepDiscontinuation.ExitReason,
    current_date() as LoadDate
from prep_patients
left join PrepDiscontinuation on PrepDiscontinuation.PatientPKHash =  prep_patients.PatientPKHash
    and PrepDiscontinuation.SiteCode = prep_patients.SiteCode
left join patient on patient.PatientPKHash = prep_patients.PatientPKHash
    and patient.SiteCode = prep_patients.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = prep_patients.SiteCode
left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join facility on facility.MFLCode = prep_patients.SiteCode
left join DimDate as Discontinuation on Discontinuation.Date = PrepDiscontinuation.ExitDate
left join age_group on age_group.Age = round((months_between(PrepDiscontinuation.ExitDate,  patient.DOB)/12),0)