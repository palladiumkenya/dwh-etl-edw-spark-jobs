SELECT
    patient.PatientKey,
    facility.FacilityKey,
    agency.AgencyKey,
    partner.PartnerKey,
    age_group.AgeGroupKey,
    patient.Gender,
    dispense_date.DateKey as DispenseDateKey,
    source_data.RefillFirstMonthDiffInDays,
    source_data.TestResultsMonth1,
    refill_month_1.DateKey as DateTestMonth1Key,
    dispense_month_1.DateKey as DateDispenseMonth1,
    source_data.RefillThirdMonthDiffInDays,
    source_data.TestResultsMonth3,
    refill_month_3.DateKey as DateTestMonth3Key,
    dispense_month_3.DateKey as DateDispenseMonth3,
    current_date() as LoadDate
from source_data
left join facility on facility.MFLCode = source_data.Sitecode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP 
left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join patient on patient.PatientPKHash = source_data.PatientPKHash
    and patient.SiteCode = source_data.SiteCode
left join age_group on age_group.Age =  round((months_between(source_data.DispenseDate, patient.DOB)/12),0)
left join DimDate as refill_month_1 on refill_month_1.Date = source_data.TestDateMonth1
left join DimDate as dispense_month_1 on dispense_month_1.Date = source_data.DispenseDateMonth1
left join DimDate as refill_month_3 on refill_month_3.Date = source_data.TestDateMonth3
left join DimDate as dispense_month_3 on dispense_month_3.Date = source_data.DispenseDateMonth3
left join DimDate as dispense_date on dispense_date.Date = source_data.DispenseDate
WHERE patient.voided =0;