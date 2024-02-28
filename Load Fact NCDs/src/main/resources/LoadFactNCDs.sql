select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    ncd_source_data.`Alzheimer's Disease and other Dementias`,
    ncd_source_data.Arthritis,
    ncd_source_data.Asthma,
    ncd_source_data.Cancer,
    ncd_source_data.`Cardiovascular diseases`,
    ncd_source_data.`Chronic Hepatitis`,
    ncd_source_data.`Chronic Kidney Disease`,
    ncd_source_data.`Chronic Obstructive Pulmonary Disease(COPD)`,
    ncd_source_data.`Chronic Renal Failure`,
    ncd_source_data.`Cystic Fibrosis`,
    ncd_source_data.`Deafness and Hearing Impairment`,
    ncd_source_data.Diabetes,
    ncd_source_data.Endometriosis,
    ncd_source_data.Epilepsy,
    ncd_source_data.Glaucoma,
    ncd_source_data.`Heart Disease`,
    ncd_source_data.Hyperlipidaemia,
    ncd_source_data.Hypertension,
    ncd_source_data.Hypothyroidism,
    ncd_source_data.`Mental illness`,
    ncd_source_data.`Multiple Sclerosis`,
    ncd_source_data.Obesity,
    ncd_source_data.Osteoporosis,
    ncd_source_data.`Sickle Cell Anaemia`,
    ncd_source_data.`Thyroid disease`,
    with_underlying_ncd_condition_indicators.IsDiabeticAndScreenedDiabetes,
    with_underlying_ncd_condition_indicators.IsDiabeticAndDiabetesControlledAtLastTest,
    with_underlying_ncd_condition_indicators.IsHyperTensiveAndScreenedBPLastVisit,
    with_underlying_ncd_condition_indicators.IsHyperTensiveAndBPControlledAtLastVisit,
    first_hypertension.DateKey as FirstHypertensionRecoredeDateKey,
    first_diabetes.DateKey as FirstDiabetesRecordedDateKey
from ncd_source_data
left join with_underlying_ncd_condition_indicators on with_underlying_ncd_condition_indicators.PatientPKHash = ncd_source_data.PatientPKHash and with_underlying_ncd_condition_indicators.SiteCode = ncd_source_data.SiteCode
left join age_as_of_last_visit on age_as_of_last_visit.PatientPKHash = ncd_source_data.PatientPKHash and age_as_of_last_visit.SiteCode = ncd_source_data.SiteCode
left join DimFacility as facility on facility.MFLCode = ncd_source_data.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = ncd_source_data.SiteCode
left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join DimAgeGroup as age_group on age_group.Age = age_as_of_last_visit.AgeLastVisit
left join DimPatient as patient on patient.PatientPKHash = ncd_source_data.PatientPKHash and patient.SiteCode = ncd_source_data.SiteCode
left join earliest_hpertension_recorded on earliest_hpertension_recorded.PatientPKHash = ncd_source_data.PatientPKHash and earliest_hpertension_recorded.SiteCode = ncd_source_data.Sitecode
left join earliest_diabetes_recorded on earliest_diabetes_recorded.PatientPKHash = ncd_source_data.PatientPKHash and earliest_diabetes_recorded.SiteCode = ncd_source_data.SiteCode
left join DimDate as first_hypertension on first_hypertension.Date = cast(earliest_hpertension_recorded.VisitDate as date)
left join DimDate as first_diabetes on first_diabetes.Date = cast(earliest_diabetes_recorded.VisitDate as date);