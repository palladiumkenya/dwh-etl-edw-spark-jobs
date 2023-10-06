select
    patient.PatientPK,
    patient.PatientPKHash,
    patient.SiteCode,
    eligible_for_VL.EligibleVL,
    valid_VL_indicators.ValidVLResult,
    case when valid_VL_indicators.ValidVLResult is not null then 1 else 0 end as HasValidVL,
    case when PBF.TestResult is not null then 1 else 0 end as PBFValidVL,
    valid_VL_indicators.ValidVLResultCategory1,
    valid_VL_indicators.ValidVLResultCategory2,
    case when valid_VL_indicators.ValidVLSup is not null then valid_VL_indicators.ValidVLSup else 0 end as ValidVLSup,
    valid_VL_indicators.ValidVLDate,
    patient_viral_load_intervals._6MonthVLDate,
    patient_viral_load_intervals._6MonthVL,
    patient_viral_load_intervals._12MonthVLDate,
    patient_viral_load_intervals._12MonthVL,
    patient_viral_load_intervals._18MonthVLDate,
    patient_viral_load_intervals._18MonthVL,
    patient_viral_load_intervals._24MonthVLDate,
    patient_viral_load_intervals._24MonthVL,
    patient_viral_load_intervals._6MonthVLSup,
    patient_viral_load_intervals._12MonthVLSup,
    patient_viral_load_intervals._18MonthVLSup,
    patient_viral_load_intervals._24MonthVLSup,
    first_vl.FirstVL,
    first_vl.FirstVLDate,
    last_vl.LastVL,
    last_vl.LastVLDate,
    time_to_first_vl.TimetoFirstVL,
    time_to_first_vl_group.TimeToFirstVLGrp,
    latest_VL_1.LatestVLDate1,
    latest_VL_1.LatestVL1,
    latest_VL_2.LatestVLDate2,
    latest_VL_2.LatestVL2,
    latest_VL_3.LatestVLDate3,
    latest_VL_3.LatestVL3,
    Case WHEN TRY_CAST(valid_VL_indicators.ValidVLResult AS INT) IS NOT NULL
             then CASE WHEN cast(Replace(valid_VL_indicators.ValidVLResult,',','')AS FLOAT) >= 200.00 then 1 ELSE 0 END
        END as HighViremia,
    Case WHEN TRY_CAST(valid_VL_indicators.ValidVLResult AS INT) IS NOT NULL
             then CASE WHEN cast(Replace(valid_VL_indicators.ValidVLResult,',','')AS FLOAT) < 200.00 then 1 ELSE 0 END
        END as LowViremia,
    DATEDIFF(last_encounter.LastEncounterDate, patient.DOB) / 365 AS AgeLastVisit
from CT_Patient as patient
inner join CT_ARTPatients art on art.PatientPK = patient.Patientpk
    and art.SiteCode = patient.SiteCode
left join eligible_for_VL on eligible_for_VL.PatientPK = patient.PatientPK
    and eligible_for_VL.SiteCode = patient.SiteCode
left join valid_VL_indicators on valid_VL_indicators.PatientPK = patient.PatientPK
    and valid_VL_indicators.SiteCode = patient.SiteCode
left join patient_viral_load_intervals on patient_viral_load_intervals.PatientPK = patient.PatientPK
    and patient_viral_load_intervals.SiteCode = patient.SiteCode
left join first_vl on first_vl.PatientPK = patient.PatientPK
    and first_vl.SiteCode = patient.SiteCode
left join last_vl on last_vl.PatientPK = patient.PatientPK
    and last_vl.SiteCode = patient.SiteCode
left join time_to_first_vl_group on time_to_first_vl_group.PatientPK = patient.PatientPK
    and time_to_first_vl_group.SiteCode = patient.SiteCode
left join time_to_first_vl on time_to_first_vl.PatientPK = patient.PatientPK
    and time_to_first_vl.SiteCode = patient.SiteCode
left join latest_VL_1 on latest_VL_1.PatientPK = patient.PatientPK
    and latest_VL_1.SiteCode = patient.SiteCode
left join latest_VL_2 on latest_VL_2.PatientPK = patient.PatientPK
    and latest_VL_2.SiteCode = patient.SiteCode
left join latest_VL_3 on latest_VL_3.PatientPK = patient.PatientPK
    and latest_VL_3.SiteCode = patient.SiteCode
left join Intermediate_LastPatientEncounter as last_encounter on patient.PatientPK = last_encounter.PatientPK
    and last_encounter.SiteCode = patient.SiteCode
left join PBF on PBF.PatientPK=patient.PatientPK and PBF.SiteCode=patient.SiteCode