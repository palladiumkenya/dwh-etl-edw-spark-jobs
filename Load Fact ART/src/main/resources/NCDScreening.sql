SELECT
    patient.PatientPKHash,
    patient.SiteCode,
    ScreenedDiabetes,
    ScreenedBPLastVisit
FROM Patient patient
left join latest_diabetes_test on latest_diabetes_test.PatientPKHash = patient.PatientPKHash and latest_diabetes_test.SiteCode = patient.SiteCode
left join visit on visit.PatientPK = patient.PatientPK and visit.SiteCode = patient.SiteCode