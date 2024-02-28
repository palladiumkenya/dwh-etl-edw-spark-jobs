select
    ncd_source_data.PatientPKHash,
    ncd_source_data.SiteCode,
    coalesce(ScreenedDiabetes, 0) as IsDiabeticAndScreenedDiabetes,
    coalesce(IsDiabetesControlledAtLastTest, 0) as IsDiabeticAndDiabetesControlledAtLastTest,
    coalesce(visit.ScreenedBPLastVisit,0) as IsHyperTensiveAndScreenedBPLastVisit,
    coalesce(visit.IsBPControlledAtLastVisit, 0) as IsHyperTensiveAndBPControlledAtLastVisit
from ncd_source_data
left join Intermediate_LatestDiabetesTests as latest_diabetes_test on latest_diabetes_test.PatientPKHash = ncd_source_data.PatientPKHash
    and latest_diabetes_test.SiteCode = ncd_source_data.SiteCode
    and ncd_source_data.Diabetes = 1
left join Intermediate_LastVisitDate as visit on visit.PatientPK = ncd_source_data.PatientPK
    and visit.SiteCode = ncd_source_data.SiteCode
    and ncd_source_data.Hypertension = 1