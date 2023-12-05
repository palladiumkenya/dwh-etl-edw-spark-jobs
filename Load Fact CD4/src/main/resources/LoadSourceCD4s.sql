select
    distinct baselines.PatientIDHash,
             baselines.PatientPKHash,
             baselines.SiteCode,
             CD4atEnrollment,
             CD4atEnrollment_Date as CD4atEnrollmentDate,
             bCD4 as BaselineCD4,
             bCD4Date as BaselineCD4Date,
             LastCD4AfterARTStart_Date as LastCD4Date,
             Case When LatestCD4s.TestName='CD4 Count'Then LatestCD4s.TestResult Else Null End as LastCD4,
             Case When LatestCD4s.TestName='CD4 Percentage' Then LatestCD4s.TestResult Else Null End as LastCD4Percentage,
             round((months_between(last_encounter.LastEncounterDate,  ct_patient.DOB)/12),0) as AgeLastVisit
from baselines
left join ct_patient on ct_patient.PatientPK = baselines.PatientPK and ct_patient.SiteCode = baselines.SiteCode
left join last_encounter on last_encounter.PatientPK = baselines.PatientPK and last_encounter.SiteCode = baselines.SiteCode
left join LatestCD4s on LatestCD4s.PatientPK=baselines.PatientPK and LatestCD4s.Sitecode=baselines.SiteCode