select
    patient.PatientPK,
    patient.SiteCode,
    patient.PatientID,
    latest_weight_height.LatestHeight,
    latest_weight_height.LatestWeight,
    age_of_last_visit.AgeLastVisit,
    latest_adherence.Adherence,
    latest_differentiated_care.DifferentiatedCare,
    latest_mmd.onMMD,
    lastest_stability_assessment.StabilityAssessment,
    latest_pregnancy.Pregnant
from patient
         left join latest_weight_height on latest_weight_height.PatientPK = patient.PatientPK
    and latest_weight_height.SiteCode = patient.SiteCode
         left join age_of_last_visit on age_of_last_visit.PatientPK = patient.PatientPK
    and age_of_last_visit.SiteCode = patient.SiteCode
         left join latest_adherence on latest_adherence.PatientPK = patient.PatientPK
    and latest_adherence.SiteCode = patient.SiteCode
         left join latest_differentiated_care on latest_differentiated_care.PatientPK = patient.PatientPK
    and latest_differentiated_care.SiteCode = patient.SiteCode
         left join latest_mmd on latest_mmd.PatientPK = patient.PatientPK
    and latest_mmd.SiteCode = patient.SiteCode
         left join lastest_stability_assessment on lastest_stability_assessment.PatientPK = patient.PatientPK
    and lastest_stability_assessment.SiteCode = patient.SiteCode
         left join latest_pregnancy on latest_pregnancy.PatientPK = patient.PatientPK
    and latest_pregnancy.SiteCode = patient.SiteCode
         left join latest_fp_method on latest_fp_method.PatientPK = patient.PatientPK
    and latest_fp_method.SiteCode = patient.SiteCode