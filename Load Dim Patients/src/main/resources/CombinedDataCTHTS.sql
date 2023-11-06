select
    coalesce(ct_patient_source.PatientPKHash, hts_patient_source.PatientPKHash) as PatientPKHash,
    coalesce(ct_patient_source.SiteCode, hts_patient_source.SiteCode) as SiteCode,
    coalesce(ct_patient_source.NupiHash, hts_patient_source.NupiHash) as NUPI,
    coalesce(ct_patient_source.DOB, hts_patient_source.DOB) as DOB,
    coalesce(ct_patient_source.MaritalStatus, hts_patient_source.MaritalStatus) as MaritalStatus,
    coalesce(ct_patient_source.Gender, hts_patient_source.Gender) as Gender,
    ct_patient_source.PatientIDHash,
    ct_patient_source.PatientType as ClientType,
    ct_patient_source.PatientSource,
    ct_patient_source.EnrollmentWHOKey,
    ct_patient_source.DateEnrollmentWHOKey,
    ct_patient_source.BaseLineWHOKey,
    ct_patient_source.DateBaselineWHOKey,
    ct_patient_source.IsTXCurr,
    hts_patient_source.HTSNumberHash,
    current_date() as LoadDate
from ct_patient_source
         full join hts_patient_source on  hts_patient_source.PatientPKHash = ct_patient_source.PatientPKHash
    and ct_patient_source.SiteCode = hts_patient_source.SiteCode