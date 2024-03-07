select
    coalesce(combined_data_ct_hts.PatientPKHash, prep_patient_source.PatientPKHash) as PatientPKHash,
    coalesce(combined_data_ct_hts.SiteCode, prep_patient_source.SiteCode) as SiteCode,
    combined_data_ct_hts.NUPI as NUPI,
    coalesce(combined_data_ct_hts.DOB, prep_patient_source.DateofBirth) as DOB,
    coalesce(combined_data_ct_hts.MaritalStatus, prep_patient_source.MaritalStatus) as MaritalStatus,
    coalesce(combined_data_ct_hts.Gender, prep_patient_source.Sex) as Gender,
    combined_data_ct_hts.PatientIDHash,
    coalesce(combined_data_ct_hts.ClientType, prep_patient_source.ClientType) as ClientType,
    combined_data_ct_hts.PatientSource,
    combined_data_ct_hts.EnrollmentWHOKey,
    combined_data_ct_hts.DateEnrollmentWHOKey,
    combined_data_ct_hts.BaseLineWHOKey,
    combined_data_ct_hts.DateBaselineWHOKey,
    combined_data_ct_hts.IsTXCurr,
    combined_data_ct_hts.HTSNumberHash,
    prep_patient_source.PrepNumber,
    cast(date_format(prep_patient_source.PrepEnrollmentDate, 'yyyyMMdd') as int) as PrepEnrollmentDateKey,
    current_date() as LoadDate,
    coalesce(int(combined_data_ct_hts.voided),int(prep_patient_source.voided)) As Voided
from combined_data_ct_hts
full join prep_patient_source on combined_data_ct_hts.PatientPKHash = prep_patient_source.PatientPKHash and prep_patient_source.SiteCode = combined_data_ct_hts.SiteCode