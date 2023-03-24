select
    combined_data_ct_hts_prep.PatientIDHash,
    combined_data_ct_hts_prep.PatientPKHash,
    combined_data_ct_hts_prep.HtsNumberHash,
    combined_data_ct_hts_prep.PrepNumber,
    combined_data_ct_hts_prep.SiteCode,
    combined_data_ct_hts_prep.NUPI,
    combined_data_ct_hts_prep.DOB,
    combined_data_ct_hts_prep.MaritalStatus,
    CASE
    WHEN combined_data_ct_hts_prep.Gender = 'M' THEN 'Male'
    WHEN combined_data_ct_hts_prep.Gender = 'F' THEN 'Female'
    ELSE combined_data_ct_hts_prep.Gender
END AS Gender,
        combined_data_ct_hts_prep.ClientType,
        combined_data_ct_hts_prep.PatientSource,
        combined_data_ct_hts_prep.EnrollmentWHOKey,
        combined_data_ct_hts_prep.DateBaselineWHOKey,
        combined_data_ct_hts_prep.BaseLineWHOKey,
        combined_data_ct_hts_prep.PrepEnrollmentDateKey,
        combined_data_ct_hts_prep.IsTXCurr,
        combined_data_ct_hts_prep.LoadDate
	from combined_data_ct_hts_prep
