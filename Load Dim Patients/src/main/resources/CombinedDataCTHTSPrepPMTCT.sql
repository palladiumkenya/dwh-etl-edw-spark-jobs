SELECT
    COALESCE(combined_data_ct_hts_prep.patientpkhash, pmtct_patient_source.patientpkhash) AS PatientPKHash,
    COALESCE(combined_data_ct_hts_prep.sitecode, pmtct_patient_source.sitecode) AS SiteCode,
    COALESCE(combined_data_ct_hts_prep.nupi, pmtct_patient_source.nupihash) AS Nupi,
    COALESCE(combined_data_ct_hts_prep.dob, pmtct_patient_source.dob) AS DOB,
    COALESCE(combined_data_ct_hts_prep.maritalstatus, pmtct_patient_source.maritalstatus) AS MaritalStatus,
    COALESCE(combined_data_ct_hts_prep.gender, pmtct_patient_source.gender) AS Gender,
    combined_data_ct_hts_prep.patientidhash,
    combined_data_ct_hts_prep.clienttype,
    combined_data_ct_hts_prep.patientsource,
    combined_data_ct_hts_prep.enrollmentwhokey,
    combined_data_ct_hts_prep.dateenrollmentwhokey,
    combined_data_ct_hts_prep.baselinewhokey,
    combined_data_ct_hts_prep.datebaselinewhokey,
    combined_data_ct_hts_prep.istxcurr,
    combined_data_ct_hts_prep.htsnumberhash,
    combined_data_ct_hts_prep.prepenrollmentdatekey,
    combined_data_ct_hts_prep.prepnumber,
    pmtct_patient_source.patientmnchidhash,
    pmtct_patient_source.firstenrollmentatmnchdatekey,
    current_date() AS LoadDate,
    COALESCE(int(combined_data_ct_hts_prep.voided),int(pmtct_patient_source.voided)) As Voided
FROM combined_data_ct_hts_prep
FULL JOIN pmtct_patient_source ON combined_data_ct_hts_prep.patientpkhash = pmtct_patient_source.patientpkhash AND combined_data_ct_hts_prep.sitecode = pmtct_patient_source.sitecode
