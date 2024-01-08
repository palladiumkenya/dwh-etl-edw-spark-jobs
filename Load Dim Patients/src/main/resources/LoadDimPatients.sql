select
    combined_data_ct_hts_prep_pmtct.patientidhash,
    combined_data_ct_hts_prep_pmtct.patientpkhash,
    combined_data_ct_hts_prep_pmtct.htsnumberhash,
    combined_data_ct_hts_prep_pmtct.prepnumber,
    combined_data_ct_hts_prep_pmtct.sitecode,
    combined_data_ct_hts_prep_pmtct.nupi,
    combined_data_ct_hts_prep_pmtct.dob,
    combined_data_ct_hts_prep_pmtct.maritalstatus,
    CASE
        WHEN combined_data_ct_hts_prep_pmtct.Gender = 'M' THEN 'Male'
        WHEN combined_data_ct_hts_prep_pmtct.Gender = 'F' THEN 'Female'
        ELSE combined_data_ct_hts_prep_pmtct.Gender
    END AS Gender,
    combined_data_ct_hts_prep_pmtct.clienttype,
    combined_data_ct_hts_prep_pmtct.patientsource,
    combined_data_ct_hts_prep_pmtct.enrollmentwhokey,
    combined_data_ct_hts_prep_pmtct.datebaselinewhokey,
    combined_data_ct_hts_prep_pmtct.baselinewhokey,
    combined_data_ct_hts_prep_pmtct.prepenrollmentdatekey,
    combined_data_ct_hts_prep_pmtct.istxcurr,
    combined_data_ct_hts_prep_pmtct.patientmnchidhash,
    combined_data_ct_hts_prep_pmtct.firstenrollmentatmnchdatekey,
    combined_data_ct_hts_prep_pmtct.loaddate,
    combined_data_ct_hts_prep_pmtct.voided
from combined_data_ct_hts_prep_pmtct
