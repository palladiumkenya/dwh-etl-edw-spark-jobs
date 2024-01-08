SELECT
    DISTINCT patientpkhash,
    patientpk,
    sitecode,
    dob,
    gender,
    nupihash,
    patientmnchidhash,
    maritalstatus,
    Cast(date_format(firstenrollmentatmnch, 'yyyyMMdd') AS INT) AS FirstEnrollmentAtMnchDateKey
    ,voided
FROM   ods.dbo.mnch_patient