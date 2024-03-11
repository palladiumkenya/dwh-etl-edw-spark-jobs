SELECT
    DISTINCT patientpkhash,
    patientpk,
    sitecode,
    dob,
    gender,
    nupihash,
    patientmnchidhash,
    maritalstatus,
    Cast(Format(firstenrollmentatmnch, 'yyyyMMdd') AS INT) AS FirstEnrollmentAtMnchDateKey
    ,voided
FROM   ods.dbo.mnch_patient