SELECT DISTINCT
    ushauri.UshauriPatientPkHash,
    ushauri.PatientIDHash,
    ushauri.patientpk,
    ushauri.sitecode,
    ushauri.patienttype,
    ushauri.patientsource,
    Try_convert(date,ushauri.DOB) AS DOB,
    ushauri.gender,
    ushauri.maritalstatus,
    ushauri.nupihash,
    ushauri.SiteType
FROM   ods.dbo.Ushauri_Patient AS ushauri
where ushauri.PatientPKHash is null