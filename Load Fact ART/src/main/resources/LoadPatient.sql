Select
    Patient.PatientIDHash,
    Patient.PatientPKHash,
    cast (Patient.SiteCode as nvarchar) As SiteCode,
    DATEDIFF(
            yy, Patient.DOB, Patient.RegistrationAtCCC
        ) AgeAtEnrol,
    DATEDIFF(
            yy, Patient.DOB, ART.StartARTDate
        ) AgeAtARTStart,
    ART.StartARTAtThisfacility,
    ART.PreviousARTStartDate,
    ART.PreviousARTRegimen,
    ART.StartARTDate,
    LastARTDate,
    CASE WHEN [DateConfirmedHIVPositive] IS NOT NULL
        AND ART.StartARTDate IS NOT NULL THEN CASE WHEN DateConfirmedHIVPositive <= ART.StartARTDate THEN DATEDIFF(
            DAY, DateConfirmedHIVPositive, ART.StartARTDate
        ) ELSE NULL END ELSE NULL END AS TimetoARTDiagnosis,
    CASE WHEN Patient.RegistrationAtCCC IS NOT NULL
        AND ART.StartARTDate IS NOT NULL THEN CASE WHEN Patient.RegistrationAtCCC <= ART.StartARTDate THEN DATEDIFF(
            DAY, Patient.[RegistrationAtCCC],
            ART.StartARTDate
        ) ELSE NULL END ELSE NULL END AS TimetoARTEnrollment,
    Pre.PregnantARTStart,
    Pre.PregnantAtEnrol,
    las.LastEncounterDate As LastVisitDate,
    las.NextAppointmentDate,
    datediff(
            yy, patient.DOB, las.LastEncounterDate
        ) as AgeLastVisit,
    lastRegimen,
    StartRegimen,
    lastRegimenline,
    StartRegimenline,
    obs.WHOStage,
    outcome.ARTOutcome
FROM ODS.dbo.CT_Patient Patient
inner join ODS.dbo.CT_ARTPatients ART on ART.PatientPK = Patient.Patientpk and ART.SiteCode = Patient.SiteCode
left join ODS.dbo.Intermediate_PregnancyAsATInitiation Pre on Pre.Patientpk = Patient.PatientPK and Pre.SiteCode = Patient.SiteCode
left join ODS.dbo.Intermediate_LastPatientEncounter las on las.PatientPK = Patient.PatientPK and las.SiteCode = Patient.SiteCode
left join ODS.dbo.Intermediate_ARTOutcomes outcome on outcome.PatientPK = Patient.PatientPK and outcome.SiteCode = Patient.SiteCode
left join ODS.dbo.intermediate_LatestObs obs on obs.PatientPK=Patient.PatientPK and obs.SiteCode=Patient.SiteCode
