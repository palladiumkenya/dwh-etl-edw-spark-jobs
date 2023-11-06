SELECT
    distinct_patients.PatientPK,
    patient.PatientPKHash,
    distinct_patients.SiteCode,
    date_started_TB_treatment.StartTBTreatmentDate,
    patient_TB_Diagnosis.TBDiagnosisDate,
    latest_visit.OnIPT,
    CASE
        WHEN latest_visit.OnTBDrugs = 'Yes' THEN
            1 ELSE 0
        END AS hasTB,
    datediff( yy, patient.DOB, last_encounter.LastEncounterDate ) AS AgeLastVisit
FROM
    ( SELECT DISTINCT PatientPk, SiteCode FROM dbo.CT_IPT ) distinct_patients
LEFT JOIN (
    SELECT DISTINCT PatientPK, SiteCode, CAST ( TBRxStartDate AS DATE ) AS StartTBTreatmentDate
    FROM dbo.CT_IPT WHERE TBRxStartDate IS NOT NULL ) date_started_TB_treatment ON date_started_TB_treatment.PatientPK = distinct_patients.PatientPK AND date_started_TB_treatment.SiteCode = distinct_patients.SiteCode
LEFT JOIN (
    SELECT
        PatientPk,
        SiteCode,
        MIN ( ReportedbyDate ) AS TBDiagnosisDate
    FROM
        dbo.CT_PatientLabs
    WHERE
        ( TestName LIKE '%TB%' OR TestName LIKE '%Sput%' OR TestName LIKE '%TB%' OR TestName LIKE '%Tuber%' )
      AND TestName <> 'SputumGramStain'
      AND ( TestResult LIKE '%Positive%' OR TestResult LIKE '%HIGH%' OR TestResult LIKE '%+%' )
    GROUP BY
        PatientID,
        PatientPk,
        SiteCode
    ) patient_TB_Diagnosis ON patient_TB_Diagnosis.PatientPk = distinct_patients.PatientPK AND patient_TB_Diagnosis.SiteCode = distinct_patients.SiteCode
LEFT JOIN (
    SELECT
        *
    FROM
        (
            SELECT
                row_number ( ) OVER ( partition BY PatientID, SiteCode, PatientPK ORDER BY VisitDate DESC ) AS rank,
                PatientID,
                PatientPK,
                SiteCode,
                OnIPT,
                OnTBDrugs
            FROM
                dbo.CT_IPT
        ) ipt_visits_ordered
    WHERE
            rank = 1
    ) latest_visit ON latest_visit.PatientPK = distinct_patients.PatientPK AND latest_visit.SiteCode = distinct_patients.SiteCode
LEFT JOIN dbo.Intermediate_LastPatientEncounter AS last_encounter ON last_encounter.PatientPK = distinct_patients.PatientPK AND last_encounter.SiteCode = distinct_patients.SiteCode
LEFT JOIN dbo.CT_Patient AS patient ON patient.PatientPK = distinct_patients.PatientPK AND patient.SiteCode = distinct_patients.SiteCode