SELECT
    SiteCode,
    PatientPKHash,
    VisitDate,
    AgeAtVisit,
    DifferentiatedCare,
    VisitID,
    TracingType,
    TracingOutcome,
    Comments,
    is_reached
FROM (
         SELECT
             defaulter_trace.PatientPKHash,
             defaulter_trace.SiteCode,
             VisitID,
             VisitDate,
             TracingType,
             TracingOutcome,
             Comments,
             CASE
                 WHEN TracingOutcome LIKE '%No contact%' THEN
                     0 ELSE 1
                 END AS is_reached,
             latest_differentiated_care.DifferentiatedCare COLLATE Latin1_General_CI_AS AS DifferentiatedCare,
             datediff( yy, patient.DOB, defaulter_trace.VisitDate ) AS AgeAtVisit
         FROM dbo.CT_DefaulterTracing AS defaulter_trace
                  LEFT JOIN ODS.dbo.CT_Patient AS patient ON patient.PatientPKHash = defaulter_trace.PatientPKHash AND patient.SiteCode = defaulter_trace.SiteCode
                  LEFT JOIN (
                        SELECT DISTINCT
                            visits.SiteCode,
                            visits.PatientPKHash,
                            visits.DifferentiatedCare
                        FROM ODS.dbo.CT_PatientVisits AS visits
                        INNER JOIN ODS.dbo.Intermediate_LastVisitDate AS last_visit ON visits.SiteCode = last_visit.SiteCode AND visits.PatientPKHash = last_visit.PatientPKHash AND visits.VisitDate = last_visit.LastVisitDate
         ) latest_differentiated_care ON latest_differentiated_care.PatientPKHash = defaulter_trace.PatientPKHash AND latest_differentiated_care.SiteCode = patient.SiteCode
         WHERE IsFinalTrace = 'Yes' AND defaulter_trace.SiteCode >= 0
     ) visits_data