Select
    pat.PatientKey,
    fac.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    StartARTDate.DateKey As StartARTDateKey,
    LastARTDate.DateKey  as LastARTDateKey,
    DateConfirmedPos.DateKey as DateConfirmedPosKey,
    ARTOutcome.ARTOutcomeKey,
    lastRegimen As CurrentRegimen,
    LastRegimenLine As CurrentRegimenline,
    StartRegimen,
    StartRegimenLine,
    AgeAtEnrol,
    AgeAtARTStart,
    AgeLastVisit,
    CASE
        WHEN floor( AgeLastVisit ) < 15 THEN
            'Child'
        WHEN floor( AgeLastVisit ) >= 15 THEN
            'Adult' ELSE 'Aii'
        END AS Agegrouping,
    TimetoARTDiagnosis,
    TimetoARTEnrollment,
    PregnantARTStart,
    PregnantAtEnrol,
    LastVisitDate,
    Patient.NextAppointmentDate,
    StartARTAtThisfacility,
    PreviousARTStartDate,
    PreviousARTRegimen,
    WhoStage,
    PHQ_9_rating,
    CASE WHEN LatestDepressionScreening.Patientpkhash is not null then 1 else 0 End as ScreenedForDepression,
    coalesce(ncd_screening.ScreenedBPLastVisit, 0) as ScreenedBPLastVisit,
    coalesce(ncd_screening.ScreenedDiabetes, 0) as ScreenedDiabetes,
    end_month.DateKey as AsOfDateKey,
    current_date() as LoadDate
from Patient
left join DimPatient as Pat on pat.PatientPKHash=Patient.PatientPkHash and Pat.SiteCode=Patient.SiteCode
left join Dimfacility fac on fac.MFLCode=Patient.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code  = Patient.SiteCode
left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join DimAgeGroup as age_group on age_group.Age = Patient.AgeLastVisit
left join DimDate as StartARTDate on StartARTDate.Date = Patient.StartARTDate
left join DimDate as LastARTDate on  LastARTDate.Date=Patient.LastARTDate
left join DimDate as DateConfirmedPos on  DateConfirmedPos.Date=Patient.DateConfirmedHIVPositive
left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join Intermediate_ARTOutcomes As IOutcomes  on IOutcomes.PatientPKHash = Patient.PatientPkHash  and IOutcomes.SiteCode = Patient.SiteCode
left join DimARTOutcome ARTOutcome on ARTOutcome.ARTOutcome=IOutcomes.ARTOutcome
left join LatestDepressionScreening on LatestDepressionScreening.Patientpkhash=patient.patientpkhash and LatestDepressionScreening.sitecode=patient.sitecode
left join ncd_screening on ncd_screening.PatientPKHash = patient.PatientPKHash and ncd_screening.SiteCode = patient.SiteCode
left join DimDate as end_month on end_month.Date = date_sub(date_trunc('MONTH', current_date()), 1)
WHERE pat.voided = 0;