Select
    pat.PatientKey,
    fac.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    StartARTDate.Date As StartARTDateKey,
    LastARTDate.DateKey  as LastARTDateKey,
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
    current_date() as LoadDate
from Patient
    left join DimPatient as Pat on pat.PatientPKHash=Patient.PatientPkHash and pat.SiteCode=Patient.SiteCode
    left join Dimfacility fac on fac.MFLCode=Patient.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = Patient.SiteCode
    left join DimPartner as partner on partner.PartnerName = upper(MFL_partner_agency_combination.SDP)
    left join DimAgeGroup as age_group on age_group.Age = Patient.AgeLastVisit
    left join DimDate as StartARTDate on StartARTDate.Date= Patient.StartARTDate
    left join DimDate as LastARTDate on  LastARTDate.Date=Patient.LastARTDate
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join Intermediate_ARTOutcomes As IOutcomes  on IOutcomes.PatientPKHash = Patient.PatientPkHash  and IOutcomes.SiteCode = Patient.SiteCode
    left join DimARTOutcome ARTOutcome on ARTOutcome.ARTOutcome=IOutcomes.ARTOutcome;