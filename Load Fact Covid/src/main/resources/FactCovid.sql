Select
    monotonically_increasing_id() + 1 as Factkey,
    Patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    VisitID,
    Covid19AssessmentDate.Datekey As Covid19AssessmentDateKey,
    ReceivedCOVID19Vaccine ,
    DateGivenFirstDose.Datekey As DateGivenFirstDoseKey,
    FirstDoseVaccineAdministered,
    DateGivenSecondDose.Datekey As DateGivenSecondDoseKey,
    SecondDoseVaccineAdministered ,
    VaccinationStatus ,
    VaccineVerification ,
    BoosterGiven ,
    BoosterDose ,
    BoosterDoseDate.Datekey As BoosterDoseDateKey,
    EverCOVID19Positive ,
    COVID19TestDate.Datekey As COVID19TestDateKey,
    PatientStatus ,
    AdmissionStatus ,
    AdmissionUnit ,
    MissedAppointmentDueToCOVID19 ,
    COVID19PositiveSinceLasVisit ,
    COVID19TestDateSinceLastVisit ,
    PatientStatusSinceLastVisit,
    AdmissionStatusSinceLastVisit,
    AdmissionStartDate.Datekey As AdmissionStartDateKey,
    AdmissionEndDate.Datekey As  AdmissionEndDateKey,
    AdmissionUnitSinceLastVisit,
    SupplementalOxygenReceived,
    PatientVentilated,
    TracingFinalOutcome ,
    CauseOfDeath,
    cast(current_date() as date) as LoadDate
from Covid
    left join DimPatient as patient on patient.PatientPKHash = Covid.PatientPKHash and patient.SiteCode = Covid.SiteCode
    left join DimFacility as facility on facility.MFLCode = Covid.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = Covid.SiteCode
    left join DimPartner as partner on partner.PartnerName = upper(MFL_partner_agency_combination.SDP)
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = Covid.AgeLastVisit
    left join DimDate as Covid19AssessmentDate on Covid19AssessmentDate.Date = Covid.Covid19AssessmentDate
    left join DimDate as DateGivenFirstDose  on DateGivenFirstDose.Date = Covid.DateGivenFirstDose
    left join DimDate as BoosterDoseDate  on BoosterDoseDate.Date = Covid.BoosterDoseDate
    left join DimDate as DateGivenSecondDose  on DateGivenSecondDose.Date = Covid.DateGivenSecondDose
    left join DimDate as COVID19TestDate  on COVID19TestDate.Date = Covid.COVID19TestDate
    left join DimDate as AdmissionStartDate  on AdmissionStartDate.Date = Covid.AdmissionStartDate
    left join DimDate as AdmissionEndDate  on AdmissionEndDate.Date = Covid.AdmissionEndDate
Where RowNumber = 1;