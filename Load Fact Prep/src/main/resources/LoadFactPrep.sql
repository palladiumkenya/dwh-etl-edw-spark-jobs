select
    patient.PatientKey,
    facility.FacilityKey,
    agency.AgencyKey,
    partner.PartnerKey,
    age_group.AgeGroupKey,
    latest_prep_visits.VisitID,
    visit.DateKey as VisitDateKey,
    latest_prep_visits.BloodPressure,
    latest_prep_visits.Temperature,
    latest_prep_visits.Weight,
    latest_prep_visits.Height,
    latest_prep_visits.BMI,
    latest_prep_visits.STIScreening,
    latest_prep_visits.STISymptoms,
    latest_prep_visits.STITreated,
    latest_prep_visits.Circumcised,
    latest_prep_visits.VMMCReferral,
    cast(latest_prep_visits.LMP as date) as LMP,
    latest_prep_visits.MenopausalStatus,
    latest_prep_visits.PregnantAtThisVisit,
    cast(latest_prep_visits.EDD as date) as EDD,
    latest_prep_visits.PlanningToGetPregnant,
    latest_prep_visits.PregnancyPlanned,
    latest_prep_visits.PregnancyEnded,
    pregnancy.DateKey as PregnancyEndDateKey,
    latest_prep_visits.PregnancyOutcome,
    latest_prep_visits.BirthDefects,
    latest_prep_visits.Breastfeeding,
    latest_prep_visits.FamilyPlanningStatus,
    latest_prep_visits.FPMethods,
    latest_prep_visits.AdherenceDone,
    latest_prep_visits.AdherenceOutcome,
    latest_prep_visits.AdherenceReasons,
    latest_prep_visits.SymptomsAcuteHIV,
    latest_prep_visits.ContraindicationsPrep,
    latest_prep_visits.PrepTreatmentPlan,
    latest_prep_visits.PrepPrescribed,
    latest_prep_visits.RegimenPrescribed,
    latest_prep_visits.MonthsPrescribed,
    latest_prep_visits.CondomsIssued,
    latest_prep_visits.Tobegivennextappointment,
    latest_prep_visits.Reasonfornotgivingnextappointment,
    latest_prep_visits.HepatitisBPositiveResult,
    latest_prep_visits.HepatitisCPositiveResult,
    latest_prep_visits.VaccinationForHepBStarted,
    latest_prep_visits.TreatedForHepB,
    latest_prep_visits.VaccinationForHepCStarted,
    latest_prep_visits.TreatedForHepC,
    cast(latest_prep_visits.NextAppointment as date) as NextAppointment,
    latest_prep_visits.ClinicalNotes,
    latest_prep_assessments.ClientRisk,
    latest_prep_assessments.EligiblePrep,
    assessment_date.DateKey As AssessmentVisitDateKey,
    latest_prep_assessments.ScreenedPrep,
    latest_exits.ExitReason,
    exits.DateKey as ExitdateKey,
    refills.RefilMonth1,
    refills.TestResultsMonth1,
    refill_month_1.DateKey as DateTestMonth1Key,
    dispense_month_1.DateKey as DateDispenseMonth1,
    refills.RefilMonth3,
    refills.TestResultsMonth3,
    refill_month_3.DateKey as DateTestMonth3Key,
    dispense_month_3.DateKey as DateDispenseMonth3,
    current_date() as LoadDate
from prep_patients
    left join Intermediate_PrepLastVisit as  latest_prep_visits on latest_prep_visits.PatientPKHash = prep_patients.PatientPKHash
    and latest_prep_visits.SiteCode = prep_patients.SiteCode
    left join latest_exits on latest_exits.PatientPKHash = prep_patients.PatientPKHash
    and latest_exits.SiteCode = prep_patients.SiteCode
    left join Intermediate_PrepRefills as refills on refills.PatientPKHash = prep_patients.PatientPKHash
    and refills.SiteCode = prep_patients.SiteCode
    left join latest_prep_assessments on latest_prep_assessments.PatientPKHash = prep_patients.PatientPKHash
    and latest_prep_assessments.SiteCode = prep_patients.SiteCode
    left join DimPatient as patient on patient.PatientPKHash = prep_patients.PatientPKHash
    and patient.SiteCode = prep_patients.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = prep_patients.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimFacility as facility on facility.MFLCode = prep_patients.SiteCode
    left join DimAgeGroup as age_group on age_group.Age = round((months_between(coalesce(latest_prep_visits.VisitDate, current_date()), cast(patient.DOB as date))/12),0)
    left join DimDate as visit on visit.Date = latest_prep_visits.VisitDate
    left join DimDate as pregnancy on pregnancy.Date = latest_prep_visits.PregnancyEndDate
    left join DimDate as appointment on appointment.Date= latest_prep_visits.NextAppointment
    left join DimDate as assessment_date on assessment_date.Date = latest_prep_assessments.VisitDate
    left join DimDate as exits on exits.Date = latest_exits.ExitDate
    left join DimDate as refill_month_1 on refill_month_1.Date = refills.TestDateMonth1
    left join DimDate as dispense_month_1 on dispense_month_1.Date = refills.DispenseDateMonth1
    left join DimDate as refill_month_3 on refill_month_3.Date = refills.TestDateMonth3
    left join DimDate as dispense_month_3 on dispense_month_3.Date = refills.DispenseDateMonth3