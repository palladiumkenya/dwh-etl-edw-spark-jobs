SELECT
        patient.PatientKey,
        facility.FacilityKey,
        agency.AgencyKey,
        partner.PartnerKey,
        age_group.AgeGroupKey,
        visit.DateKey as VisitDateKey,
        appointment.DateKey as NextAppointmentDateKey,
        pregnancy.DateKey as PregnancyEndDateKey,
        PrepVisits.VisitID,
        PrepVisits.BloodPressure,
        PrepVisits.Temperature,
        PrepVisits.Weight,
        PrepVisits.Height,
        PrepVisits.BMI,
        PrepVisits.STIScreening,
        PrepVisits.STISymptoms,
        case when  PrepVisits.STISymptoms  is not null then 1 else 0 end as STIPositive,
        case when  PrepVisits.STISymptoms  is  null then 1 else 0 end as STINegative,
        PrepVisits.STITreated,
        PrepVisits.Circumcised,
        PrepVisits.VMMCReferral,
        PrepVisits.LMP,
        PrepVisits.MenopausalStatus,
        PrepVisits.PregnantAtThisVisit,
        PrepVisits.EDD,
        PrepVisits.PlanningToGetPregnant,
        PrepVisits.PregnancyPlanned,
        PrepVisits.PregnancyEnded,
        PrepVisits.PregnancyEndDate,
        PrepVisits.PregnancyOutcome,
        PrepVisits.BirthDefects,
        PrepVisits.Breastfeeding,
        PrepVisits.FamilyPlanningStatus,
        PrepVisits.FPMethods,
        PrepVisits.AdherenceDone,
        PrepVisits.AdherenceOutcome,
        PrepVisits.AdherenceReasons,
        PrepVisits.SymptomsAcuteHIV,
        PrepVisits.ContraindicationsPrep,
        PrepVisits.PrepTreatmentPlan,
        PrepVisits.PrepPrescribed,
        PrepVisits.RegimenPrescribed,
        PrepVisits.MonthsPrescribed,
        PrepVisits.CondomsIssued,
        PrepVisits.Tobegivennextappointment,
        PrepVisits.Reasonfornotgivingnextappointment,
        PrepVisits.HepatitisBPositiveResult,
        PrepVisits.HepatitisCPositiveResult,
        PrepVisits.VaccinationForHepBStarted,
        PrepVisits.TreatedForHepB,
        PrepVisits.VaccinationForHepCStarted,
        PrepVisits.TreatedForHepC,
        PrepVisits.NextAppointment,
        PrepVisits.ClinicalNotes,
        current_date() as LoadDate
    from prep_patients
    left join PrepVisits as  PrepVisits on PrepVisits.PatientPK = prep_patients.PatientPK
        and PrepVisits.SiteCode = prep_patients.SiteCode
    left join DimPatient as patient on patient.PatientPKHash = prep_patients.PatientPK
        and patient.SiteCode = prep_patients.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = prep_patients.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP 
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimFacility as facility on facility.MFLCode = prep_patients.SiteCode
    left join DimAgeGroup as age_group on age_group.Age = round((months_between(coalesce(PrepVisits.VisitDate, current_date()),  patient.DOB)/12),0)
    left join DimDate as visit on visit.Date = PrepVisits.VisitDate
    left join DimDate as pregnancy on pregnancy.Date = PrepVisits.PregnancyEndDate
    left join DimDate as appointment on appointment.Date= PrepVisits.NextAppointment
WHERE patient.voided =0;