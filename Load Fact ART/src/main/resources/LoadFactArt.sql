Select
    pat.PatientKey,
    fac.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    StartARTDate.Date As StartARTDateKey,
    LastARTDate.DateKey  as LastARTDateKey,
    lastreg.DrugKey As CurrentRegimen,
    lastregline.RegimenLineKey As CurrentRegimenLine,
    firstreg.DrugKey As StartRegimen,
    firstregline.RegimenLineKey As StartRegimenLine,
    AgeAtEnrol,
    AgeAtARTStart,
    TimetoARTDiagnosis,
    TimetoARTEnrollment,
    PregnantARTStart,
    PregnantAtEnrol,
    LastEncounterDate As LastVisitDate,
    Patient.NextAppointmentDate,
    StartARTAtThisfacility,
    Patient.PreviousARTStartDate,
    Patient.PreviousARTRegimen,
    outcome.ARTOutcome,
    current_date() as LoadDate
from  Patient
    left join DimPatient as Pat on pat.PatientPK=Patient.PatientPk
    left join Dimfacility fac on fac.MFLCode=Patient.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = Patient.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgeGroup as age_group on age_group.Age = Patient.AgeLastVisit
    left join DimDate as StartARTDate on StartARTDate.Date= Patient.StartARTDate
    left join DimDate as LastARTDate on  LastARTDate.Date=Patient.LastARTDate
    left join DimDrug lastreg on lastreg.drug=Patient.LastRegimen
    left join DimDrug firstreg on firstreg.drug=Patient.StartRegimen
    left join DimRegimenLine lastregline on lastregline.RegimenLine=Patient.LastRegimenLine
    left join DimRegimenLine firstregline on firstregline.RegimenLine=Patient.StartRegimenLine
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join Intermediate_ARTOutcomes  outcome on outcome.PatientPK=Patient.PatientPK and outcome.SiteCode=Patient.SiteCode