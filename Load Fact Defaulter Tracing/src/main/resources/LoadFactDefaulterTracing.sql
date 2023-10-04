select
      patient.PatientKey,
      facility.FacilityKey,
      partner.PartnerKey,
      agency.AgencyKey,
      age_group.AgeGroupKey,
      visit.DateKey as VisitDateKey,
      diff_care.DifferentiatedCareKey,
      VisitID,
      TracingType,
      TracingOutcome,
      Comments,
      is_reached,
      current_date() as LoadDate
from visits_data
     left join DimFacility as facility on facility.MFLCode = visits_data.SiteCode
     left join DimPatient as patient on patient.PatientPKHash = visits_data.PatientPKHash and patient.SiteCode = visits_data.SiteCode
     left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = visits_data.SiteCode
     left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
     left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
     left join DimDate as visit on visit.Date = visits_data.VisitDate
     left join DimAgeGroup as age_group on age_group.Age = visits_data.AgeAtVisit
     left join DimDifferentiatedCare as diff_care on diff_care.DifferentiatedCare = visits_data.DifferentiatedCare;