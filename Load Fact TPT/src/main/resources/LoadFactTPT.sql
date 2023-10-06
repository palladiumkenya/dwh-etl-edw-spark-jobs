SELECT
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    tb_start_treatment.DateKey AS StartTBTreatmentDateKey,
    tb_diagnosis.DateKey AS TBDiagnosisDateKey,
    combined_ipt_data.OnIPT,
    combined_ipt_data.hasTB,
    current_date() as LoadDate
FROM
    combined_ipt_data
        LEFT JOIN DimPatient AS patient ON patient.PatientPKHash = combined_ipt_data.PatientPKHash
        AND patient.SiteCode = combined_ipt_data.SiteCode
        LEFT JOIN DimFacility AS facility ON facility.MFLCode = combined_ipt_data.SiteCode
        LEFT JOIN DimDate AS tb_start_treatment ON tb_start_treatment.Date = combined_ipt_data.StartTBTreatmentDate
        LEFT JOIN DimDate AS tb_diagnosis ON tb_diagnosis.Date = combined_ipt_data.TBDiagnosisDate
        LEFT JOIN MFL_partner_agency_combination ON MFL_partner_agency_combination.MFL_Code = combined_ipt_data.SiteCode
        LEFT JOIN DimPartner AS partner ON partner.PartnerName = MFL_partner_agency_combination.SDP
        LEFT JOIN DimAgency AS agency ON agency.AgencyName = MFL_partner_agency_combination.Agency
        LEFT JOIN DimAgeGroup AS age_group ON age_group.Age = combined_ipt_data.AgeLastVisit