select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    agegroup.AgeGroupKey,
    evaluation.DateKey as RiskEvaluationDateKey,
    int(appointment.DateKey) as LastVisitAppointmentGivenDateKey,
    float(RiskScore) as LatestRiskScore,
    RiskCategory as LastestRiskCategory
from iit_risk_scores_ordering as risk_scores
         inner join active_clients on active_clients.PatientPK = risk_scores.PatientPK
    and active_clients.SiteCode = risk_scores.SiteCode
         left join patient on patient.PatientPKHash = risk_scores.PatientPKHash
    and patient.SiteCode = risk_scores.SiteCode
         left join facility on facility.MFLCode = risk_scores.SiteCode
         left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = risk_scores.SiteCode
         left join partner on partner.PartnerName = MFL_partner_agency_combination.SDP
         left join agency on agency.AgencyName = MFL_partner_agency_combination.Agency
         left join DimDate as evaluation on evaluation.Date = risk_scores.RiskEvaluationDate
         left join agegroup on agegroup.Age = datediff(YEAR, patient.DOB, risk_scores.RiskEvaluationDate)
         left join appointments_from_last_visit on appointments_from_last_visit.PatientPK = risk_scores.PatientPK
    and appointments_from_last_visit.SiteCode = risk_scores.SiteCode
         left join DimDate as appointment on appointment.Date = appointments_from_last_visit.NextAppointment
where rank = 1 and patient.voided = 0