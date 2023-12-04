select
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    patient.PatientKey,
    as_of.DateKey AS AsOfDateKey,
    CASE WHEN txcurr_report.ARTOutcome = 'V' THEN 1 ELSE 0 END AS IsTXCurr,
    art_outcome.ARTOutcomeKey,
    current_date() as LoadDate
from txcurr_report
left join as_of on as_of.Date = txcurr_report.AsOfDate
left join facility on facility.MFLCode = txcurr_report.MFLCode
left join patient on patient.PatientPKHash =  txcurr_report.PatientPKHash and patient.SiteCode = txcurr_report.MFLCode
left join mfl_partner_agency_combination on mfl_partner_agency_combination.MFL_Code = txcurr_report.MFLCode
left join partner on partner.PartnerName = mfl_partner_agency_combination.SDP
left join agency on agency.AgencyName = mfl_partner_agency_combination.Agency
left join art_outcome on art_outcome.ARTOutcome = txcurr_report.ARTOutcome
WHERE patient.voided =0;