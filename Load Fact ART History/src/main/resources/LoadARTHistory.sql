select
    facility.FacilityKey,
    partner.PartnerName as PartnerKey,
    agency.AgencyName as AgencyKey,
    patient.PatientKey,
    as_of.DateKey as AsOfDateKey,
    case
    when txcurr_report.ARTOutcome = 'V' then 1
    else 0
end as IsTXCurr,
	art_outcome.ARTOutcome as ARTOutcomeKey,
	current_date() as LoadDate
from txcurr_report
left join as_of on as_of.Date = txcurr_report.AsOfDate
left join facility on facility.MFLCode = txcurr_report.MFLCode
left join patient on patient.PatientPKHash =  txcurr_report.PatientPKHash and patient.SiteCode = txcurr_report.MFLCode
left join mfl_partner_agency_combination on mfl_partner_agency_combination.MFL_Code = txcurr_report.MFLCode
left join partner on partner.PartnerName = upper(mfl_partner_agency_combination.SDP)
left join agency on agency.AgencyName = mfl_partner_agency_combination.Agency
left join art_outcome on art_outcome.ARTOutcome = txcurr_report.ARTOutcome