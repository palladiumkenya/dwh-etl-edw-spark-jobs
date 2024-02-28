select
    heis.PatientPk,
    heis.SiteCode,
    latest_cwc_visit.InfantFeeding,
    datediff(month, DOB, latest_cwc_visit.VisitDate) as age_in_months_as_last_cwc_visit
from MNCH_HEIs as heis
    inner join latest_cwc_visit on latest_cwc_visit.PatientPK = heis.PatientPK
    and latest_cwc_visit.SiteCode = heis.Sitecode
    inner join pmtct_client_demographics on pmtct_client_demographics.PatientPk = heis.PatientPk
    and pmtct_client_demographics.SiteCode = heis.SiteCode