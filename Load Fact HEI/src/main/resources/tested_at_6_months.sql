select
    heis.PatientPk,
    heis.SiteCode,
    datediff(month, DOB, heis.DNAPCR2Date) as age_in_months_at_DNAPCR2Date
from MNCH_HEIs as heis
    inner join pmtct_client_demographics as demographics on demographics.PatientPk = heis.PatientPK and demographics.SiteCode = heis.SiteCode
where datediff(month, DOB, heis.DNAPCR2Date) = 6