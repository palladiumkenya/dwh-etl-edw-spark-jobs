select
    heis.PatientPk,
    heis.SiteCode,
    datediff(week, DOB, heis.DNAPCR1Date) as age_in_weeks_at_DNAPCR1Date,
    DOB,
    heis.DNAPCR1Date
from MNCH_HEIs as heis
    inner join pmtct_client_demographics as demographics on demographics.PatientPk = heis.PatientPK
    and demographics.SiteCode = heis.SiteCode
where datediff(week, DOB, heis.DNAPCR1Date) >=8 and datediff(week, DOB, heis.DNAPCR1Date) <= 48