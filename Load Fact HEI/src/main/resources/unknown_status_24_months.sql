select
    heis.PatientPK,
    heis.SiteCode,
    heis.FinalyAntibody,
    datediff(month, DOB, current_date()) as current_age_in_months
from MNCH_HEIs as heis
    inner join pmtct_client_demographics on pmtct_client_demographics.PatientPk = heis.PatientPk and pmtct_client_demographics.SiteCode = heis.SiteCode
where heis.FinalyAntibody is null or heis.FinalyAntibody = '' and datediff(month, DOB, current_date()) >= 24