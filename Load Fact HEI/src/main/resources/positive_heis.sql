select
    heis.PatientPK,
    heis.SiteCode,
    HEIExitCritearia,
    HEIHIVStatus
from MNCH_HEIs as heis
where HEIHIVStatus = 'Positive'