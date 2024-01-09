select
    heis.PatientPK,
    heis.SiteCode,
    HEIExitCritearia,
    HEIHIVStatus,
    StartARTDate
from dbo.MNCH_HEIs as heis
    inner join dbo.CT_ARTPatients as art on art.PatientPK = heis.PatientPk
    and art.SiteCode = heis.SiteCode
where HEIHIVStatus = 'Positive' and art.StartARTDate is not null