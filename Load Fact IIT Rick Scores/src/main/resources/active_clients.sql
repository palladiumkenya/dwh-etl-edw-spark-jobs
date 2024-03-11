select
    PatientPk,
    SiteCode
from ODS.dbo.Intermediate_ARTOutcomes
where ARTOutcome = 'V'