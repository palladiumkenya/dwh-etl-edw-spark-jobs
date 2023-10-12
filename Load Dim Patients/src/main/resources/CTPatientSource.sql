select
    distinct
    patients.PatientIDHash,
    patients.PatientPKHash,
    patients.PatientID,
    patients.PatientPK,
    patients.SiteCode,
    Gender,
    cast(DOB as date) as DOB,
    MaritalStatus,
    NupiHash,
    PatientType,
    PatientSource,
    baselines.eWHO as EnrollmentWHOKey,
    cast(format(coalesce(eWHODate, '1900-01-01'),'yyyyMMdd') as int) as DateEnrollmentWHOKey,
    bWHO as BaseLineWHOKey,
    cast(format(coalesce(bWHODate, '1900-01-01'),'yyyyMMdd') as int) as DateBaselineWHOKey,
    case
        when outcomes.ARTOutcome =  'V' then 1
        else 0
        end as IsTXCurr,
    cast(getdate() as date) as LoadDate
from
    dbo.CT_Patient as patients
        left join dbo.CT_PatientBaselines as baselines on patients.PatientPKHash = baselines.PatientPKHash
        and patients.SiteCode = baselines.SiteCode
        left join dbo.Intermediate_ARTOutcomes as outcomes on outcomes.PatientPKHash = patients.PatientPKHash
        and outcomes.SiteCode = patients.SiteCode