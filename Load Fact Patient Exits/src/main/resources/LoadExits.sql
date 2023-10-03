Select
    Exits.PatientPKHash,
    Exits.SiteCode,
    Died.dtDead As dtDead,
    LTFU.dtLTFU As dtLTFU,
    TransferOut.dtTO As dtTO,
    Stopped.dtARTStop As dtARTStop
from (
         select
             PatientID,
             PatientPKHash,
             SiteCode
         from ODS.dbo.CT_PatientStatus
         where ExitReason is not null
     )	Exits
         left join (
    select
        PatientID,
        PatientPKHash,
        SiteCode,
        ExitDate as dtDead
    from ODS.dbo.CT_PatientStatus
    where ExitReason in ('Died','death')
) Died on Died.PatientPKHash=Exits.PatientPKHash and Died.SiteCode=Exits.SiteCode
         left join (
    select
        PatientID,
        PatientPKHash,
        SiteCode,
        ExitDate as dtARTStop
    from ODS.dbo.CT_PatientStatus
    where ExitReason in ('Stopped','Stopped Treatment')
) [Stopped] on [Stopped].PatientPKHash=Exits.PatientPKHash and [Stopped].SiteCode=Exits.SiteCode
         left join (
    Select
        PatientID,
        PatientPK,
        SiteCode,
        ExitDate as dtTO
    from ODS.dbo.CT_PatientStatus
    where ExitReason in ('Transfer Out','transfer_out','Transferred out','Transfer')
) TransferOut on TransferOut.PatientPK=Exits.PatientPKHash and TransferOut.SiteCode=Exits.SiteCode
         left join (
    Select
        PatientID,
        PatientPKHash,
        SiteCode,
        ExitDate as dtLTFU
    from ODS.dbo.CT_PatientStatus
    where ExitReason in ('Lost','Lost to followup','LTFU')
) LTFU on LTFU.PatientPKHash=Exits.PatientPKHash and LTFU.SiteCode=Exits.SiteCode