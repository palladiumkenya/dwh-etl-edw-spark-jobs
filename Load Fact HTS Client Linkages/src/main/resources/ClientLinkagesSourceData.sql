select
    SiteCode,
    PatientPKHash,
    EnrolledFacilityName,
    ReferralDate,
    DateEnrolled,
    DatePrefferedToBeEnrolled,
    FacilityReferredTo,
    HandedOverTo,
    HandedOverToCadre,
    convert(nvarchar(64), hashbytes('SHA2_256', cast(ReportedCCCNumber as nvarchar(36))), 2) as ReportedCCCNumber,
    row_number() over(partition by Sitecode,PatientPK order by DateEnrolled desc) as row_num
from dbo.HTS_ClientLinkages