select
    SiteCode,
    PatientPK,
    PatientPKHash,
    EnrolledFacilityName,
    ReferralDate,
    DateEnrolled,
    DatePrefferedToBeEnrolled,
    FacilityReferredTo,
    HandedOverTo,
    HandedOverToCadre,
    ReportedCCCNumber,
    row_number() over(partition by Sitecode,PatientPK order by DateEnrolled desc) as row_num
from dbo.HTS_ClientLinkages