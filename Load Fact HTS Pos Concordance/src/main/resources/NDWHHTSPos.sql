Select
    hts_encounter.SiteCode,
    Facility_Name,
    SDP as PartnerName,
    emr.County,
    count (*) as HTSPos_total
from ODS.dbo.Intermediate_EncounterHTSTests as hts_encounter
         left join ODS.dbo.ALL_EMRSites as emr on emr.MFL_Code = hts_encounter.SiteCode
WHERE  TestDate  between  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1) and FinalTestResult='Positive' and SiteCode is not null and TestType in ('Initial Test', 'Initial')
Group by SiteCode, Facility_Name, SDP, County
