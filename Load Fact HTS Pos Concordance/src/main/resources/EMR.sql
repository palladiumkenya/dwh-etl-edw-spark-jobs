SELECT
    Row_Number () over (partition by FacilityCode order by statusDate desc) as Num,
    facilityCode
    ,facilityName
    ,CONVERT(varchar, value) AS value
    ,statusDate
    ,indicatorDate
FROM ODS.dbo.livesync_Indicator
where stage like '%EMR' and name like '%HTS_TESTED_POS' and indicatorDate=EOMONTH(DATEADD(mm,-1,GETDATE()))