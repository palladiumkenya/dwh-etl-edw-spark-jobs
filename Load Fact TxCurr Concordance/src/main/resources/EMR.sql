SELECT
    Row_Number () over (partition by FacilityCode order by statusDate desc) as Num,
    facilityCode
     ,facilityName
     ,CAST(CONVERT (varchar, value ) AS DECIMAL(10, 4)) value
     ,statusDate
     ,indicatorDate
FROM dbo.livesync_Indicator
WHERE stage like '%EMR' and name like '%TX_CURR' and indicatorDate= EOMONTH(DATEADD(mm,-1,GETDATE()))