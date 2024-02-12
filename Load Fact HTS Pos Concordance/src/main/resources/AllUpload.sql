SELECT
    DateRecieved as DateUploaded,
    SiteCode,
    ROW_NUMBER()OVER(Partition by Sitecode Order by DateRecieved Desc) as Num
FROM ods.dbo.CT_FacilityManifest m