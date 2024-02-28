SELECT
    try_cast([SiteCode] as int) SiteCode,
    [FacilityName] As  FacilityName,
    [County],
    Positive_Total,
    ReportMonth_Year
FROM dbo.HTS_DHIS2
WHERE ReportMonth_Year =CONVERT(VARCHAR(6), DATEADD(MONTH, -1, GETDATE()), 112) and ISNUMERIC(SiteCode) >0