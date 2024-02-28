SELECT
    [SiteCode],
    [FacilityName],
    [County],
    [CurrentOnART_Total],
    ReportMonth_Year
FROM dbo.CT_DHIS2
WHERE ReportMonth_Year =CONVERT(VARCHAR(6), DATEADD(MONTH, -1, GETDATE()), 112) and ISNUMERIC(SiteCode) >0