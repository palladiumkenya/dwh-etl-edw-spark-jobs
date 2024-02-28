SELECT
    MFLCode SiteCode,
    FacilityName,
    PartnerName SDP,
    County  collate Latin1_General_CI_AS County,
    SUM(positive) AS HTSPos_total
FROM NDWH.dbo.FactHTSClientTests link
LEFT JOIN NDWH.dbo.DimPatient AS pat ON link.PatientKey = pat.PatientKey
LEFT JOIN NDWH.dbo.DimPartner AS part ON link.PartnerKey = part.PartnerKey
LEFT JOIN NDWH.dbo.DimFacility AS fac ON link.FacilityKey = fac.FacilityKey
LEFT JOIN NDWH.dbo.DimAgency AS agency ON link.AgencyKey = agency.AgencyKey
WHERE link.DateTestedKey between  DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) and DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1) and FinalTestResult='Positive' and MFLCode is not null and TestType in ('Initial Test', 'Initial')
GROUP BY MFLCode, FacilityName, PartnerName, County