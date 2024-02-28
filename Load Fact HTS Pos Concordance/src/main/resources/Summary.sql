Select
    Coalesce (DHIS2_HTSPos.SiteCode, NDW_HTSPos.sitecode,LatestEMR.facilityCode ) As MFLCode,
    Coalesce (NDW_HTSPos.Facility_Name, DHIS2_HTSPos.FacilityName) As FacilityName,
    fac.SDP As SDP,
    fac.emr as EMR,
    Coalesce (NDW_HTSPos.County, DHIS2_HTSPos.County) As County,
    DHIS2_HTSPos.Positive_Total As KHIS_HTSPos,
    Coalesce (NDW_HTSPos.HTSPos_total, 0 )AS DWH_HTSPos,
    LatestEMR.EMRValue As EMR_HTSPos,
    LatestEMR.EMRValue-HTSPos_total As Diff_EMR_DWH,
    DHIS2_HTSPos.Positive_Total-HTSPos_total As DiffKHISDWH,
    DHIS2_HTSPos.Positive_Total-LatestEMR.EMRValue As DiffKHISEMR,
    CAST(ROUND((CAST(LatestEMR.EMRValue AS DECIMAL(7,2)) - CAST(coalesce(NDW_HTSPos.HTSPos_total, null) AS DECIMAL(7,2)))
                   /NULLIF(CAST(LatestEMR.EMRValue  AS DECIMAL(7,2)),0)* 100, 2) AS float) AS Percent_variance_EMR_DWH,
    CAST(ROUND((CAST(DHIS2_HTSPos.Positive_Total AS DECIMAL(7,2)) - CAST(NDW_HTSPos.HTSPos_total AS DECIMAL(7,2)))
                   /CAST(DHIS2_HTSPos.Positive_Total  AS DECIMAL(7,2))* 100, 2) AS float) AS Percent_variance_KHIS_DWH,
    CAST(ROUND((CAST(DHIS2_HTSPos.Positive_Total AS DECIMAL(7,2)) - CAST(LatestEMR.EMRValue AS DECIMAL(7,2)))
                   /CAST(DHIS2_HTSPos.Positive_Total  AS DECIMAL(7,2))* 100, 2) AS float) AS Percent_variance_KHIS_EMR,
    cast (Upload.DateUploaded as date) As DateUploaded,
    DWAPI.DwapiVersion
from DHIS2_HTSPos
left join LatestEMR on DHIS2_HTSPos.sitecode=LatestEMR.facilityCode
LEFT JOIN DWAPI ON DWAPI.SiteCode= LatestEMR.facilityCode
left join NDW_HTSPos on NDW_HTSPos.sitecode=DHIS2_HTSPos.SiteCode
left join Upload on NDW_HTSPos.SiteCode=Upload.SiteCode
left join Facilityinfo fac on DHIS2_HTSPos.SiteCode=fac.MFL_Code
where DHIS2_HTSPos.Positive_Total is not null