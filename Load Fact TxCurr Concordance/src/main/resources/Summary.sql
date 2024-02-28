Select
    coalesce (NDW_CurTx.SiteCode, null ) As MFLCode,
    fac.Facility_Name As FacilityName,
    fac.PartnerName,
    fac.County,
    fac.emr as EMR,
    DHIS2_CurTx.CurrentOnART_Total As KHIS_TxCurr,
    NDW_CurTx.CurTx_total AS DWH_TXCurr,
    CAST(ROUND(LatestEMR.EMRValue, 2) AS float) AS EMR_TxCurr,
    CAST(ROUND(LatestEMR.EMRValue - CurTx_total, 2) AS float) AS Diff_EMR_DWH,
    DHIS2_CurTx.CurrentOnART_Total-CurTx_total As DiffKHISDWH,
    CAST(ROUND(DHIS2_CurTx.CurrentOnART_Total - LatestEMR.EMRValue, 2) AS FLOAT) AS DiffKHISEMR,
    CAST(ROUND((CAST(LatestEMR.EMRValue AS DECIMAL(7,2)) - CAST(NDW_CurTx .CurTx_total AS DECIMAL(7,2)))
                   /NULLIF(CAST(LatestEMR.EMRValue  AS DECIMAL(7,2)),0)* 100, 2) AS float) AS Percent_variance_EMR_DWH,
    CAST(ROUND((CAST(DHIS2_CurTx.CurrentOnART_Total AS DECIMAL(7,2)) - CAST(NDW_CurTx .CurTx_total AS DECIMAL(7,2)))
                   /CAST(DHIS2_CurTx.CurrentOnART_Total  AS DECIMAL(7,2))* 100, 2) AS float) AS Percent_variance_KHIS_DWH,
    CAST(ROUND((CAST(DHIS2_CurTx.CurrentOnART_Total AS DECIMAL(7,2)) - CAST(LatestEMR.EMRValue AS DECIMAL(7,2)))
                   /CAST(DHIS2_CurTx.CurrentOnART_Total  AS DECIMAL(7,2))* 100, 2) AS float) AS Percent_variance_KHIS_EMR,
    cast (Upload.DateUploaded as date) As DateUploaded,
    case when CompletenessStatus is null then 'Complete' else 'Incomplete' End As Completeness,
    DWAPI.DwapiVersion
from NDW_CurTx
left join LatestEMR on NDW_CurTx.SiteCode=LatestEMR.facilityCode
LEFT JOIN DWAPI ON DWAPI.SiteCode= LatestEMR.facilityCode
left join DHIS2_CurTx on NDW_CurTx.SiteCode=DHIS2_CurTx.SiteCode
left join Upload on NDW_CurTx.SiteCode=Upload.SiteCode
left join Uploaddata on NDW_CurTx.SiteCode=Uploaddata.MFL_Code
left join Facilityinfo fac on NDW_CurTx.SiteCode=fac.MFL_Code