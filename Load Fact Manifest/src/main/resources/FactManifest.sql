Select
    ID as  manifestId,
    Cast(max(m.DateRecieved) as date) timeId,
    max(m.SiteCode) facilityId,
    max(coalesce(h.emr,'Unkown')) emrId,
    'CT' docketId,
    1 upload,
    [Start],
    [End]
from ODS.dbo.CT_FacilityManifest m
    INNER JOIN ODS.dbo.ALL_EMRSites h on m.SiteCode=h.MFL_Code
GROUP BY ID,[start],[end],YEAR(m.DateRecieved), MONTH(m.DateRecieved), SiteCode

UNION ALL

SELECT  Id  AS manifestId,
        CAST(MAX(m.DateArrived) AS DATE) AS timeId,
        MAX(m.SiteCode) AS facilityId,
        MAX(COALESCE(h.emr, 'Unknown')) AS emrId,
        'HTS' AS docketId,
        1 AS upload,
        [Start],
        [End]
FROM ODS.DBO.HTS_FacilityManifest m
    INNER JOIN ODS.DBO.ALL_EMRSites h ON m.SiteCode = h.MFL_Code
GROUP BY    ID,[start],[end],
            YEAR(DateArrived),
            MONTH(DateArrived), SiteCode

UNION ALL

SELECT Id AS manifestId,
       CAST(MAX(m.DateArrived) AS DATE) AS timeId,
       MAX(m.SiteCode) AS facilityId,
       MAX(COALESCE(h.emr, 'Unknown')) AS emrId,
       'PKV' AS docketId,
       1 AS upload ,
       [Start],
       [End]
FROM ODS.dbo.CBS_FacilityManifest m
    INNER JOIN ODS.dbo.all_emrsites h ON m.SiteCode = h.MFL_Code
GROUP BY ID,[start],[end],
         YEAR(DateArrived),
         MONTH(DateArrived), SiteCode
