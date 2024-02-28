Select  [DateRecieved],
        ROW_NUMBER()OVER(Partition by Sitecode Order by [DateRecieved] Desc) as Num ,
        SiteCode,
        cast(DateRecieved as date) As DateReceived,
        EmrName as   Emr,
        Name,
        Start,
        PatientCount
from ODS.dbo.CT_FacilityManifest
where cast  (DateRecieved as date) > DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)
  and cast (DateRecieved as date) <= DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)