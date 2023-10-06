select
    distinct viral_loads.PatientID,
             viral_loads.SiteCode,
             viral_loads.PatientPK,
             OrderedbyDate,
             Replace(TestResult ,',','') as TestResult
from dbo.Intermediate_LatestViralLoads as viral_loads
         left join ODS.dbo.CT_ARTPatients as art_patient on art_patient.PatientPK = viral_loads.PatientPK
    and art_patient.SiteCode = viral_loads.SiteCode
         inner join ODS.dbo.intermediate_LatestObs as obs on obs.PatientPK=viral_loads.PatientPK and obs.SiteCode=viral_loads.SiteCode
where datediff(month, OrderedbyDate, eomonth(dateadd(mm,-1,getdate()))) <= 6
    and Pregnant='Yes'OR breastfeeding='Yes' and Gender='Female'
    and  DATEDIFF(DAY, DATEADD(DAY, -(CAST(FLOOR(CONVERT(FLOAT, GestationAge)) * 7 AS INT)), CAST(LMP AS DATE)), GETDATE()) <= 450