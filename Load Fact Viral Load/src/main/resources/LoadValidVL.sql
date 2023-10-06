select
    distinct viral_loads.PatientID,
             viral_loads.SiteCode,
             viral_loads.PatientPK,
             OrderedbyDate,
             Replace(TestResult ,',','') as TestResult
from dbo.Intermediate_LatestViralLoads as viral_loads
         left join ODS.dbo.CT_ARTPatients as art_patient on art_patient.PatientPK = viral_loads.PatientPK
    and art_patient.SiteCode = viral_loads.SiteCode
where datediff(month, OrderedbyDate, eomonth(dateadd(mm,-1,getdate()))) <= 6
  and art_patient.AgeLastVisit <= 24
union
/*clients who are above 24 years have a valid VL that is within the last 12 months from reporting period*/
select
    distinct viral_loads.PatientID,
             viral_loads.SiteCode,
             viral_loads.PatientPK,
             OrderedbyDate,
             Replace(TestResult ,',','') as TestResult
from dbo.Intermediate_LatestViralLoads as viral_loads
         left join ODS.dbo.CT_ARTPatients as art_patient on art_patient.PatientPK = viral_loads.PatientPK
    and art_patient.SiteCode = viral_loads.SiteCode
where datediff(month, OrderedbyDate, eomonth(dateadd(mm,-1,getdate()))) <= 12
  and art_patient.AgeLastVisit > 24