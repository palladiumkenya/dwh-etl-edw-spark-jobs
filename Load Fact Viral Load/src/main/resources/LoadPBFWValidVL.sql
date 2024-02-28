select distinct
    pbfw.SiteCode,
    pbfw.PatientPK,
    viral_loads.OrderedbyDate,
    Replace(viral_loads.TestResult ,',','') as TestResult,
    ISNUMERIC(Replace(viral_loads.TestResult ,',','') ) AS isnumericTestResult -- added to solve isnumeric error in sparksql
from dbo.Intermediate_Pbfw as pbfw
    left join ODS.dbo.Intermediate_LatestViralLoads as viral_loads on viral_loads.PatientPK = pbfw.PatientPK
    and viral_loads.SiteCode = pbfw.SiteCode
    left join ODS.dbo.CT_ARTPatients as art_patient on art_patient.PatientPK = pbfw.PatientPK
    and art_patient.SiteCode = pbfw.SiteCode
where datediff(month, OrderedbyDate, eomonth(dateadd(mm,-1,getdate()))) <= 6