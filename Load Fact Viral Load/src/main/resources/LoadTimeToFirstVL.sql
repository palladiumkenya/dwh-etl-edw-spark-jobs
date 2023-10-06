select
    distinct baseline.PatientPK,
             baseline.SiteCode,
             case
                 when baseline.OrderedbyDate is not null and StartARTDate is not null then
                     case
                         when OrderedbyDate >= art_patients.StartARTDate then datediff(day, art_patients.StartARTDate, baseline.OrderedbyDate)
                     end
             end as TimetoFirstVL
from dbo.Intermediate_BaseLineViralLoads as baseline
left join ODS.dbo.CT_ARTPatients as art_patients on art_patients.PatientPK = baseline.PatientPK
    and art_patients.SiteCode = baseline.SiteCode