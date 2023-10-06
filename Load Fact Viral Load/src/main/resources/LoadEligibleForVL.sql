select
    distinct PatientID,
             PatientPK,
             SiteCode,
             case
                 when datediff(month,StartARTDate, getdate()) >=3 then 1
                 when DATEDIFF(MONTH, StartARTDate, getdate()) <3 then 0
             end as EligibleVL
from dbo.CT_ARTPatients