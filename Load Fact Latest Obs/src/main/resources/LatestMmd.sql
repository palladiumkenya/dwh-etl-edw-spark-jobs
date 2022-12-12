select
    distinct PatientPK,
             PatientID,
             SiteCode,
             case
                 when abs(datediff(day,LastEncounterDate, NextAppointmentDate)) <=84 then 0
                 when abs(datediff(day,LastEncounterDate, NextAppointmentDate))  >= 85 THEN  1
                 end as onMMD
from ODS.dbo.Intermediate_LastPatientEncounter