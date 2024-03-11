select
    lastencounter.PatientPK,
    lastencounter.SiteCode,
    lastencounter.LastEncounterDate as lastencounterDate,
    lastencounter.NextAppointmentDate as NextAppointment
from ODS.dbo.Intermediate_LastPatientEncounter as lastencounter
