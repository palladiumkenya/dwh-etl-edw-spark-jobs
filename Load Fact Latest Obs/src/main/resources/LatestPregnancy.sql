select
    distinct visits.PatientPK,
             visits.PatientID,
             visits.SiteCode,
             visits.Pregnant
from ODS.dbo.CT_PatientVisits as visits
         inner join ODS.dbo.Intermediate_LastVisitDate as last_visit on visits.SiteCode = last_visit.SiteCode
    and visits.PatientPK = last_visit.PatientPK
    and visits.VisitDate = last_visit.LastVisitDate
    and Pregnant is not null