select
    visits_ordering.PatientPKHash,
    visits_ordering.PatientPK,
    visits_ordering.SiteCode,
    datediff(year, patient.DOB, coalesce(visits_ordering.VisitDate, current_date() )) As  AgeLastVisit
from visits_ordering
inner join CT_Patient as patient on patient.PatientPKHash = visits_ordering.PatientPKHash
    and patient.SiteCode = visits_ordering.SiteCode
    and patient.voided = 0
where rank = 1