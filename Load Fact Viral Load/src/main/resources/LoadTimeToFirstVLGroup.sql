select
    PatientPK,
    SiteCode,
    case
        when TimetoFirstVL < 0 then 'Before ARTStart'
        when TimetoFirstVL between 0 and 90 then '3 Months'
        when TimetoFirstVL between 91 and 180 then '6 Months'
        when TimetoFirstVL between 181 and 365 then '12 Months'
        when TimetoFirstVL > 365 then '> 12 Months'
    end as TimeToFirstVLGrp
from time_to_first_vl