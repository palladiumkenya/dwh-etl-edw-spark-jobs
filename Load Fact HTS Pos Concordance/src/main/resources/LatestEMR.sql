Select
    Emr.facilityCode
    ,Emr.facilityName
    ,Emr.value As EMRValue
    ,Emr.statusDate
    ,Emr.indicatorDate
from EMR
where Num=1 and Emr.facilityCode is not null