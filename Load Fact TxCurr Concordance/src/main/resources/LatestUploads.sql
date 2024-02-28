Select
    SiteCode,
    cast( DateRecieved as date) As DateReceived,
    Emr,
    Name,
    Start,
    PatientCount
from Uploads
where Num=1