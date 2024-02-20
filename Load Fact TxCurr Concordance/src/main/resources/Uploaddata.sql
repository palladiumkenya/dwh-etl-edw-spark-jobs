Select
    Combined.MFL_Code ,
    Combined.Facility_Name,
    PartnerName,
    AgencyName,
    DateReceived,
    ExpectedPatients,
    Received.Received as CompletenessStatus
from Combined
left join Received on Combined.MFL_Code=Received.MFL_Code
where Received<ExpectedPatients