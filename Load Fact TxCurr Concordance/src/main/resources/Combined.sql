Select distinct
    MFL_Code ,
    Facility_Name,
    PartnerName,
    Facilityinfo.Agency AgencyName,
    LatestUploads.DateReceived,
    LatestUploads.PatientCount As ExpectedPatients
from Facilityinfo
left join LatestUploads on Facilityinfo.MFL_Code =LatestUploads.SiteCode