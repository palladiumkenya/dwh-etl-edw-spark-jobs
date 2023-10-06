Select
    Uploaddates.DateKey as UploadsDateKey,
    facility.FacilityKey ,
    partner.PartnerKey,
    agency.AgencyKey,
    started.DateKey as StartDateKey,
    ended.DateKey as EndDateKey,
    current_date() as LoadDate
from Uploads
         left join facility on facility.MFLCode=Uploads.Sitecode
         left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=Uploads.SiteCode
         left join partner on partner.PartnerName=MFL_partner_agency_combination.SDP
         left join agency on Agency.AgencyName=MFL_partner_agency_combination.Agency
         left join DimDate as UploadDates on UploadDates.Date = Uploads.Dateuploaded
         left join DimDate as started on started.Date = Uploads.Start
         left join DimDate as ended on ended.Date = Uploads.End