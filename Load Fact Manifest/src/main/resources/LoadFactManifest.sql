SELECT
     manifestId,
     timeId,
     facilityId,
     emrId,
     docketId,
     upload,
     partner.PartnerKey,
     agency.AgencyKey,
     started.DateKey as StartDateKey,
     ended.DateKey as EndDateKey,
    current_date() as LoadDate
FROM Fact_manifest as  manifest
     left join facility on facility.MFLCode=manifest.facilityId
     left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code=manifest.facilityId
     left join DimPartner as partner on partner.PartnerName=MFL_partner_agency_combination.SDP
     left join agency on Agency.AgencyName=MFL_partner_agency_combination.Agency
     left join DimDate as UploadDates on UploadDates.Date = manifest.timeId
     left join DimDate as started on started.Date = manifest.Start
     left join DimDate as ended on ended.Date = manifest.End;
