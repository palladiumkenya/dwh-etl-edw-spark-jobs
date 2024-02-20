Select
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey ,
    Summary.EMR,
    KHIS_TxCurr,
    DWH_TXCurr,
    EMR_TxCurr,
    Diff_EMR_DWH,
    DiffKHISDWH,
    DiffKHISEMR,
    Percent_variance_EMR_DWH as Proportion_variance_EMR_DWH,
    Percent_variance_KHIS_DWH as Proportion_variance_KHIS_DWH ,
    Percent_variance_KHIS_EMR as Proportion_variance_KHIS_EMR,
    dwapi.DwapiVersion
from Summary
left join facility on facility.MFLCode = Summary.MFLCode
left join Facilityinfo on Facilityinfo.MFL_Code=Summary.MFLCode
left join partner on partner.PartnerName = Facilityinfo.PartnerName
left join agency on agency.AgencyName = Facilityinfo.Agency
left join DWAPI on DWAPI.SiteCode=Summary.MFLCode;