Select
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey ,
    Summary.EMR,
    KHIS_HTSPos,
    DWH_HTSPos,
    EMR_HTSPos,
    Diff_EMR_DWH,
    DiffKHISDWH,
    DiffKHISEMR,
    Percent_variance_EMR_DWH as Proportion_variance_EMR_DWH,
    Percent_variance_KHIS_DWH as Proportion_variance_KHIS_DWH,
    Percent_variance_KHIS_EMR as Proportion_variance_KHIS_EMR,
    date_sub(date_trunc('MONTH', current_date()), 1) as Reporting_Month,
    Cast(current_date() as date) as LoadDate
from Summary
left join DimFacility as facility on facility.MFLCode = Summary.MFLCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = Summary.MFLCode
left join DimPartner as partner on partner.PartnerName = Summary.SDP
left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
ORDER BY Percent_variance_EMR_DWH DESC