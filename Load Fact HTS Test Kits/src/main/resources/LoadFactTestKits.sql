select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    kit_name1.TestKitNameKey as TestKitName1Key,
    TestKitLotNumber1,
    TestResult1,
    kit_name2.TestKitNameKey as TestKitName2Key,
    TestKitLotNumber2,
    TestResult2,
    current_date() as LoadDate
from source_data
    left join DimPatient as patient on patient.PatientPKHash =  source_data.PatientPKHash
    and patient.SiteCode = source_data.SiteCode
    left join DimFacility as facility on facility.MFLCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimTestKitName as kit_name1 on kit_name1.TestKitName = source_data.TestKitName1
    left join DimTestKitName as kit_name2 on kit_name2.TestKitName = source_data.TestKitName2