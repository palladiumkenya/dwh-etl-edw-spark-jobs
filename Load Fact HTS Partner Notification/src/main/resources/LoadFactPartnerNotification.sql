select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    PartnerPatientPk,
    KnowledgeOfHivStatus,
    PartnerPersonID,
    CCCNumber,
    IpvScreeningOutcome,
    ScreenedForIpv,
    PnsConsent,
    RelationsipToIndexClient,
    LinkedToCare,
    source_data.MaritalStatus As PartnerMaritalStatus,
    PnsApproach,
    FacilityLinkedTo,
    CurrentlyLivingWithIndexClient,
    DateElicited.Datekey as DateElicitedKey,
    LinkDateLinkedToCare.DateKey as DateLinkedToCareKey,
    current_date() as LoadDate
from source_data
    left join DimPatient as patient on patient.PatientPKHash = source_data.PatientPKHash
    and patient.SiteCode = source_data.SiteCode
    left join DimFacility as facility on facility.MFLCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimDate as DateElicited on DateElicited.Date = source_data.DateElicited
    left join DimDate as LinkDateLinkedToCare on LinkDateLinkedToCare.Date = source_data.LinkDateLinkedToCare
    left join DimAgeGroup as age_group on age_group.Age = source_data.AgeAtElicitation