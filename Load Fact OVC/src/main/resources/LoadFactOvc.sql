select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    ovc_enrollment.DateKey as OVCEnrollmentDateKey,
    relationship_client.RelationshipWithPatientKey,
    source_ovc.EnrolledinCPIMS,
    CPIMSUniqueIdentifier,
    PartnerOfferingOVCServices,
    OVCExitReason,
    exit_date.DateKey as OVCExitDateKey,
    current_date() as LoadDate
from source_ovc
    left join DimPatient as patient on patient.PatientPK = source_ovc.PatientPK
    and patient.SiteCode = source_ovc.SiteCode
    left join DimFacility as facility on facility.MFLCode = source_ovc.SiteCode
    left join DimDate as ovc_enrollment on ovc_enrollment.Date = source_ovc.OVCEnrollmentDate
    left join DimDate as exit_date on exit_date.Date = source_ovc.ExitDate
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_ovc.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = source_ovc.AgeLastVisit
    left join DimRelationshipWithPatient as relationship_client on relationship_client.RelationshipWithPatient = source_ovc.RelationshipToClient