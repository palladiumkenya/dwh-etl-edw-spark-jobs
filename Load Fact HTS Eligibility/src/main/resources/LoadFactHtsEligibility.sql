select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    VisitDate.DateKey As VisitDateKey,
    DateTestedSelf.DateKey as DateTestedSelfKey,
    source_data.Department,
    source_data.IsHealthWorker,
    source_data.RelationshipWithContact,
    source_data.TestedHIVBefore,
    source_data.WhoPErformedTest,
    source_data.ResultOfHIV,
    source_data.StartedOnART,
    source_data.CCCNumberHash,
    source_data.EverHadSex,
    source_data.SexuallyActive,
    source_data.NewPartner,
    source_data.PartnerHIVStatus,
    source_data.CoupleDiscordant,
    source_data.MultiplePartners,
    source_data.NumberOfPartners,
    source_data.AlcoholSex,
    source_data.MoneySex,
    source_data.CondomBurst,
    source_data.UnknownStatusPartner,
    source_data.KnownStatusPartner,
    source_data.Pregnant,
    source_data.BreastfeedingMother,
    source_data.ExperiencedGBV,
    source_data.ContactWithTBCase,
    source_data.Lethargy,
    source_data.EverOnPrep,
    source_data.CurrentlyOnPep,
    source_data.EverHadSTI,
    source_data.CurrentlyHasSTI,
    source_data.EverHadTB,
    source_data.SharedNeedle,
    source_data.NeedleStickInjuries,
    source_data.TraditionalProcedures,
    source_data.ChildReasonsForIneligibility,
    source_data.EligibleForTest,
    source_data.ReasonsforIneligibility,
    source_data.specificReasonForIneligibility,
    source_data.Cough,
    cast (DateTestedProvider as date) As DateTestedProvider,
    source_data.Fever,
    source_data.MothersStatus,
    source_data.NightSweats,
    source_data.ReferredForTesting,
    source_data.ResultOfHIVSelf,
    source_data.ScreenedTB,
    source_data.TBStatus,
    source_data.WeightLoss,
    source_data.AssessmentOutcome,
    source_data.ForcedSex,
    source_data.ReceivedServices,
    source_data.TypeGBV,
    source_data.HIVRiskCategory,
    source_data.HtsRiskScore,
    source_data.ReasonRefferredForTesting,
    current_date() as LoadDate
from source_data
    left join DimPatient as patient on patient.PatientPKHash = source_data.PatientPKHash
    and patient.SiteCode = source_data.SiteCode
    left join DimFacility as facility on facility.MFLCode = source_data.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = source_data.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimDate as VisitDate on VisitDate.Date = source_data.VisitDate
    left join DimDate as DateTestedSelf on DateTestedSelf.Date = source_data.DateTestedSelf
    left join DimDate as DateTestedProvider on DateTestedProvider.Date = source_data.DateTestedProvider