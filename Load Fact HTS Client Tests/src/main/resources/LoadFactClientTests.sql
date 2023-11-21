select
    patient.PatientKey,
    facility.FacilityKey,
    partner.PartnerKey,
    agency.AgencyKey,
    age_group.AgeGroupKey,
    round((months_between(hts_encounter.TestDate, patient.DOB )/12),0) as AgeAtTesting,
    testing.DateKey as DateTestedKey,
    hts_encounter.EverTestedForHiv,
    hts_encounter.MonthsSinceLastTest,
    hts_encounter.ClientTestedAs,
    hts_encounter.EntryPoint,
    hts_encounter.TestStrategy,
    hts_encounter.TestResult1,
    hts_encounter.TestResult2,
    hts_encounter.FinalTestResult,
    hts_encounter.PatientGivenResult,
    hts_encounter.TestType,
    hts_encounter.TBScreening,
    hts_encounter.ClientSelfTested,
    hts_encounter.CoupleDiscordant,
    hts_encounter.Consent,
    hts_encounter.EncounterId,
    case when hts_encounter.FinalTestResult is not null then 1 else 0 end as Tested,
    case when hts_encounter.FinalTestResult = 'Positive' then 1 else 0 end as Positive,
    case when (hts_encounter.FinalTestResult = 'Positive' and client_linkage_data.ReportedCCCNumber is not null) then 1 else 0 end as Linked,
    case when client_linkage_data.ReportedCCCNumber is not null then 1 else 0 end ReportedCCCNumber,
    case
        when (hts_encounter.MonthsSinceLastTest < 3 and hts_encounter.MonthsSinceLastTest is not null) then '<3 Months'
        when (hts_encounter.MonthsSinceLastTest >= 3 and hts_encounter.MonthsSinceLastTest < 6) then '3-6 Months'
        when (hts_encounter.MonthsSinceLastTest >= 6 and hts_encounter.MonthsSinceLastTest < 9) then '6-9 Months'
        when (hts_encounter.MonthsSinceLastTest >= 9 and hts_encounter.MonthsSinceLastTest < 12) then '9-12 Months'
        when (hts_encounter.MonthsSinceLastTest >= 12 and hts_encounter.MonthsSinceLastTest < 18) then '12-18 Months'
        when (hts_encounter.MonthsSinceLastTest >= 18 and hts_encounter.MonthsSinceLastTest < 24) then '18-24 Months'
        when (hts_encounter.MonthsSinceLastTest >= 24 and hts_encounter.MonthsSinceLastTest < 36) then '24-36 Months'
        when (hts_encounter.MonthsSinceLastTest >= 36 and (hts_encounter.MonthsSinceLastTest < 48)) then '36-48 Months'
        when (hts_encounter.MonthsSinceLastTest >= 48 and hts_encounter.MonthsSinceLastTest is not null) then '>48Months'
        end as MonthsLastTest,
    case
        when (hts_encounter.EverTestedForHiv = 'Yes' and hts_encounter.MonthsSinceLastTest < 12) then 'Retest'
        else 'New' end as TestedBefore,
    hts_encounter.Setting,
    current_date ()  as LoadDate
from hts_encounter
left join DimPatient as patient on patient.SiteCode = hts_encounter.SiteCode and patient.PatientPKHash = hts_encounter.PatientPKHash
left join DimFacility as facility on facility.MFLCode = hts_encounter.SiteCode
left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = hts_encounter.SiteCode
left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
left join DimAgeGroup as age_group on age_group.Age = round((months_between(hts_encounter.TestDate, patient.DOB)/12), 0)
left join DimDate as testing on testing.Date = cast(hts_encounter.TestDate as date)
left join client_linkage_data on client_linkage_data.PatientPk = hts_encounter.PatientPK
    and client_linkage_data.SiteCode = hts_encounter.SiteCode
    and client_linkage_data.row_num = 1;