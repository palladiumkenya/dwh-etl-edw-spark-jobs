select
      patient.PatientKey,
      facility.FacilityKey,
      partner.PartnerKey,
      agency.AgencyKey,
      DNAPCR1.DateKey as DNAPCR1DateKey ,
      DNAPCR2.DateKey as DNAPCR2DateKey,
      antiboday_date.DateKey as FinalyAntibodyDateKey,
      age_group.AgeGroupKey,
      case
          when tested_at_6wks_first_contact.age_in_weeks_at_DNAPCR1Date is not null then 1
          else 0
          end as TestedAt6wksOrFirstContact,
      case
          when tested_at_6_months.age_in_months_at_DNAPCR2Date is not null then 1
          else 0
          end as TestedAt6months,
      case
          when tested_at_12_months.age_in_months_at_DNAPCR2Date is not null then 1
          else 0
          end as TestedAt12months,
      case
          when initial_PCR_less_than_8wks.age_in_weeks_at_DNAPCR1Date is not null then 1
          else 0
          end as InitialPCRLessThan8wks,
      case
          when initial_PCR_btwn_8wks_12mnths.age_in_weeks_at_DNAPCR1Date is not null then 1
          else 0
          end as InitialPCRBtwn8wks_12mnths,
      case
          when final_antibody_data.FinalyAntibody is not null then 1
          else 0
          end as HasFinalAntibody,
      final_antibody_data.FinalyAntibodyDate,
      case
          when feeding_data.age_in_months_as_last_cwc_visit = 6 and feeding_data.InfantFeeding in ('Exclusive Breastfeeding(EBF)', 'EBF') then 1
          else 0
          end as EBF6mnths,
      case
          when feeding_data.age_in_months_as_last_cwc_visit = 6 and feeding_data.InfantFeeding in ('Exclusive Replacement(ERF)', 'ERF') then 1
          else 0
          end as ERF6mnths,
      case
          when feeding_data.age_in_months_as_last_cwc_visit = 12 and feeding_data.InfantFeeding in ('BF') then 1
          else 0
          end as BF12mnths,
      case
          when feeding_data.age_in_months_as_last_cwc_visit = 18 and feeding_data.InfantFeeding in ('BF') then 1
          else 0
          end as BF18mnths,
      case
          when infected_and_ART.StartARTDate is not null then 1
          else 0
          end as InfectedOnART,
      case
          when positive_heis.HEIHIVStatus is not null then 1
          else 0
          end as InfectedAt24mnths,
      positive_heis.HEIExitCritearia as HEIExitCriteria,
      positive_heis.HEIHIVStatus,
      case
          when prophylaxis_data.PatientPk is not null then 1
          else 0
          end as OnProhylaxis,
      case
          when unknown_status_24_months.PatientPk is not null then 1
          else 0
          end as  UnknownOutocomeAt24months

from MNCH_HEIs as heis
         left join tested_at_6wks_first_contact on tested_at_6wks_first_contact.PatientPk = heis.PatientPk
    and tested_at_6wks_first_contact.SiteCode = heis.SiteCode
         left join tested_at_6_months on tested_at_6_months.PatientPk = heis.PatientPk
    and tested_at_6_months.SiteCode = heis.SiteCode
         left join tested_at_12_months on tested_at_12_months.PatientPk = heis.PatientPk
    and tested_at_12_months.SiteCode = heis.SiteCode
         left join initial_PCR_less_than_8wks on initial_PCR_less_than_8wks.PatientPk = heis.PatientPk
    and initial_PCR_less_than_8wks.SiteCode = heis.SiteCode
         left join initial_PCR_btwn_8wks_12mnths on initial_PCR_btwn_8wks_12mnths.PatientPK = heis.PatientPk
    and initial_PCR_btwn_8wks_12mnths.SiteCode = heis.SiteCode
         left join final_antibody_data on final_antibody_data.PatientPk = heis.PatientPk
    and final_antibody_data.SiteCode = heis.SiteCode
         left join feeding_data on feeding_data.PatientPk = heis.PatientPk
    and feeding_data.SiteCode = heis.SiteCode
         left join infected_and_ART on infected_and_ART.PatientPk = heis.PatientPk
    and infected_and_ART.SiteCode = heis.SiteCode
         left join positive_heis on positive_heis.PatientPk = heis.PatientPk
    and positive_heis.SiteCode = heis.SiteCode
         left join prophylaxis_data on prophylaxis_data.PatientPk = heis.PatientPk
    and prophylaxis_data.SiteCode = heis.SiteCode
         left join unknown_status_24_months on unknown_status_24_months.PatientPk = heis.PatientPk
    and unknown_status_24_months.SiteCode = heis.SiteCode
         left join DimFacility as facility on facility.MFLCode = heis.SiteCode
         left join latest_cwc_visit on latest_cwc_visit.PatientPk = heis.PatientPk
    and latest_cwc_visit.SiteCode =heis.SiteCode
         left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = heis.SiteCode
         left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
         left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
         left join DimPatient as patient on patient.PatientPKHash = heis.PatientPKHash
    and patient.SiteCode = heis.SiteCode
         left join DimDate as DNAPCR1 on DNAPCR1.Date = cast(heis.DNAPCR1Date as date)
         left join DimDate as DNAPCR2 on DNAPCR2.Date = cast(heis.DNAPCR2Date as date)
         left join DimDate as antiboday_date on antiboday_date.Date = cast(final_antibody_data.FinalyAntibodyDate as date)
         left join DimAgeGroup as age_group on age_group.Age =  datediff(year, patient.DOB, coalesce(latest_cwc_visit.VisitDate, current_date()))
WHERE patient.voided =0