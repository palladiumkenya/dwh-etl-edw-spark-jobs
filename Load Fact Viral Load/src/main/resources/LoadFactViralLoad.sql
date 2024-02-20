select
      patient.PatientKey,
      facility.FacilityKey,
      partner.PartnerKey,
      agency.AgencyKey,
      age_group.AgeGroupKey,
      validVL_date.DateKey as ValidVLDateKey,
      _6_monthVL_date.DateKey as 6MonthVLDateKey,
      _12_monthVL_date.DateKey as 12MonthVLDateKey,
      _18_monthVL_date.DateKey as 18MonthVLDateKey,
      _24_monthVL_date.Datekey as 24MonthVLDateKey,
      first_VL_date.DateKey as FirstVLDateKey,
      last_VL_date.DateKey as LastVLDateKey,
      lastest_VL_date1.DateKey as LatestVLDate1Key,
      lastest_VL_date2.DateKey as LatestVLDate2Key,
      lastest_VL_date3.DateKey as LatestVLDate3Key,
      combined_viral_load_dataset.LatestVL1,
      combined_viral_load_dataset.LatestVL2,
      combined_viral_load_dataset.LatestVL3,
      combined_viral_load_dataset.EligibleVL,
      combined_viral_load_dataset.ValidVLResult,
      Case
          when combined_viral_load_dataset.HasValidVL = 1 and IsPBFW = 1 and PBFW_ValidVL = 0 then 0
          else combined_viral_load_dataset.HasValidVL
          end as HasValidVL,
      combined_viral_load_dataset.IsPBFW,
      combined_viral_load_dataset.PBFW_ValidVL,
      combined_viral_load_dataset.PBFW_ValidVLResultCategory,
      combined_viral_load_dataset.PBFW_ValidVLSup,
      pbfw_validVL_date.DateKey as PBFW_ValidDateKey,
      combined_viral_load_dataset.ValidVLResultCategory1,
      combined_viral_load_dataset.ValidVLResultCategory2,
      combined_viral_load_dataset.ValidVLSup,
      combined_viral_load_dataset._6MonthVL,
      combined_viral_load_dataset._12MonthVL,
      combined_viral_load_dataset._18MonthVL,
      combined_viral_load_dataset._24MonthVL,
      combined_viral_load_dataset._6MonthVLSup as 6MonthVLSup,
      combined_viral_load_dataset._12MonthVLSup as 12MonthVLSup,
      combined_viral_load_dataset._18MonthVLSup as 18MonthVLSup,
      combined_viral_load_dataset._24MonthVLSup as 24MonthVLSup,
      combined_viral_load_dataset.FirstVL,
      combined_viral_load_dataset.LastVL,
      combined_viral_load_dataset.TimetoFirstVL,
      combined_viral_load_dataset.TimeToFirstVLGrp,
      combined_viral_load_dataset.HighViremia,
      combined_viral_load_dataset.LowViremia,
      combined_viral_load_dataset.RepeatVls,
      combined_viral_load_dataset.RepeatSuppressed,
      combined_viral_load_dataset.RepeatUnSuppressed
from combined_viral_load_dataset
    left join DimPatient as patient on patient.PatientPKHash = combined_viral_load_dataset.PatientPKHash and patient.SiteCode = combined_viral_load_dataset.SiteCode
    left join DimFacility as facility on facility.MFLCode = combined_viral_load_dataset.SiteCode
    left join MFL_partner_agency_combination on MFL_partner_agency_combination.MFL_Code = combined_viral_load_dataset.SiteCode
    left join DimPartner as partner on partner.PartnerName = MFL_partner_agency_combination.SDP
    left join DimAgency as agency on agency.AgencyName = MFL_partner_agency_combination.Agency
    left join DimAgeGroup as age_group on age_group.Age = combined_viral_load_dataset.AgeLastVisit
    left join DimDate as validVL_date on validVL_date.Date = combined_viral_load_dataset.ValidVLDate
    left join DimDate as _6_monthVL_date on _6_monthVL_date.Date = combined_viral_load_dataset._6MonthVLDate
    left join DimDate as _12_monthVL_date on _12_monthVL_date.Date = combined_viral_load_dataset._12MonthVLDate
    left join DimDate as _18_monthVL_date on _18_monthVL_date.Date = combined_viral_load_dataset._18MonthVLDate
    left join DimDate as _24_monthVL_date on _24_monthVL_date.Date = combined_viral_load_dataset._24MonthVLDate
    left join DimDate as first_VL_date on first_VL_date.Date = combined_viral_load_dataset.FirstVLDate
    left join DimDate as last_VL_date on last_VL_date.Date = combined_viral_load_dataset.LastVLDate
    left join DimDate as lastest_VL_date1 on lastest_VL_date1.Date = combined_viral_load_dataset.LatestVLDate1
    left join DimDate as lastest_VL_date2 on lastest_VL_date2.Date = combined_viral_load_dataset.LatestVLDate2
    left join DimDate as lastest_VL_date3 on lastest_VL_date3.Date = combined_viral_load_dataset.LatestVLDate3
    left join DimDate as pbfw_validVL_date on pbfw_validVL_date.Date = combined_viral_load_dataset.PBFW_ValidVLDate
WHERE patient.voided =0;