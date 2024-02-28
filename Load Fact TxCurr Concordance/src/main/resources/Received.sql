Select distinct
    Fac.MFL_Code,
    fac.Facility_Name,
    Count (*) As Received
FROM dbo.CT_Patient(NoLock) Patient
INNER JOIN dbo.all_EMRSites(NoLock) Fac ON Patient.SiteCode = Fac.MFL_Code AND Patient.Voided=0 and Patient.SiteCode>0
group by
    Fac.MFL_Code,
    fac.Facility_Name