SELECT DISTINCT DifferentiatedCare as DifferentiatedCare
FROM ODS.dbo.CT_PatientVisits
where DifferentiatedCare <> 'NULL' and DifferentiatedCare <>''