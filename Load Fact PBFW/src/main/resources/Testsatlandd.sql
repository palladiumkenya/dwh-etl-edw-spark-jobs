SELECT Row_number()
               OVER ( Partition BY Tests.Sitecode, Tests.Patientpk,tests.TestDate,tests.TestType ORDER BY EncounterId ASC ) AS NUM,
       Tests.Patientpkhash,
       Tests.Patientpk,
       Tests.Sitecode
FROM   Ods.Dbo.Hts_clienttests Tests
WHERE  Entrypoint IN ( 'Maternity', 'PMTCT MAT' )