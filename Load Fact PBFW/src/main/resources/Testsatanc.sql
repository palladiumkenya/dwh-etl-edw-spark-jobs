SELECT Row_number()
               OVER ( Partition BY Tests.Sitecode, Tests.Patientpk,tests.TestDate,tests.TestType ORDER BY EncounterId ASC ) AS NUM,
       Tests.Patientpkhash,
       Tests.Sitecode,
       Tests.Patientpk
FROM   Ods.Dbo.Hts_clienttests Tests
WHERE  Entrypoint IN ( 'PMTCT ANC', 'MCH' )