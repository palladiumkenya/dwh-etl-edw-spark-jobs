SELECT Row_number()
               OVER ( Partition BY Eac.Sitecode, Eac.Patientpk ORDER BY Eac.Visitdate ASC ) AS NUM,
       Patientpkhash,
       Sitecode,
       Patientpk,
       Visitdate
FROM   Ods.Dbo.Ct_enhancedadherencecounselling Eac