SELECT Row_number()
               OVER ( Partition BY Pharm.Sitecode, Pharm.Patientpk ORDER BY Pharm.Dispensedate DESC ) AS NUM,
       Pharm.Patientpk,
       Pharm.Sitecode,
       CASE
           WHEN Regimenchangedswitched IS NOT NULL THEN 1
           ELSE 0
           END                                    AS PBFWRegLineSwitch
FROM   Ods.Dbo.Ct_patientpharmacy Pharm
WHERE  Regimenchangedswitched IS NOT NULL