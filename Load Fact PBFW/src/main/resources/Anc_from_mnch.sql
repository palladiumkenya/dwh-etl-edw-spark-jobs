SELECT
    Row_number()
               OVER ( Partition BY Patientpk, Sitecode ORDER BY Visitdate ASC ) AS NUM,
    Patientpk,
    Patientpkhash,
    Sitecode,
    Visitdate
FROM   Ods.Dbo.Mnch_ancvisits
WHERE  Hivstatusbeforeanc = 'KP'
   OR Hivtestfinalresult = 'Positive'