SELECT Patient.PatientPKHash,
       Patient.Sitecode,
       Dob,
       Gender,
       coalesce(GreenCardAncDate1, Ancdate1.ANCDate1) AS ANCDate1,
       ANCDate2.Ancdate2,
       ANCDate3.Ancdate3,
       ANCDate4.Ancdate4,
       CASE
           WHEN Testedatlandd.Patientpkhash IS NOT NULL THEN 1
           ELSE 0
           END       AS TestedatLandD,
       CASE
           WHEN Testedatanc.Patientpkhash IS NOT NULL THEN 1
           ELSE 0
           END       AS TestedatANC,
       CASE
           WHEN Testedatpnc.Patientpkhash IS NOT NULL THEN 1
           ELSE 0
           END       AS TestedatPNC,
       CASE
           WHEN Datediff(Year, Dob,  last_day(add_months(current_date, -1))) BETWEEN 10 AND 19 THEN
               1
           ELSE 0
           END       AS PositiveAdolescent,
       CASE
           WHEN PBFWCategory = 'New Positive'  THEN 1
           ELSE 0
           END       AS NewPositives,
       CASE
           WHEN PBFWCategory = 'Known Positive'  THEN 1
           ELSE 0
           END      AS KnownPositive,
       CASE
           WHEN Startartdate IS NOT NULL THEN 1
           ELSE 0
           END       AS RecieivedART,
       CASE
           WHEN Receivedeac1.Patientpk IS NOT NULL THEN 1
           ELSE 0
           END       AS ReceivedEAC1,
       CASE
           WHEN Receivedeac2.Patientpk IS NOT NULL THEN 1
           ELSE 0
           END       AS ReceivedEAC2,
       CASE
           WHEN Receivedeac3.Patientpk IS NOT NULL THEN 1
           ELSE 0
           END       AS ReceivedEAC3,
       Pbfwreglineswitch
FROM   Pbfw_patient AS Patient
           LEFT JOIN Ancdate1
                     ON Patient.Patientpk = Ancdate1.Patientpk
                         AND Patient.Sitecode = Ancdate1.Sitecode
           LEFT JOIN Ancdate2
                     ON Patient.Patientpk = Ancdate2.Patientpk
                         AND Patient.Sitecode = Ancdate2.Sitecode
           LEFT JOIN Ancdate3
                     ON Patient.Patientpk = Ancdate3.Patientpk
                         AND Patient.Sitecode = Ancdate3.Sitecode
           LEFT JOIN Ancdate4
                     ON Patient.Patientpk = Ancdate4.Patientpk
                         AND Patient.Sitecode = Ancdate4.Sitecode
           LEFT JOIN Testedatanc
                     ON Patient.Patientpk = Testedatanc.Patientpk
                         AND Patient.Sitecode = Testedatanc.Sitecode
           LEFT JOIN Testedatlandd
                     ON Patient.Patientpk = Testedatlandd.Patientpk
                         AND Patient.Sitecode = Testedatlandd.Sitecode
           LEFT JOIN Testedatpnc
                     ON Patient.Patientpk = Testedatpnc.Patientpk
                         AND Patient.Sitecode = Testedatpnc.Sitecode
           LEFT JOIN Receivedeac1
                     ON Patient.Patientpk = Receivedeac1.Patientpk
                         AND Patient.Sitecode = Receivedeac1.Sitecode
           LEFT JOIN Receivedeac2
                     ON Patient.Patientpk = Receivedeac2.Patientpk
                         AND Patient.Sitecode = Receivedeac2.Sitecode
           LEFT JOIN Receivedeac3
                     ON Patient.Patientpk = Receivedeac3.Patientpk
                         AND Patient.Sitecode = Receivedeac3.Sitecode
           LEFT JOIN Pbfwreglineswitch
                     ON Patient.Patientpk = Pbfwreglineswitch.Patientpk
                         AND Patient.Sitecode = Pbfwreglineswitch.Sitecode