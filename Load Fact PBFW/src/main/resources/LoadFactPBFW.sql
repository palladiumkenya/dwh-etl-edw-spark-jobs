SELECT
    Patient.Patientkey,
    Facility.Facilitykey,
    Partner.Partnerkey,
    Agency.Agencykey,
    Age_group.Agegroupkey,
    Ancdate1,
    Ancdate2,
    Ancdate3,
    Ancdate4,
    Testedatanc,
    Testedatlandd,
    Testedatpnc,
    Positiveadolescent,
    Newpositives,
    Knownpositive,
    Recieivedart,
    Ancdate1.Datekey AS ANCDate1Key,
    Ancdate2.Datekey AS ANCDate2Key,
    Ancdate3.Datekey AS ANCDate3Key,
    Ancdate4.Datekey AS ANCDate4Key,
    Receivedeac1,
    Receivedeac2,
    Receivedeac3,
    Pbfwreglineswitch
FROM Summary
LEFT JOIN Facility ON Facility.Mflcode = Summary.Sitecode
LEFT JOIN Mfl_partner_agency_combination ON Mfl_partner_agency_combination.Mfl_code = Summary.Sitecode
LEFT JOIN Partner ON Partner.Partnername = Mfl_partner_agency_combination.Sdp
LEFT JOIN Agency ON Agency.Agencyname = Mfl_partner_agency_combination.Agency
LEFT JOIN Patient ON Patient.Patientpkhash = Summary.Patientpkhash AND Patient.Sitecode = Summary.Sitecode
LEFT JOIN Dimdate AS Ancdate1 ON Ancdate1.Date = Cast(Summary.Ancdate1 AS Date)
LEFT JOIN Dimdate AS Ancdate2 ON Ancdate2.Date = Cast(Summary.Ancdate2 AS Date)
LEFT JOIN Dimdate AS Ancdate3 ON Ancdate3.Date = Cast(Summary.Ancdate3 AS Date)
LEFT JOIN Dimdate AS Ancdate4 ON Ancdate4.Date = Cast(Summary.Ancdate4 AS Date)
LEFT JOIN Age_group ON Age_group.Age = Datediff(YEAR, Summary.Dob, current_date())
WHERE  Patient.Voided = 0;