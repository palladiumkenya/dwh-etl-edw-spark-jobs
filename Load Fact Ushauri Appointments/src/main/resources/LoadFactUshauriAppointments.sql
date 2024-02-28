SELECT
    Facility.Facilitykey,
    Partner.Partnerkey,
    Patient.Patientkey,
    Agency.Agencykey,
    Age_group.Agegroupkey,
    Appointment.Datekey           AS AppointmentDateKey,
    Appointmenttype,
    Appointmentstatus,
    Entrypoint,
    Visittype,
    Attended.Datekey               AS DateAttendedDateKey,
    Consentforsms,
    Smslanguage,
    Smstargetgroup,
    Smspreferredsendtime,
    Fourweeksmssent,
    Fourweeksdate.Datekey         AS FourWeekSMSSendDateKey,
    Fourweeksmsdeliverystatus,
    Fourweeksmsdeliveryfailurereason,
    Threeweeksmssent,
    Threeweeksdate.Datekey        AS ThreeWeekSMSSendDateKey,
    Threeweeksmsdeliverystatus,
    Threeweeksmsdeliveryfailurereason,
    Twoweeksmssent,
    Twoweeksdate.Datekey          AS TwoWeekSMSSendDateKey,
    Twoweeksmsdeliverystatus,
    Twoweeksmsdeliveryfailurereason,
    Oneweeksmssent,
    Oneweeksdate.Datekey          AS OneWeekSMSSendDateKey,
    Oneweeksmsdeliverystatus,
    Oneweeksmsdeliveryfailurereason,
    Onedaysmssent,
    Onedaydate.Datekey           AS OneDaySMSSendDateKey,
    Onedaysmsdeliverystatus,
    Onedaysmsdeliveryfailurereason,
    Missedappointmentsmssent,
    Missedappointmentdate.Datekey  AS  MissedAppointmentSMSSendDateKey,
    Missedappointmentsmsdeliverystatus,
    Missedappointmentsmsdeliveryfailurereason,
    Tracingcalls,
    Tracingsms,
    Tracinghomevisits,
    Tracingoutcome,
    Tracingdate.Datekey          AS TracingOutcomeDateKey,
    Datereturnedtocare.Datekey    AS DateReturnedToCareDateKey,
    Daysdefaulted,
    Nupihash
FROM Apt
LEFT JOIN Dimfacility AS Facility
         ON Facility.Mflcode = Apt.Sitecode
LEFT JOIN MFL_partner_agency_combination
         ON MFL_partner_agency_combination.Mfl_code = Apt.Sitecode
LEFT JOIN Dimpartner AS Partner
         ON Partner.Partnername = MFL_partner_agency_combination.Sdp
LEFT JOIN Dimpatient AS Patient
         ON Patient.Patientpkhash = Apt.Patientpkhash AND Patient.Sitecode = Apt.Sitecode
LEFT JOIN Dimagency AS Agency
         ON Agency.Agencyname = MFL_partner_agency_combination.Agency
LEFT JOIN Dimagegroup AS Age_group
         ON Age_group.Agegroupkey = DATEDIFF(YEAR, CAST(Apt.Dob As DATE), CAST(Appointmentdate As DATE))
LEFT JOIN DimDate AS As_of
         ON As_of.Date = CAST(Apt.Appointmentdate As DATE)
LEFT JOIN DimDate AS Appointment
         ON Appointment.Date = CAST(Apt.Appointmentdate As DATE)
LEFT JOIN DimDate AS Attended
         ON Attended.Date = CAST(Apt.Dateattended As DATE)
LEFT JOIN DimDate AS Fourweeksdate
         ON Fourweeksdate.Date = CAST(Apt.Fourweeksmssenddate As DATE)
LEFT JOIN DimDate AS Threeweeksdate
         ON Threeweeksdate.Date = CAST(Apt.Threeweeksmssenddate As DATE)
LEFT JOIN DimDate AS Twoweeksdate
         ON Twoweeksdate.Date = CAST(Apt.Twoweeksmssenddate As DATE)
LEFT JOIN DimDate AS Oneweeksdate
         ON Oneweeksdate.Date = CAST(Apt.Oneweeksmssenddate As DATE)
LEFT JOIN DimDate AS Onedaydate
         ON Onedaydate.Date = CAST(Apt.Onedaysmssenddate As DATE)
LEFT JOIN DimDate AS Missedappointmentdate
         ON Missedappointmentdate.Date = CAST(Apt.Missedappointmentsmssenddate As DATE)
LEFT JOIN DimDate AS Tracingdate
         ON Tracingdate.Date = CAST(Apt.Tracingoutcomedate As DATE )
LEFT JOIN DimDate AS Datereturnedtocare
         ON Datereturnedtocare.Date = CAST(Apt.Datereturnedtocare As DATE)