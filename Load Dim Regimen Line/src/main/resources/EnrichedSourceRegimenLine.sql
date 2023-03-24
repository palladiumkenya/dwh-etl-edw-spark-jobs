SELECT
    DISTINCT RegimenLine,
             CASE
                 WHEN RegimenLine IN ('1st line','Adult ART FirstLine','Adult first line','Adult FirstLine','Child first line','First line','First line substitute','Paeds ART FirstLine','1st Alternative', 'Child FirstLine') THEN 'First Line'
                 WHEN RegimenLine IN ('Adult ART SecondLine','Adult second line','Adult SecondLine','Child SecondLine','Paeds ART Secondline','Second line','Second line substitute', '2nd Line') THEN 'Second Line'
                 WHEN RegimenLine IN ('Adult ART ThirdLine ','Adult ThirdLine','Child ThirdLine','Third line','Adult ART ThirdLine') THEN 'Third Line'
                 WHEN RegimenLine IN ('unknown') THEN 'Unknown'
                 WHEN RegimenLine IN ('PMTCT Maternal Regimens', 'PMTCT Regimens') THEN 'PMTCT'
                 WHEN RegimenLine IN ('Other') THEN 'Other'
                 ELSE 'Unknown'
                 END AS RegimenLineCategory
FROM source_regimen_line
WHERE RegimenLine IS NOT NULL AND LTRIM(RTRIM(RegimenLine)) != '' AND RegimenLine != 'null' AND length(RegimenLine) > 1