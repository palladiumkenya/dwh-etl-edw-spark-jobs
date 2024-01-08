select
    *
from (
         select
             distinct
             case
                 when value = 'Alzheimers Disease and other Dementias' then 'Alzheimer''s Disease and other Dementias'
                 else value
                 end as value,
             PatientPKHash,
             PatientPK,
             SiteCode,
             voided
         from ODS.dbo.CT_AllergiesChronicIllness as chronic
                  cross apply STRING_SPLIT(chronic.ChronicIllness, '|')
     ) as chronic
pivot(
         count(value)
         for value IN (
            "Alzheimers Disease and other Dementias",
            "Alzheimer's Disease and other Dementias",
            "Arthritis",
            "Asthma",
            "Cancer",
            "Cardiovascular diseases",
            "Chronic Hepatitis",
            "Chronic Kidney Disease",
            "Chronic Obstructive Pulmonary Disease(COPD)",
            "Chronic Renal Failure",
            "Cystic Fibrosis",
            "Deafness and Hearing Impairment",
            "Diabetes",
            "Endometriosis",
            "Epilepsy",
            "Glaucoma",
            "Heart Disease",
            "Hyperlipidaemia",
            "Hypertension",
            "Hypothyroidism",
            "Mental illness",
            "Multiple Sclerosis",
            "Obesity",
            "Osteoporosis",
            "Sickle Cell Anaemia",
            "Thyroid disease"
        )
     ) as pivot_table
where voided = 0