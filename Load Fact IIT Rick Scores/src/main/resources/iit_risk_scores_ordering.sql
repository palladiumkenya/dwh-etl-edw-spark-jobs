select
    scores.PatientPK,
    scores.PatientPKHash,
    scores.SiteCode,
    cast(scores.RiskEvaluationDate as date) as RiskEvaluationDate,
    scores.RiskScore,
    case
        when cast(scores.RiskScore as decimal(9,2)) >= 0.0 and scores.RiskScore <= 0.04587387 then 'Low'
        when cast(scores.RiskScore as decimal(9,2)) >= 0.04587388 and scores.RiskScore <= 0.1458252 then 'Medium'
        when cast(scores.RiskScore as decimal(9, 2)) >= 0.1458253  and scores.RiskScore <= 1.0 then 'High'
        end as RiskCategory,
    row_number() over (partition by scores.PatientPK, scores.SiteCode order by scores.RiskEvaluationDate desc) as rank
from ODS.dbo.CT_IITRiskScores as scores
    left join ODS.dbo.CT_Patient as patient on patient.PatientPK = scores.PatientPK and patient.SiteCode = scores.PatientPK