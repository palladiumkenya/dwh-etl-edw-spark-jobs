select
    distinct PatientPK,
             SiteCode,
             _6MonthVL,
             _6MonthVLDate,
             _12MonthVL,
             _12MonthVLDate,
             _18MonthVL,
             _18MonthVLDate,
             _24MonthVL,
             _24MonthVLDate,
             _6MonthVLSup,
             _12MonthVLSup,
             _18MonthVLSup,
             _24MonthVLSup
from dbo.Intermediate_ViralLoadsIntervals