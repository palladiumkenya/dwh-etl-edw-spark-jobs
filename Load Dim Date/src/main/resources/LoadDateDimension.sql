select cast(date_format(Date, 'yyyyMMdd') as int) as DateKey,
    Date,
    year(Date) as Year,
    month(Date) as Month,
    day(Date) as Day,
    quarter(Date) as CalendarQuarter,
    case
    when month(Date) between 10 and 12 then 1
    when month(Date) between 1 and 3 then 2
    when month(Date) between 4 and 6 then 3
    when month(Date) between 7 and 9 then 4
end as CDCFinancialQuarter,
    current_date() AS LoadDate
from
  dates
-- order by
--   Date