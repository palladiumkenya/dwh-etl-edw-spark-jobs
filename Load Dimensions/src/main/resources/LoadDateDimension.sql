select cast(date_format(calendarDate, 'yyyyMMdd') as int) as DateKey,
       calendarDate as Date,
    year(calendarDate) as Year,
    month(calendarDate) as Month,
    day(calendarDate) as Day,
    quarter(calendarDate) as CalendarQuarter,
    case
		when month(calendarDate) between 10 and 12 then 1
		when month(calendarDate) between 1 and 3 then 2
		when month(calendarDate) between 4 and 6 then 3
		when month(calendarDate) between 7 and 9 then 4
    end as CDCFinancialQuarter,
    current_date() AS LoadDate
from
  dates
order by
  calendarDate