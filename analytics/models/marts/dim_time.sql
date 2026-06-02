select
    hour as time_key,
    hour,

    case
        when hour >= 22 or hour <= 4 then true
        else false
    end as is_night,

    case
        when hour >= 22 or hour <=4 then 'Night'
        when hour between 5 and 11 then 'Morning'
        when hour between 12 and 17 then 'Day'
        else 'Evening'
    end as day_period,

    case
        when hour < 12 then 'AM'
        else 'PM'
    end as am_pm

from {{ref('int_hours')}}