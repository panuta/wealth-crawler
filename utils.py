from dateutil.relativedelta import *


def split_date_period(from_date, to_date):
    dates = []

    prev_date = from_date
    next_date = from_date + relativedelta(months=1)

    while next_date < to_date:
        dates.append((prev_date, next_date))
        prev_date = next_date + relativedelta(days=1)
        next_date = prev_date + relativedelta(months=1)

    if next_date >= to_date:
        dates.append((prev_date, to_date))

    return dates


def to_thai_date(d):
    return '{day}/{month}/{year}'.format(day=d.day, month=d.month, year=d.year + 543)
