import csv
import logging
import re
from datetime import datetime, date

from dateutil.relativedelta import *
from flask import request, Response, abort

from google.appengine.api import urlfetch
from google.appengine.ext import ndb

from main import app


class MutualFundNav(ndb.Model):
    fund_manager = ndb.StringProperty(indexed=False)
    fund_code = ndb.StringProperty()
    fund_name_th = ndb.StringProperty(indexed=False)
    fund_name_en = ndb.StringProperty(indexed=False)

    nav_date = ndb.DateProperty()
    nav = ndb.FloatProperty(indexed=False)
    total_nav = ndb.FloatProperty(indexed=False)

    bid_price = ndb.FloatProperty(indexed=False)
    offer_price = ndb.FloatProperty(indexed=False)

    raw = ndb.TextProperty(indexed=False)


def _to_thai_date(d):
    return '{day}/{month}/{year}'.format(day=d.day, month=d.month, year=d.year + 543)


def _split_date(from_date, to_date):
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


@app.route('/task/crawl/mutual_fund/', methods=['POST'])
def crawl_mutual_fund_task():
    # No QA on date
    from_date_param = request.form.get('from')
    to_date_param = request.form.get('to')

    base_url = 'https://www.thaimutualfund.com/AIMC/aimc_navCenterDownloadRepRep.jsp'

    if from_date_param and to_date_param:
        url = '{base_url}?date1={date1}&date2={date2}'.format(
            base_url=base_url,
            date1=from_date_param,
            date2=to_date_param)
    else:
        url = '{base_url}?date={date}'.format(
            base_url=base_url,
            date=from_date_param)

    try:
        result = urlfetch.fetch(url, deadline=30, validate_certificate=True)

        if result.status_code != 200:
            logging.exception('Error with status code {}'.format(result.status_code))
            return abort(result.status_code)

        put_multi_items = []

        unicode_content = result.content.decode('cp874')
        for response_line in unicode_content.splitlines():
            if not response_line.strip():
                continue

            response_line = response_line.encode('utf-8')

            # Read and parse CSV one line at a time to be able to store raw text
            response_csv = None
            for _ in csv.reader([response_line]):
                response_csv = _

            if not response_csv:
                continue

            # If the first item is date, shift item by 1

            pattrn = re.compile('\d+\/\d+\/\d+')
            if pattrn.match(response_csv[0]):
                shift = 1
                nav_date = datetime.strptime(response_csv[0], '%d/%m/%Y').date()
            else:
                shift = 0
                from_date_day, from_date_month, from_date_year = from_date_param.split('/')
                nav_date = date(from_date_year - 543, from_date_month, from_date_day)

            fund_manager = response_csv[2 + shift]
            fund_code = response_csv[6 + shift]
            fund_name_th = response_csv[4 + shift]
            fund_name_en = response_csv[5 + shift]

            id = '{code}-{date}'.format(code=fund_code, date=nav_date.strftime('%Y%m%d'))

            try:
                nav = float(response_csv[8 + shift])
            except ValueError:
                nav = None

            try:
                total_nav = float(response_csv[7 + shift])
            except ValueError:
                total_nav = None

            try:
                offer_price = float(response_csv[11 + shift])
            except ValueError:
                offer_price = None

            try:
                bid_price = float(response_csv[12 + shift])
            except ValueError:
                bid_price = None

            item = MutualFundNav(
                id=id,
                raw=response_line,
                nav_date=nav_date,
                fund_manager=fund_manager,
                fund_code=fund_code,
                fund_name_th=fund_name_th,
                fund_name_en=fund_name_en,
                nav=nav,
                total_nav=total_nav,
                offer_price=offer_price,
                bid_price=bid_price,
            )

            put_multi_items.append(item)

            if len(put_multi_items) == 500:
                ndb.put_multi(put_multi_items)
                put_multi_items = []

    except urlfetch.Error:
        logging.exception('urlfetch error')
        return abort(400)

    context = ndb.get_context()
    context.clear_cache()

    return Response('success', status=200)