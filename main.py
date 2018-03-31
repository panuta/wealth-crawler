import logging
from datetime import date, timedelta, datetime

from flask import Flask, abort
from flask import request, Response

from google.appengine.api import taskqueue

from utils import split_date_period, to_thai_date

app = Flask(__name__)

from tasks import crawl_fund

@app.route('/', methods=['GET'])
def home():
    return Response('', status=200)


@app.route('/crawl/mutual_fund/period/', methods=['GET'])
def crawl_mutual_fund_by_period():
    from_date = request.args.get('from')
    to_date = request.args.get('to')

    if not from_date or not to_date:
        return abort(400)

    else:
        try:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        except ValueError:
            logging.exception('Error parsing from date')
            return abort(400)

        try:
            to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
        except ValueError:
            logging.exception('Error parsing to date')
            return abort(400)

        if from_date > to_date:
            logging.exception('From date is later than to date')
            return abort(400)

        for date_tuple in split_date_period(from_date, to_date):
            taskqueue.add(url='/task/crawl/mutual_fund/', params={
                'from': to_thai_date(date_tuple[0]),
                'to': to_thai_date(date_tuple[1]),
            })

        return Response('done', status=200)


@app.route('/crawl/mutual_fund/daily/', methods=['GET'])
def crawl_mutual_fund_daily():
    yesterday = date.today() + timedelta(days=-1)  # Yesterday

    task = taskqueue.add(url='/task/crawl/mutual_fund/', params={
        'from': to_thai_date(yesterday)
    })
    return Response(task.name, status=200)


if __name__ == '__main__':
    app.run()
