
from datetime import datetime

KEYS = [
    'user',
    'asin',
    'rating',
    'timestamp',
]


def parse(line):
    line = line.split(',')
    parsed = {}

    for i in range(len(KEYS)):
        parsed[KEYS[i]] = line[i]

    return parsed


def convert_ts_to_date(row):
    row['review_date'] = datetime.fromtimestamp(float(row.pop('timestamp'))) \
        .replace(hour=0, minute=0, second=0, microsecond=0)

    return row


def cast_rating_to_int(row):
    row['rating'] = int(float(row['rating']))

    return row
