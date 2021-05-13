
import re
from datetime import datetime

from pyspark import Row


KEYS = [
    'ASIN',
    'title',
    'group',
    'salesrank',
    'similar',
    'categories',
    'reviews'
]


def __make_dictio(arr):
    dictio = {elem[0].lower(): elem[1] for elem in arr}

    reviews = {elem[0]: elem[1] for elem in dictio['reviews'][0]}

    if len(dictio['reviews']) == 2:
        reviews.update({'records': [{data[0]: data[1] for data in record} for record in dictio['reviews'][1]]})
    else:
        reviews['records'] = []

    dictio['reviews'] = reviews

    return dictio


def filter_junk_lines(line):
    return 'Total items' not in line and 'discontinued product' not in line


def parse(line):
    """
    Parser for "Amazon product co-purchasing network metadata" file
    :param line: a line of the file
    :return: parsed, dictionary-structured object, representing the line
    """

    for key in KEYS:
        line = re.sub(r'\r\n\s*{}:'.format(key), '~$!<{}>&^&:'.format(key), line)

    line = line.split('~$!<')

    line[0] = line[0].split(':')
    line[0][1] = re.sub(r'^\s+', '', line[0][1])

    for i in range(1, len(line)):
        line[i] = line[i].split('>&^&:')

        line[i][1] = re.sub(r'^\s+', '', line[i][1])

        if line[i][0] == 'similar':
            line[i][1] = re.sub(r'\s+', '~$!<', line[i][1])

            line[i][1] = line[i][1].split('~$!<', 1)

            if len(line[i][1]) == 2:
                line[i][1][1] = line[i][1][1].split('~$!<')

        elif line[i][0] == 'categories':
            line[i][1] = re.split(r'\r\n\s+', line[i][1], 1)

            if len(line[i][1]) == 2:
                line[i][1][1] = re.split(r'\r\n\s+', line[i][1][1])

        elif line[i][0] == 'reviews':
            line[i][1] = line[i][1].replace('avg rating:', 'avg_rating:')
            line[i][1] = line[i][1].replace('cutomer:', 'customer:')
            line[i][1] = re.sub(r':\s+', ':&?!*', line[i][1])
            line[i][1] = re.split(r'\r\n\s+', line[i][1], 1)

            line[i][1][0] = re.split(r'\s+', line[i][1][0])

            for j in range(len(line[i][1][0])):
                line[i][1][0][j] = line[i][1][0][j].split(':&?!*')

            if len(line[i][1]) == 2:
                line[i][1][1] = re.split(r'\r\n\s+', line[i][1][1])

                for j in range(len(line[i][1][1])):
                    line[i][1][1][j] = re.split(r'\s+', line[i][1][1][j])

                    if len(line[i][1][1][j]) > 0:
                        line[i][1][1][j][0] = ['date', line[i][1][1][j][0]]

                    if len(line[i][1][1][j]) > 1:
                        for k in range(1, len(line[i][1][1][j])):
                            line[i][1][1][j][k] = line[i][1][1][j][k].split(':&?!*')

    return __make_dictio(line)


def apply_product_review_record_staging_schema(row):
    return Row(
            user=row[1]['customer'].encode('utf-8'),
            asin=row[0].encode('utf-8'),
            rating=int(row[1]['rating']),
            date=datetime.strptime(row[1]['date'], '%Y-%m-%d'),
    )


def apply_product_dimension_schema(row):
    return Row(
            asin=row['asin'].encode('utf-8'),
            group=row['group'].encode('utf-8'),
            title=row['title'].encode('utf-8'),
    )
