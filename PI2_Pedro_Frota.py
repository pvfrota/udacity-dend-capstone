# coding: utf8

import datetime
import re

import click
import matplotlib.pyplot as plt

from pyspark import SparkConf, SparkContext

conf = SparkConf() \
    .setMaster('local')\
    .setAppName('pi2p1')

sc = SparkContext(conf=conf)

text_file = sc.newAPIHadoopFile('amazon-meta.txt', "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
            conf = {"textinputformat.record.delimiter":  "\r\n\r\n"}).map(lambda l: l[1])

filtered = text_file.filter(lambda l: 'Total items' not in l and 'discontinued product' not in l)


KEYS = [
    'ASIN',
    'title',
    'group',
    'salesrank',
    'similar',
    'categories',
    'reviews'
]


def make_dictio(arr):
    dictio = {elem[0]: elem[1] for elem in arr}

    reviews = {elem[0]: elem[1] for elem in dictio['reviews'][0]}

    if len(dictio['reviews']) == 2:
        reviews.update({'records': [{data[0]: data[1] for data in record} for record in dictio['reviews'][1]]})

    dictio['reviews'] = reviews

    return dictio


def parse(line):
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

    return make_dictio(line)


parsed = filtered.map(parse)


def avg(lst):
    return sum(lst)/len(lst)


def format_comment(comment):
    return ', '.join([': '.join([comment.keys()[i], comment.values()[i]]) for i in range(len(comment))])


@click.group()
def cli():
    """
    PPGINF545
    - Projeto de Implementação 2
    - Pedro Vitor Mesquita da Frota
    - 2200299
    - Selecione uma questão (letter-<letra>):
    """
    pass


@cli.command()
@click.option('--product-id', default=463526, help='Product ID to search')
def letter_a(product_id):
    get_product_by_id = parsed.filter(lambda product: int(product['Id']) == product_id)

    product_reviews = get_product_by_id.map(lambda product: product['reviews'])

    product_reviews.persist()

    helpful_comments_sort_rating_asc = product_reviews \
        .map(lambda reviews: sorted(
            reviews['records'],
            key=lambda rec: (int(rec['helpful']), int(rec['rating'])),
            reverse=True
        )[:5])

    helpful_comments_sort_rating_desc = product_reviews \
        .map(lambda reviews: sorted(
            reviews['records'],
            key=lambda rec: (int(rec['helpful']), -int(rec['rating'])),
            reverse=True
        )[:5])

    helpful_comments = {
        'rating_sort_asc': helpful_comments_sort_rating_asc.first(),
        'rating_sort_desc': helpful_comments_sort_rating_desc.first()
    }

    print('\n\nComentários mais úteis para o produto de #ID: {}\n---'.format(product_id))

    print('\nMaiores avaliações:\n---\n')
    for comment in helpful_comments['rating_sort_desc']:
        print(format_comment(comment))

    print('\nMenores avaliações:\n---\n')
    for comment in helpful_comments['rating_sort_asc']:
        print(format_comment(comment))

    print('\n\n')


@cli.command()
@click.option('--product-asin', default='0865779287', help='Product ASIN to search')
def letter_b(product_asin):
    similar_product_with_product_asin_salesrank = parsed.map(
        lambda product: ((product['ASIN'], int(product['salesrank'])), product['similar'][1]) if len(
            product['similar']) > 1 else None) \
        .filter(
        lambda product_asin_salesrank_with_similar_product: product_asin_salesrank_with_similar_product is not None) \
        .flatMapValues(lambda similar: similar) \
        .map(lambda product_asin_salesrank_with_similar_product: (product_asin_salesrank_with_similar_product[1], product_asin_salesrank_with_similar_product[0]))

    product_asin_with_salesrank = parsed.map(lambda product: (product['ASIN'], int(product['salesrank'])))

    best_selling_similar_products = similar_product_with_product_asin_salesrank \
        .join(product_asin_with_salesrank) \
        .map(lambda rel: (rel[1][0], (rel[0], rel[1][1]))) \
        .filter(lambda rel: rel[0][1] > rel[1][1]) \
        .groupByKey() \
        .mapValues(lambda similar_products: sorted(similar_products, key=lambda similar_product: similar_product[1]))

    best_selling_similar_products_for_product = best_selling_similar_products \
        .filter(lambda product: product[0][0] == product_asin)

    best_selling_similar_products_for_product_ = best_selling_similar_products_for_product.first()

    print(
        "\n\nProdutos similares mais vendidos para o produto de #ASIN: {} (salesrank {})"
        .format(
            best_selling_similar_products_for_product_[0][0],
            best_selling_similar_products_for_product_[0][1]
        )
    )

    i = 1
    for product in best_selling_similar_products_for_product_[1]:
        print('{}º - ASIN: {}, Salesrank: {}'.format(i, product[0], product[1]))

        i = i+1

    print("\n\n")


@cli.command()
@click.option('--product-id', default=463526, help='Product ID to search')
def letter_c(product_id):
    get_product_by_id = parsed.filter(lambda product: int(product['Id']) == product_id)

    product_reviews = get_product_by_id.map(lambda product: product['reviews'])

    product_review_ratings_by_date = product_reviews \
        .map(lambda product_reviews__: [(record['date'], record['rating']) for record in product_reviews__['records']]) \
        .map(lambda ratings_with_dates: [
        [ratings_with_dates[i][0], round(avg([float(rating[1]) for rating in ratings_with_dates][:i + 1]), 1)] for i in
        range(len(ratings_with_dates))])

    product_review_ratings_by_date = product_review_ratings_by_date.first()

    product_review_ratings_by_date = [
        [datetime.datetime.strptime(elem[0], "%Y-%m-%d").date() for elem in product_review_ratings_by_date],
        [elem[1] for elem in product_review_ratings_by_date]
    ]

    print(product_review_ratings_by_date)

    plt.plot_date(product_review_ratings_by_date[0], product_review_ratings_by_date[1], 'b-')
    plt.title("Evolucao das medias de avaliacao, produto #ID: {}".format(product_id), fontsize=8)

    plt.show()


@cli.command()
def letter_d():
    product_categories = parsed.map(lambda product: (
        ((product['Id'], int(product['salesrank'])), product['categories'][1]) if product['categories'][0] != '0' else None)) \
        .filter(lambda product: product is not None) \
        .flatMapValues(lambda product_category: product_category)

    category_products = product_categories \
        .map(lambda product_category: (product_category[1], product_category[0])) \
        .groupByKey()

    leading_selling_products_by_category = category_products \
        .map(lambda category_product: (category_product[0], sorted(category_product[1], key=lambda product: product[1])[:10]))

    print(leading_selling_products_by_category.collect())


@cli.command()
def letter_e():
    product_with_avg_rating_and_group = parsed \
        .map(lambda product: (product['group'], (product['Id'], float(product['reviews']['avg_rating']))))

    groups_with_product_avg_ratings = product_with_avg_rating_and_group.groupByKey()

    best_rated_products_by_group = groups_with_product_avg_ratings \
        .mapValues(lambda product_ratings: sorted(product_ratings, key=lambda product: product[1], reverse=True)[:10])

    print(best_rated_products_by_group.collect())


@cli.command()
def letter_f():
    categories_with_avg_ratings = parsed.map(lambda product: (
        (float(product['reviews']['avg_rating']), product['categories'][1]) if product['categories'][0] != '0' else None)) \
        .filter(lambda categories: categories is not None) \
        .flatMapValues(lambda category: category) \
        .map(lambda category: (category[1], category[0])) \
        .groupByKey()

    category_avg_ratings = categories_with_avg_ratings.map(lambda category: (category[0], avg(category[1])))

    sorted_category_avg_ratings = category_avg_ratings \
        .sortBy(lambda category: category[1], ascending=False) \
        .take(5)

    for category_avg_rating in sorted_category_avg_ratings:
        print("Categoria {}: {}".format(category_avg_rating[0], category_avg_rating[1]))


@cli.command()
def letter_g():
    customer_reviews_by_group_count = parsed \
        .map(lambda product: (product['group'], [review['customer'] for review in product['reviews']['records']]) if 'records' in product['reviews'].keys() else None) \
        .filter(lambda group_customer_reviews_: group_customer_reviews_ is not None) \
        .flatMapValues(lambda customer_review: customer_review) \
        .map(lambda customer_review: (customer_review, 1)) \
        .groupByKey() \
        .map(lambda group_customer_review: (group_customer_review[0], len(group_customer_review[1])))

    all_customer_reviews_with_count_by_group = customer_reviews_by_group_count \
        .map(lambda group_customer_review_count: (group_customer_review_count[0][0], (group_customer_review_count[0][1], group_customer_review_count[1]))) \
        .groupByKey()

    customers_who_commented_most_per_group = all_customer_reviews_with_count_by_group \
        .mapValues(lambda user_comments: sorted(user_comments, key=lambda user_comment: user_comment[1], reverse=True)[:10])

    customers_who_commented_most_per_group_ = customers_who_commented_most_per_group.collect()

    for group_customer_comments in customers_who_commented_most_per_group_:
        print('Grupo "{}": {}'.format(
            group_customer_comments[0],
            ', '.join(
                ['Usuário {}: {} comentários'.format(customer_comment[0], customer_comment[1])
                 for customer_comment in group_customer_comments[1]]
            )
        ))


if __name__ == '__main__':
    cli()

# [[elem[1], datetime.datetime.strptime(elem[0], "%Y-%m-%d").date()] for elem in arr]
