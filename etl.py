from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from helpers.metadata import parse as parse_metadata, \
    only_with_all_must_exist_keys, \
    remove_unnecessary_keys, \
    only_products_with_brand, related_products, apply_related_products_staging_schema

from helpers.ratings import parse as parse_ratings, convert_ts_to_date, cast_rating_to_int

conf = SparkConf() \
    .setMaster('local')\
    .setAppName('dend-capstone')

sc = SparkContext(conf=conf)

session = SparkSession(sc)

# Initializes amazon ratings text files
ratings_staging = sc.textFile('sample_data/amazon_ratings/ratings_Musical_Instruments.csv') \
    .map(parse_ratings) \
    .map(convert_ts_to_date) \
    .map(cast_rating_to_int) \

metadata_staging = sc.textFile('sample_data/metadata_test.json') \
    .map(parse_metadata) \
    .filter(only_with_all_must_exist_keys) \
    .filter(only_products_with_brand) \
    .map(remove_unnecessary_keys)

metadata_staging.persist()

related_products_staging = metadata_staging \
    .map(related_products) \
    .flatMapValues(dict.items) \
    .map(lambda row: ((row[0], row[1][0]), row[1][1])) \
    .flatMapValues(lambda asin: asin) \
    .map(apply_related_products_staging_schema)

metadata_staging.toDF().createOrReplaceTempView('metadata_staging')
ratings_staging.toDF().createOrReplaceTempView('product_review_record_staging')
related_products_staging.toDF().createOrReplaceTempView('related_products_staging')

product_review_record_fact = session.sql('''
    SELECT
        product_review_record_staging.*
    FROM 
        product_review_record_staging
    INNER JOIN metadata_staging
        ON metadata_staging.asin = product_review_record_staging.asin

    ''')

product_review_record_fact.persist()
product_review_record_fact.createOrReplaceTempView('product_review_record_fact')

product_review_dimension = session.sql('''
    SELECT
        asin,
        count(asin) AS review_count,
        round(sum(rating)/count(asin), 2) AS avg_rating,
        min(rating) AS min_rating,
        max(rating) AS max_rating,
        min(review_date) AS first_review_date,
        max(review_date) AS last_review_date
    FROM 
        product_review_record_fact
    GROUP BY asin
    ''')

product_review_dimension.show()
exit()

product_dimension = session.sql('''
    SELECT
        DISTINCT metadata_staging.asin,
        metadata_staging.brand,
        metadata_staging.title,
        metadata_staging.price
    FROM 
        metadata_staging
    INNER JOIN product_review_record_fact
        ON metadata_staging.asin = product_review_record_fact.asin
    ''')

product_dimension.createOrReplaceTempView('product_dimension')

related_products_dimension = session.sql('''
    SELECT
        DISTINCT related_products_staging.*
    FROM 
        related_products_staging
    INNER JOIN product_dimension
        ON related_products_staging.asin = product_dimension.asin
            OR related_products_staging.related_asin = product_dimension.asin
    ''')

#
# product_review_records_fact.createOrReplaceTempView('product_review_records_fact')
#
# # product_review_records_fact.write \
# #     .parquet(
# #         'output/product_review_records_fact/product_review_records_fact.parquet',
# #         'overwrite',
# #     )
#
# asin_count = session.sql('''
#     SELECT
#         count(*)
#     FROM product_review_record_staging
#     ''')
#
# asin_count2 = session.sql('''
#     SELECT
#         count(*)
#     FROM product_review_records_fact
#     ''')
#
# #count = asin_count.rdd.first()
# count2 = asin_count2.rdd.first()
#
# #print(count)
# print(count2)