from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from helpers.metadata import *
from helpers.ratings import *

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
    .map(map_related_products) \
    .flatMapValues(dict.items) \
    .map(lambda row: ((row[0], row[1][0]), row[1][1])) \
    .flatMapValues(lambda asin: asin) \
    .map(apply_related_products_staging_schema)

product_sales_rank_staging = metadata_staging \
    .map(map_sales_rank) \
    .flatMapValues(dict.items) \
    .map(apply_product_sales_rank_staging_schema)

product_categories_staging = metadata_staging \
    .map(map_categories) \
    .flatMapValues(lambda categories: categories) \
    .flatMapValues(lambda category: category) \
    .map(apply_product_category_staging_schema)

metadata_staging.toDF().createOrReplaceTempView('metadata_staging')
ratings_staging.toDF().createOrReplaceTempView('product_review_record_staging')
related_products_staging.toDF().createOrReplaceTempView('related_products_staging')
product_sales_rank_staging.toDF().createOrReplaceTempView('product_sales_rank_staging')
product_categories_staging.toDF().createOrReplaceTempView('product_categories_staging')

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

product_dimension.persist()
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

product_sales_rank_dimension = session.sql('''
    SELECT
        pss.*
    FROM 
        product_sales_rank_staging AS pss
    INNER JOIN product_dimension AS pd
        ON pss.asin = pd.asin
    ORDER BY
        product_category,
        sales_rank
    ''')

product_categories_dimension = session.sql('''
    SELECT
        pcs.*
    FROM 
        product_categories_staging AS pcs
    INNER JOIN product_dimension AS pd
        ON pcs.asin = pd.asin
    ''')

user_review_dimension = session.sql('''
    SELECT
        user,
        count(asin) AS review_count,
        round(sum(rating)/count(asin), 2) AS avg_rating,
        min(rating) AS min_rating,
        max(rating) AS max_rating,
        min(review_date) AS first_review_date,
        max(review_date) AS last_review_date
    FROM 
        product_review_record_fact
    GROUP BY user
    ''')

