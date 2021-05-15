from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from helpers.metadata import *
from helpers.ratings import *

conf = SparkConf() \
        .setMaster('local')\
        .setAppName('dend-capstone')

sc = SparkContext(conf=conf)


def etl():

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

    product_related_product_staging = metadata_staging \
        .map(map_product_related_product) \
        .flatMapValues(dict.items) \
        .map(lambda row: ((row[0], row[1][0]), row[1][1])) \
        .flatMapValues(lambda asin: asin) \
        .map(apply_product_related_product_staging_schema)

    product_sales_rank_staging = metadata_staging \
        .map(map_sales_rank) \
        .flatMapValues(dict.items) \
        .map(apply_product_sales_rank_staging_schema)

    product_category_staging = metadata_staging \
        .map(map_categories) \
        .flatMapValues(lambda categories: categories) \
        .flatMapValues(lambda category: category) \
        .map(apply_product_category_staging_schema)

    metadata_staging.toDF().createOrReplaceTempView('metadata_staging')
    ratings_staging.toDF().createOrReplaceTempView('product_review_record_staging')
    product_related_product_staging.toDF().createOrReplaceTempView('product_related_product_staging')
    product_sales_rank_staging.toDF().createOrReplaceTempView('product_sales_rank_staging')
    product_category_staging.toDF().createOrReplaceTempView('product_category_staging')

    product_review_record_fact = session.sql('''
        SELECT
            prrs.user,
            prrs.asin,
            prrs.rating,
            prrs.review_date,
            YEAR(prrs.review_date) AS review_year,
            MONTH(prrs.review_date) AS review_month,
            DAY(prrs.review_date) AS review_day
        FROM 
            product_review_record_staging AS prrs
        INNER JOIN metadata_staging AS ms
            ON ms.asin = prrs.asin
    
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

    product_related_product_dimension = session.sql('''
        SELECT
            DISTINCT product_related_product_staging.*
        FROM 
            product_related_product_staging
        INNER JOIN product_dimension
            ON product_related_product_staging.asin = product_dimension.asin
                OR product_related_product_staging.related_asin = product_dimension.asin
        ''')

    product_sales_rank_by_category_dimension = session.sql('''
        SELECT
            pss.*
        FROM 
            product_sales_rank_staging AS pss
        INNER JOIN product_dimension AS pd
            ON pss.asin = pd.asin
        ORDER BY
            pss.product_category,
            pss.sales_rank
        ''')

    product_sales_rank_by_category_dimension.createOrReplaceTempView('product_sales_rank_by_category_dimension')

    product_sales_rank_dimension = session.sql('''
        SELECT
            psscd.asin,
            min(psscd.sales_rank) as sales_rank
        FROM 
            product_sales_rank_by_category_dimension AS psscd
        GROUP BY
            psscd.asin
        ORDER BY
            sales_rank
        ''')

    product_category_dimension = session.sql('''
        SELECT
            pcs.*
        FROM 
            product_category_staging AS pcs
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
        GROUP BY
            user
        ORDER BY
            review_count DESC,
            last_review_date DESC,
            first_review_date DESC
        ''')

    product_review_record_fact.write \
        .partitionBy(
            'review_year',
            'review_month',
            'review_day'
        ).parquet(
            'output/product_review_record_fact.parquet',
            'overwrite',
        )

    product_review_dimension.write \
        .parquet(
            'output/product_review_dimension.parquet',
            'overwrite',
        )

    product_dimension.write \
        .parquet(
            'output/product_dimension.parquet',
            'overwrite',
        )

    product_related_product_dimension.write \
        .parquet(
            'output/product_related_product_dimension.parquet',
            'overwrite',
        )

    product_sales_rank_dimension.write \
        .parquet(
            'output/product_sales_rank_dimension.parquet',
            'overwrite',
        )

    product_sales_rank_by_category_dimension.write \
        .partitionBy('product_category') \
        .parquet(
            'output/product_sales_rank_by_category_dimension.parquet',
            'overwrite',
        )

    product_category_dimension.write \
        .partitionBy('product_category') \
        .parquet(
            'output/product_category_dimension.parquet',
            'overwrite',
        )

    user_review_dimension.write \
        .parquet(
            'output/user_review_dimension.parquet',
            'overwrite',
        )


def capstone():

    session = SparkSession(sc)

    product_review_record_fact = session.read.parquet(
        'output/product_review_record_fact.parquet'
    )

    product_review_dimension = session.read.parquet(
        'output/product_review_dimension.parquet'
    )

    product_dimension = session.read.parquet(
        'output/product_dimension.parquet'
    )

    product_related_product_dimension = session.read.parquet(
        'output/product_related_product_dimension.parquet'
    )

    product_sales_rank_dimension = session.read.parquet(
        'output/product_sales_rank_dimension.parquet'
    )

    product_sales_rank_by_category_dimension = session.read.parquet(
        'output/product_sales_rank_by_category_dimension.parquet'
    )

    product_category_dimension = session.read.parquet(
        'output/product_category_dimension.parquet'
    )

    user_review_dimension = session.read.parquet(
        'output/user_review_dimension.parquet'
    )

    ####

    product_review_record_fact.createOrReplaceTempView('product_review_record_fact')
    product_review_dimension.createOrReplaceTempView('product_review_dimension')
    product_dimension.createOrReplaceTempView('product_dimension')
    product_related_product_dimension.createOrReplaceTempView('product_related_product_dimension')
    product_sales_rank_dimension.createOrReplaceTempView('product_sales_rank_dimension')
    product_sales_rank_by_category_dimension.createOrReplaceTempView('product_sales_rank_by_category_dimension')
    product_category_dimension.createOrReplaceTempView('product_category_dimension')
    user_review_dimension.createOrReplaceTempView('user_review_dimension')

    best_selling_product_related_product = session.sql('''
        SELECT
            prp.relation_type,
            r_rank.sales_rank,
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_related_product_dimension AS prp
                ON p.asin = prp.asin
        INNER JOIN
            product_dimension AS r
                ON r.asin = prp.related_asin
        INNER JOIN
            product_sales_rank_dimension AS p_rank
                ON p.asin = p_rank.asin
        INNER JOIN
            product_sales_rank_dimension AS r_rank
                ON r.asin = r_rank.asin
        WHERE
            p_rank.sales_rank > r_rank.sales_rank
                AND p.asin = 0739045067
        ORDER BY
            prp.relation_type,
            r_rank.sales_rank,
            r.price
    ''')

    best_selling_product_same_category = session.sql('''
        SELECT
            r_rank.product_category,
            r_rank.sales_rank,
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_category_dimension AS p_category
                ON p.asin = p_category.asin
        INNER JOIN
            product_sales_rank_by_category_dimension AS p_rank
                ON p.asin = p_rank.asin
        INNER JOIN
            product_sales_rank_by_category_dimension AS r_rank
                ON r_rank.product_category = p_rank.product_category
        INNER JOIN
            product_dimension AS r
                ON r.asin = r_rank.asin
        WHERE
            p_rank.sales_rank > r_rank.sales_rank
                AND p.asin = 0739045067
        GROUP BY
            r_rank.product_category,
            r_rank.sales_rank,
            r.asin,
            r.brand,
            r.title,
            r.price
        ORDER BY
            r_rank.product_category,
            r_rank.sales_rank,
            r.price
    ''')

    best_rated_related_product = session.sql('''
        SELECT
            prp.relation_type,
            r_review.avg_rating,
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_related_product_dimension AS prp
                ON p.asin = prp.asin
        INNER JOIN
            product_dimension AS r
                ON r.asin = prp.related_asin
        INNER JOIN
            product_review_dimension AS p_review
                ON p.asin = p_review.asin
        INNER JOIN
            product_review_dimension AS r_review
                ON r.asin = r_review.asin
        WHERE
            p_review.avg_rating < r_review.avg_rating
                AND p.asin = 0739045067
        ORDER BY
            prp.relation_type,
            r_review.avg_rating DESC,
            r_review.review_count DESC,
            r.price
    ''')

    best_rated_product_same_category = session.sql('''
        SELECT
            r_review.avg_rating,
            r_review.review_count,
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_category_dimension AS p_category
                ON p.asin = p_category.asin
        INNER JOIN
            product_category_dimension AS r_category
                ON r_category.product_category = p_category.product_category
        INNER JOIN
            product_dimension AS r
                ON r.asin = r_category.asin
        INNER JOIN
            product_review_dimension AS p_review
                ON p.asin = p_review.asin
        INNER JOIN
            product_review_dimension AS r_review
                ON r.asin = r_review.asin
        WHERE
            p_review.avg_rating < r_review.avg_rating
                AND p.asin = 0739045067
        GROUP BY
            r_review.avg_rating,
            r_review.review_count,
            r.asin,
            r.brand,
            r.title,
            r.price
        ORDER BY
            r_review.avg_rating DESC,
            r_review.review_count DESC,
            r.price
    ''')

    cheaper_related_product = session.sql('''
        SELECT
            prp.relation_type,
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_related_product_dimension AS prp
                ON p.asin = prp.asin
        INNER JOIN
            product_dimension AS r
                ON r.asin = prp.related_asin
        WHERE
            p.price > r.price
                AND p.asin = 0739045067
        ORDER BY
            prp.relation_type,
            r.price
    ''')

    cheaper_product_same_category = session.sql('''
        SELECT
            r.*
        FROM
            product_dimension AS p
        INNER JOIN
            product_category_dimension AS p_category
                ON p.asin = p_category.asin
        INNER JOIN
            product_category_dimension AS r_category
                ON r_category.product_category = p_category.product_category
        INNER JOIN
            product_dimension AS r
                ON r.asin = r_category.asin
        WHERE
            p.price > r.price
                AND p.asin = 0739045067
        GROUP BY
            r.asin,
            r.brand,
            r.title,
            r.price
        ORDER BY
            r.price
    ''')

    best_rated_product = session.sql('''
        SELECT
            *
        FROM
            product_dimension AS p
        INNER JOIN
            product_review_dimension AS prd
                ON p.asin = prd.asin
        ORDER BY
            prd.avg_rating DESC,
            prd.review_count DESC,
            p.price
    ''')

    worst_rated_product = session.sql('''
        SELECT
            *
        FROM
            product_dimension AS p
        INNER JOIN
            product_review_dimension AS prd
                ON p.asin = prd.asin
        ORDER BY
            prd.avg_rating,
            prd.review_count DESC,
            p.price
    ''')

    most_rated_product = session.sql('''
        SELECT
            *
        FROM
            product_dimension AS p
        INNER JOIN
            product_review_dimension AS prd
                ON p.asin = prd.asin
        ORDER BY
            prd.review_count DESC,
            prd.avg_rating DESC,
            p.price
    ''')

    best_rated_product_by_category = session.sql('''
        SELECT 
            product_category,
            asin,
            brand,
            title,
            price,
            review_count,
            avg_rating,
            min_rating,
            max_rating,
            first_review_date,
            last_review_date 
        FROM (
                SELECT
                    row_number() OVER (
                                        PARTITION BY 
                                            pcd.product_category 
                                        ORDER BY 
                                            prd.avg_rating DESC,
                                            prd.review_count DESC
                                        ) AS pos,
                    pcd.product_category,
                    p.asin,
                    p.brand,
                    p.title,
                    p.price,
                    prd.review_count,
                    prd.avg_rating,
                    prd.min_rating,
                    prd.max_rating,
                    prd.first_review_date,
                    prd.last_review_date
                FROM
                    product_category_dimension AS pcd
                INNER JOIN
                    product_dimension AS p
                        ON p.asin = pcd.asin
                INNER JOIN
                    product_review_dimension AS prd
                        ON p.asin = prd.asin
            )
        WHERE
            pos = 1
        ORDER BY
            product_category
    ''')

    best_selling_product = session.sql('''
        SELECT
            psrd.sales_rank,
            p.*
        FROM
            product_sales_rank_dimension AS psrd
        INNER JOIN
            product_dimension AS p
            ON p.asin = psrd.asin
        ORDER BY
            psrd.sales_rank,
            p.price
    ''')

    worst_selling_product = session.sql('''
        SELECT
            psrd.sales_rank,
            p.*
        FROM
            product_sales_rank_dimension AS psrd
        INNER JOIN
            product_dimension AS p
            ON p.asin = psrd.asin
        ORDER BY
            psrd.sales_rank DESC,
            p.price
    ''')

    best_selling_product_by_category = session.sql('''
        SELECT 
            product_category,
            sales_rank,
            asin,
            brand,
            title,
            price 
        FROM (
            SELECT
                row_number() OVER (
                                    PARTITION BY 
                                        pcd.product_category 
                                    ORDER BY 
                                        psrd.sales_rank,
                                        p.price
                                    ) AS pos,
                pcd.product_category,
                psrd.sales_rank,
                p.asin,
                p.brand,
                p.title,
                p.price
            FROM
                product_category_dimension AS pcd
            INNER JOIN
                product_dimension AS p
                ON p.asin = pcd.asin
            INNER JOIN
                product_sales_rank_dimension AS psrd
                ON p.asin = psrd.asin
            )   
        WHERE
            pos = 1
        ORDER BY
            product_category
    ''')

    user_review_count = session.sql('''
        SELECT
            *
        FROM 
            user_review_dimension
        ORDER BY
            review_count DESC,
            last_review_date DESC,
            first_review_date DESC
    ''')

    best_selling_product_by_category.show()

etl()

