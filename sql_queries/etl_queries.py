
product_review_record_fact_query = '''
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
    '''

product_review_dimension_query = '''
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
'''

product_dimension_query = '''
    SELECT
        DISTINCT metadata_staging.asin,
        metadata_staging.brand,
        metadata_staging.title,
        metadata_staging.price
    FROM 
        metadata_staging
    INNER JOIN product_review_record_fact
        ON metadata_staging.asin = product_review_record_fact.asin
'''

product_related_product_dimension_query = '''
    SELECT
        DISTINCT product_related_product_staging.*
    FROM 
        product_related_product_staging
    INNER JOIN product_dimension
        ON product_related_product_staging.asin = product_dimension.asin
            OR product_related_product_staging.related_asin = product_dimension.asin
'''

product_sales_rank_by_category_dimension_query = '''
    SELECT
        pss.*
    FROM 
        product_sales_rank_staging AS pss
    INNER JOIN product_dimension AS pd
        ON pss.asin = pd.asin
    ORDER BY
        pss.product_category,
        pss.sales_rank
'''

product_sales_rank_dimension_query = '''
    SELECT
        psscd.asin,
        min(psscd.sales_rank) as sales_rank
    FROM 
        product_sales_rank_by_category_dimension AS psscd
    GROUP BY
        psscd.asin
    ORDER BY
        sales_rank
'''

product_category_dimension_query = '''
    SELECT
        pcs.*
    FROM 
        product_category_staging AS pcs
    INNER JOIN product_dimension AS pd
        ON pcs.asin = pd.asin
'''

user_review_dimension_query = '''
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
'''