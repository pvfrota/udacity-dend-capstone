
best_selling_product_related_product_query = '''
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
                AND p.asin = '{}'
        ORDER BY
            prp.relation_type,
            r_rank.sales_rank,
            r.price
    '''

best_selling_product_same_category_query = '''
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
            AND p.asin = '{}'
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
'''

best_rated_related_product_query = '''
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
            AND p.asin = '{}'
    ORDER BY
        prp.relation_type,
        r_review.avg_rating DESC,
        r_review.review_count DESC,
        r.price
'''

best_rated_product_same_category_query = '''
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
            AND p.asin = '{}'
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
'''

cheaper_related_product_query = '''
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
            AND p.asin = '{}'
    ORDER BY
        prp.relation_type,
        r.price
'''

cheaper_product_same_category_query = '''
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
            AND p.asin = '{}'
    GROUP BY
        r.asin,
        r.brand,
        r.title,
        r.price
    ORDER BY
        r.price
'''

best_rated_product_query = '''
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
'''

worst_rated_product_query = '''
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
'''

most_rated_product_query = '''
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
'''

best_rated_product_by_category_query = '''
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
'''

best_selling_product_query = '''
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
'''

worst_selling_product_query = '''
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
'''

best_selling_product_by_category_query = '''
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
'''

user_review_count_query = '''
    SELECT
        *
    FROM 
        user_review_dimension
    ORDER BY
        review_count DESC,
        last_review_date DESC,
        first_review_date DESC
'''

best_rated_product_in_interval_query = '''
    SELECT
        prrf.asin,
        count(prrf.asin) AS review_count,
        round(sum(prrf.rating)/count(prrf.asin), 2) AS avg_rating,
        pd.*
    FROM
        product_review_record_fact AS prrf
    INNER JOIN
        product_dimension AS pd
            ON pd.asin = prrf.asin
    WHERE
        prrf.review_year >= {}
        AND prrf.review_month >= {}
    GROUP BY
        prrf.asin,
        pd.asin,
        pd.brand,
        pd.title,
        pd.price
    ORDER BY 
        avg_rating DESC,
        review_count DESC,
        pd.price
    '''