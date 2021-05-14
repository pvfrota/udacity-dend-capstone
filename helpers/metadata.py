import ast

from pyspark import Row

MUST_EXIST_KEYS = [
    'asin',
    'title',
    'price',
    'related',
    'salesRank',
    'brand',
    'categories',
]


def parse_metadata(row):
    return ast.literal_eval(row)


def only_with_all_must_exist_keys(row):
    for key in MUST_EXIST_KEYS:
        if key not in row.keys():
            return False

    return True


def only_products_with_brand(row):
    return row['brand'] is not None \
            and type(row['brand']) == str \
            and row['brand'].strip()


def remove_unnecessary_keys(row):
    keys_to_remove = set(row.keys()) - set(MUST_EXIST_KEYS)

    for key in keys_to_remove:
        row.pop(key)

    return row


def map_related_products(row):
    return [row['asin'], row['related']]


def map_sales_rank(row):
    return [row['asin'], row['salesRank']]


def map_categories(row):
    return [row['asin'], row['categories']]


def apply_related_products_staging_schema(row):
    return Row(
            asin=row[0][0],
            related_asin=row[1],
            relation_type=row[0][1],
    )


def apply_product_sales_rank_staging_schema(row):
    return Row(
            asin=row[0],
            product_category=row[1][0],
            sales_rank=row[1][1],
    )


def apply_product_category_staging_schema(row):
    return Row(
            asin=row[0],
            product_category=row[1],
    )
