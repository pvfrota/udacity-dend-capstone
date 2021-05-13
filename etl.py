
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from helpers.amazon_meta import filter_junk_lines, \
    parse as parse_amazon_meta, \
    apply_product_review_record_staging_schema, \
    apply_product_dimension_schema

from helpers.ratings import parse as parse_ratings, convert_ts_to_date, cast_rating_to_int

path = 'sample_data/amazon-meta.txt'

conf = SparkConf() \
    .setMaster('local')\
    .setAppName('dend-capstone')

sc = SparkContext(conf=conf)

session = SparkSession(sc)

# Initializes amazon meta text file
amazon_meta = sc.newAPIHadoopFile(
    path,
    "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text",
    conf={
        "textinputformat.record.delimiter": "\r\n\r\n"
    }
)

# Initializes amazon ratings text files
ratings = sc.textFile('sample_data/amazon_ratings/ratings_*.csv') \
    .map(parse_ratings) \
    .map(convert_ts_to_date) \
    .map(cast_rating_to_int)

amazon_meta = amazon_meta.map(lambda l: l[1]) \
    .filter(filter_junk_lines) \
    .map(parse_amazon_meta)

# product_review_record_staging = amazon_meta \
#         .map(lambda product: [product['asin'], product['reviews']['records']]) \
#         .flatMapValues(lambda product_review_record: product_review_record) \
#         .map(apply_product_review_record_staging_schema) \
#         .toDF() \
#         .union(ratings.toDF())

product_dimension = amazon_meta \
    .map(apply_product_dimension_schema) \
    .toDF()


ratings.toDF().createOrReplaceTempView('product_review_record_staging')
product_dimension.createOrReplaceTempView('product_dimension')

product_review_records_fact = session.sql('''
    SELECT 
        product_review_record_staging.*
    FROM product_review_record_staging
        INNER JOIN product_dimension
            ON product_dimension.asin = product_review_record_staging.asin
    
    ''') \


product_review_records_fact.createOrReplaceTempView('product_review_records_fact')

# product_review_records_fact.write \
#     .parquet(
#         'output/product_review_records_fact/product_review_records_fact.parquet',
#         'overwrite',
#     )

asin_count = session.sql('''
    SELECT 
        count(*)
    FROM product_review_record_staging
    ''')

asin_count2 = session.sql('''
    SELECT 
        count(*)
    FROM product_review_records_fact
    ''')

#count = asin_count.rdd.first()
count2 = asin_count2.rdd.first()

#print(count)
print(count2)