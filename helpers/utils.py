

def data_quality_check(table, df):
    row_count = df.count()

    if not row_count:
        raise Exception("Data quality check: '{}' table: failed (0 rows)".format(table))
    else:
        print("Data quality check: '{}' table: succeed ({} rows)".format(table, row_count))
