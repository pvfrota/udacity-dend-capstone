import ast

MUST_EXIST_KEYS = [
    'asin',
    'title',
    'price',
    'related',
    'salesRank',
    'brand',
    'categories',
]


def parse(row):
    return ast.literal_eval(row)


def only_with_all_must_exist_keys(row):
    for key in MUST_EXIST_KEYS:
        if key not in row.keys():
            return False

    return True

def 

def remove_unnecessary_keys(row):
    keys_to_remove = set(row.keys()) - set(MUST_EXIST_KEYS)

    for key in keys_to_remove:
        row.pop(key)

    return row
