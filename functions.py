# functions.py
from pyspark.sql.functions import pandas_udf, concat_ws, sha2, rand
from pyspark.sql.types import StringType
import random

@pandas_udf(StringType())
def row_hash_func(*cols):
    def hash_with_random(row):
        random_number = random.randint(1, 1000000)
        concatenated = ''.join(str(val) for val in row) + str(random_number)
        hash_value = sha2(concatenated, 256)
        return f"{hash_value}##{random_number}"
    
    return concat_ws('', *cols).apply(hash_with_random)
