from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import hashlib

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SystemUserUpsertTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Create sample empty table
def create_sample_table(table_name):
    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("system_columns", ArrayType(StringType()), nullable=True),
        StructField("system_hash", StringType(), nullable=True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode('overwrite').saveAsTable(table_name)
    print(f"Created empty table: {table_name}")

# Hash function
@udf(returnType=StringType())
def compute_hash(row: Row, cols: List[str]) -> str:
    return hashlib.md5("".join(str(row[c]) for c in cols if c in row and row[c] is not None).encode()).hexdigest()

# 2. Upsert logic for system
from pyspark.sql.functions import col, array_union, array, lit, when, struct
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

@udf(returnType=StringType())
def compute_hash(*cols):
    return hashlib.md5("".join(str(v) for v in cols if v is not None).encode()).hexdigest()


def system_upsert(table_name, new_data):
    delta_table = DeltaTable.forName(spark, table_name)
    
    # Step 1: Left join new data with existing delta table
    current_data = delta_table.toDF()
    joined_data = new_data.alias("new").join(current_data.alias("current"), "id", "left_outer")
    
    # Define columns to check (excluding metadata columns)
    columns_to_check = [c for c in new_data.columns if c not in ["id", "system_columns", "system_hash"]]
    
    # Step 2: Update system columns
    joined_data = joined_data.withColumn(
        "updated_system_columns",
        array_union(
            coalesce(col("current.system_columns"), array()),
            array([when(col(f"new.{c}").isNotNull(), lit(c)) for c in columns_to_check])
        )
    )
    
    # Remove null values from updated_system_columns
    joined_data = joined_data.withColumn(
        "updated_system_columns",
        expr("filter(updated_system_columns, x -> x is not null)")
    )
    
    # Step 3: Compute new hash based on updated_system_columns
    joined_data = joined_data.withColumn(
        "new_system_hash",
        compute_hash(struct(*[col(f"new.{c}") for c in columns_to_check]), col("updated_system_columns"))
    )
    
    # Prepare data for upsert
    upsert_data = joined_data.select(
        "id",
        *[f"new.{c}" for c in columns_to_check],
        col("updated_system_columns").alias("system_columns"),
        col("new_system_hash").alias("system_hash")
    )
    
    # Step 4: Upsert the data
    (delta_table.alias("oldData")
     .merge(
        upsert_data.alias("newData"),
        "oldData.id = newData.id"
     )
     .whenMatchedUpdate(
        condition = " OR ".join([f"coalesce(oldData.{c},'') != coalesce(newData.{c},'')" for c in columns_to_check]),
        set = {c: f"newData.{c}" for c in upsert_data.columns}
     )
     .whenNotMatchedInsert(
        values = {c: f"newData.{c}" for c in upsert_data.columns}
     )
     .execute()
    )
    print("System upsert completed")
# 3. Upsert logic for user
from pyspark.sql.functions import coalesce, col, lit, when

from pyspark.sql.functions import coalesce, col
from pyspark.sql.functions import coalesce, col, lit, when, array

def user_upsert(table_name, new_data, primary_key='id'):
    delta_table = DeltaTable.forName(spark, table_name)
    current_data = delta_table.toDF()
    
    print(f"Starting user upsert for table: {table_name}")
    print(f"New data count: {new_data.count()}")
    
    # Join new data with current data
    joined_df = new_data.alias("new").join(current_data.alias("current"), primary_key, "left_outer")
    
    # Check for conflicts
    columns_to_check = [c for c in new_data.columns if c != primary_key]
    conflict_condition = " OR ".join([
        f"""
        array_contains(current.system_columns, '{c}') AND
        (
            (new.{c} IS NULL AND current.{c} IS NOT NULL) OR
            (new.{c} IS NOT NULL AND current.{c} IS NULL) OR
            (new.{c} IS NOT NULL AND current.{c} IS NOT NULL AND new.{c} != current.{c})
        )
        """
        for c in columns_to_check
    ])
    
    conflicts = joined_df.filter(conflict_condition)
    conflict_count = conflicts.count()
    print(f"Conflicts found: {conflict_count}")
    
    if conflict_count > 0:
        error_message = "Conflicts detected: User attempting to update system-modified cells\n"
        for row in conflicts.collect():
            error_message += f"\nConflict for {primary_key}: {row[f'new.{primary_key}']}\n"
            error_message += f"System columns: {row['current.system_columns']}\n"
            for c in columns_to_check:
                if c in row["current.system_columns"] and (
                    (row[f"new.{c}"] is None and row[f"current.{c}"] is not None) or
                    (row[f"new.{c}"] is not None and row[f"current.{c}"] is None) or
                    (row[f"new.{c}"] is not None and row[f"current.{c}"] is not None and row[f"new.{c}"] != row[f"current.{c}"])
                ):
                    error_message += f"  Column '{c}': current value = '{row[f'current.{c}']}', new value = '{row[f'new.{c}']}'\n"
        raise ValueError(error_message)
    
    # Prepare update set clause
    update_set = {c: f"newData.{c}" for c in new_data.columns if c != primary_key}
    
    # Prepare update condition
    update_condition = " OR ".join([
        f"""
        (
            (newData.{c} IS NULL AND oldData.{c} IS NOT NULL) OR
            (newData.{c} IS NOT NULL AND oldData.{c} IS NULL) OR
            (newData.{c} IS NOT NULL AND oldData.{c} IS NOT NULL AND newData.{c} != oldData.{c})
        )
        """
        for c in columns_to_check
    ])
    
    print("Executing merge operation")
    # Perform merge
    (delta_table.alias("oldData")
     .merge(
        new_data.alias("newData"),
        f"oldData.{primary_key} = newData.{primary_key}"
     )
     .whenMatchedUpdate(
        condition = update_condition,
        set = update_set
     )
     .whenNotMatchedInsert(
        values = {c: f"newData.{c}" for c in new_data.columns}
     )
     .execute()
    )
    
    print("User upsert completed successfully")
  

def run_test():
    table_name = "metadata.default.test_table"
    create_sample_table(table_name)
    
    # System upsert
    system_data = spark.createDataFrame([
        ("1", None, 31, "New York"),
        ("2", "Bob", 25, "Los Angeles")
    ], ["id", "name", "age", "city"])
    system_upsert(table_name, system_data)
    
    # Display table after system upsert
    print("Table after system upsert:")
    spark.table(table_name).show()
    
    # User upsert (successful)
    user_data = spark.createDataFrame([
        ("1", "Alice", 31, "New York"),
        ("3", "Charlie", 35, "Chicago")
    ], ["id", "name", "age", "city"])
    user_upsert(table_name, user_data)
    
    # Display table after user upsert
    print("Table after user upsert:")
    spark.table(table_name).show()
    
    # User upsert (conflict)
    conflict_data = spark.createDataFrame([
        ("2", "Bob", 26, "San Francisco")
    ], ["id", "name", "age", "city"])
    try:
        user_upsert(table_name, conflict_data)
    except ValueError as e:
        print(f"Expected error caught: {e}")

    system_data = spark.createDataFrame([
        ("1", "Jackie", 31, "New York"),
        ("2", None, 25, "Los Angeles")
    ], ["id", "name", "age", "city"])
    system_upsert(table_name, system_data)
    print("Table after system upsert:")
    spark.table(table_name).show()
    
    system_data = spark.createDataFrame([
        ("1", "Jackie", 31, "New York"),
        ("3", None, 25, "Los Angeles")
    ], ["id", "name", "age", "city"])
    system_upsert(table_name, system_data)
    print("Table after system upsert:")
    spark.table(table_name).show()

    # Display final table state
    print("Final table state:")
    spark.table(table_name).show()

# Run the test
run_test()
