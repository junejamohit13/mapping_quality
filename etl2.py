# COMMAND ----------
# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
import pandas as pd
import networkx as nx
from itertools import permutations

# COMMAND ----------
# Helper functions

def read_mapping_files(file_path):
    """Read all mapping sheets from the Excel file."""
    xls = pd.ExcelFile(file_path)
    column_mapping = pd.read_excel(xls, 'Column Mapping')
    relationships = pd.read_excel(xls, 'Relationships')
    table_filters = pd.read_excel(xls, 'Table Filters')
    unpivot_mapping = pd.read_excel(xls, 'Unpivot Operations')
    pivot_mapping = pd.read_excel(xls, 'Pivot Operations')
    sort_order = pd.read_excel(xls, 'Sort Order')
    return column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping, sort_order

def find_optimal_join_order(relationships, source_tables):
    """Find the optimal join order for the given tables."""
    G = nx.Graph()
    for _, row in relationships.iterrows():
        if row['table_left'] in source_tables and row['table_right'] in source_tables:
            G.add_edge(row['table_left'], row['table_right'])
    
    if not nx.is_connected(G):
        return None, None  # Indicate that a complete join is not possible
    
    for start, end in permutations(source_tables, 2):
        try:
            path = nx.shortest_path(G, start, end)
            if set(path) == set(source_tables):
                join_order = list(zip(path[:-1], path[1:]))
                return path[0], join_order
        except nx.NetworkXNoPath:
            continue
    
    return None, None  # If no valid path is found

# COMMAND ----------
# Main ETL function

def perform_etl(spark, column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping, sort_order):
    target_tables = column_mapping['target_table'].unique()
    
    for target_table in target_tables:
        mappings = column_mapping[column_mapping['target_table'] == target_table]
        unpivot_ops = unpivot_mapping[unpivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        pivot_ops = pivot_mapping[pivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        
        source_tables = set(col.split('.')[0] for col in mappings['source_columns'].str.split(',').explode() if col != '*')
        primary_table, join_order = find_optimal_join_order(relationships, source_tables)
        
        if primary_table is None:
            return f"Not possible: The source tables for {target_table} are not fully connected."
        
        # Generate initial DataFrames for each source table
        table_dfs = {}
        for source_table in source_tables:
            table_columns = [col for col in mappings['source_columns'].str.split(',').explode() if col.startswith(f"{source_table}.")]
            unique_columns = list(dict.fromkeys(table_columns))
            column_exprs = [expr(f"{col}").alias(col.replace('.', '_')) for col in unique_columns]
            
            table_filter = table_filters[
                (table_filters['target_table'] == target_table) & 
                (table_filters['source_table'] == source_table)
            ]['filter_condition'].iloc[0] if not table_filters.empty else "1=1"
            
            df = spark.table(source_table).select(*column_exprs).filter(table_filter)
            table_dfs[source_table] = df
        
        # Apply unpivot operations
        for _, unpivot_op in unpivot_ops.iterrows():
            value_column_groups = unpivot_op['value_columns'].split(';')
            metric_column = unpivot_op['metric_column']
            category_columns = unpivot_op['category_columns'].split(',')
            metric_values = unpivot_op['metric_values'].split(';')

            stack_expr = "stack({}, {}) as ({}, {})".format(
                len(metric_values),
                ", ".join(f"'{metric}', {columns}" for metric, columns in zip(metric_values, value_column_groups)),
                metric_column,
                ", ".join(category_columns)
            )

            df = table_dfs[unpivot_op['source_table']].select("*", expr(stack_expr))
            table_dfs[unpivot_op['target_table']] = df
        
        # Apply pivot operations
        for _, pivot_op in pivot_ops.iterrows():
            pivot_column = pivot_op['pivot_column']
            value_column = pivot_op['value_column']
            category_columns = pivot_op['category_columns'].split(',')
            pivot_values = pivot_op['pivot_values'].split(',')

            pivot_expr = [when(col(pivot_column) == value, col(value_column)).alias(f"{value}_{category}") 
                          for value in pivot_values 
                          for category in category_columns]

            df = table_dfs[pivot_op['source_table']].groupBy("id", "date").agg(*pivot_expr)
            table_dfs[pivot_op['target_table']] = df
        
        # Perform the joins
        joined_df = table_dfs[primary_table]
        for left_table, right_table in join_order:
            if right_table not in table_dfs:
                continue
            rel = relationships[(relationships['table_left'] == left_table) & 
                                (relationships['table_right'] == right_table) |
                                (relationships['table_left'] == right_table) & 
                                (relationships['table_right'] == left_table)].iloc[0]
            
            join_condition = rel['join_condition'].replace('.', '_')
            join_type = rel['join_type'].lower()
            
            joined_df = joined_df.join(table_dfs[right_table], on=expr(join_condition), how=join_type)
        
        # Select final columns
        final_columns = [expr(col).alias(target_col) for col, target_col in zip(mappings['source_columns'], mappings['target_column_name'])]
        result_df = joined_df.select(*final_columns)
        
        # Apply sorting
        table_sort_order = sort_order[sort_order['target_table'] == target_table]['sort_columns'].iloc[0]
        if table_sort_order:
            sort_columns = table_sort_order.split(',')
            result_df = result_df.orderBy(*sort_columns)
        
        # Show the result
        result_df.show()
        
        # Optionally, you can write the result to a table or file
        # result_df.write.mode("overwrite").saveAsTable(f"{target_table}_result")
        
        return result_df

# COMMAND ----------
# Main execution block

# Initialize Spark session (this is usually not needed in Databricks as it's automatically available)
spark = SparkSession.builder.appName("ETL_Job").getOrCreate()

# Specify the path to your mapping file
mapping_file_path = "/dbfs/FileStore/shared_uploads/your_mapping_file.xlsx"

# Read mapping files
column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping, sort_order = read_mapping_files(mapping_file_path)

# Perform ETL
result = perform_etl(spark, column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping, sort_order)

# If result is a DataFrame, you can perform additional operations or analysis here
if isinstance(result, pyspark.sql.DataFrame):
    # Example: Count the number of rows
    print(f"Number of rows in the result: {result.count()}")

# COMMAND ----------
# Display the generated SQL (optional)
# Note: This will only show the SQL for the final query, not for intermediate operations
print(result._jdf.queryExecution().simpleString())
