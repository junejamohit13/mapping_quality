# COMMAND ----------
# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
import pandas as pd
import networkx as nx
from itertools import permutations
import openpyxl
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
def get_table_filter(table_filters, target_table, source_table):
    matching_filters = table_filters[
        (table_filters['target_table'] == target_table) & 
        (table_filters['source_table'] == source_table)
    ]
    
    if not matching_filters.empty:
        return matching_filters['filter_condition'].iloc[0]
    else:
        return "1=1"  # Default to no filter if no matching condition is found

def find_optimal_join_order(relationships, source_tables):
    G = nx.Graph()
    
    for _, row in relationships.iterrows():
        if row['table_left'] in source_tables and row['table_right'] in source_tables:
            G.add_edge(row['table_left'], row['table_right'])
    
    print("Graph edges:", G.edges())
    print("Is connected:", nx.is_connected(G))
    
    if not nx.is_connected(G):
        return None, None  # Indicate that a complete join is not possible
    
    # Find the table with the most connections (likely to be the central table)
    central_table = max(G.degree, key=lambda x: x[1])[0]
    
    # Use BFS to get a join order starting from the central table
    join_order = list(nx.bfs_edges(G, central_table))
    
    return central_table, join_order
def perform_etl(column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping):
    target_tables = column_mapping['target_table'].unique()
    
    for target_table in target_tables:
        mappings = column_mapping[column_mapping['target_table'] == target_table]
        unpivot_ops = unpivot_mapping[unpivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        pivot_ops = pivot_mapping[pivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        
        source_tables = set(col.split('.')[0] for col in mappings['source_columns'].str.split(',').explode() if col != '*')
        
        primary_table, join_order = find_optimal_join_order(relationships, source_tables)
        
        if primary_table is None:
            return f"Not possible: The source tables for {target_table} are not fully connected."
        
        cte_expressions = []
        unpivot_ctes = []
        
        # Generate initial CTEs for each source table
        for source_table in source_tables:
            if source_table in unpivot_ops['target_table'].values:
                # This is an unpivoted table, so we need to generate its CTE differently
                unpivot_op = unpivot_ops[unpivot_ops['target_table'] == source_table].iloc[0]
                value_column_groups = unpivot_op['value_columns'].split(';')
                metric_column = unpivot_op['metric_column']
                category_columns = unpivot_op['category_columns'].split(',')
                metric_values = unpivot_op['metric_values'].split(';')

                stack_args = []
                for metric, columns in zip(metric_values, value_column_groups):
                    stack_args.extend([f"'{metric}'"] + columns.split(','))

                unpivot_cte = f"""
                {source_table} AS (
                    SELECT 
                        id,
                        date,
                        {metric_column},
                        {', '.join(category_columns)}
                    FROM (
                        SELECT 
                            id,
                            date,
                            stack({len(metric_values)}, {', '.join(stack_args)}) AS ({metric_column}, {', '.join(category_columns)})
                        FROM {unpivot_op['source_table']}
                    )
                )
                """
                unpivot_ctes.append(unpivot_cte)
            else:
                # This is a regular source table
                table_columns = [col for col in mappings['source_columns'].str.split(',').explode() if col.startswith(f"{source_table}.")]
                unique_columns = list(dict.fromkeys(table_columns))
                column_exprs = [f"{col} as {col.replace('.', '_')}" for col in unique_columns]
                
                table_filter = get_table_filter(table_filters, target_table, source_table)
                
                cte_expr = f"""
                {source_table}_cte AS (
                    SELECT {', '.join(column_exprs)}
                    FROM {source_table}
                    WHERE {table_filter}
                )
                """
                cte_expressions.append(cte_expr)
        
        # Add unpivot CTEs after regular CTEs
        cte_expressions.extend(unpivot_ctes)
        
        # Apply pivot operations
        for _, pivot_op in pivot_ops.iterrows():
            pivot_column = pivot_op['pivot_column']
            value_column = pivot_op['value_column']
            category_columns = pivot_op['category_columns'].split(',')
            pivot_values = pivot_op['pivot_values'].split(',')

            pivot_expr = f"""
            {pivot_op['target_table']} AS (
                SELECT 
                    id,
                    date,
                    {', '.join(f"MAX(CASE WHEN {pivot_column} = '{pivot_value}' THEN {value_column} END) AS {pivot_value}_{category}" for pivot_value in pivot_values for category in category_columns)}
                FROM {pivot_op['source_table']}
                GROUP BY id, date
            )
            """
            cte_expressions.append(pivot_expr)
        
        # Generate the joined CTE
        join_conditions = []
        joined_tables = {primary_table}
        for left_table, right_table in join_order:
            if right_table not in joined_tables:
                rel = relationships[(relationships['table_left'] == left_table) & 
                                    (relationships['table_right'] == right_table) |
                                    (relationships['table_left'] == right_table) & 
                                    (relationships['table_right'] == left_table)].iloc[0]
                
                join_condition = rel['join_condition']
                join_type = rel['join_type'].upper()
                
                right_table_alias = f"{right_table}_cte" if right_table not in unpivot_ops['target_table'].values else right_table
                
                join_expr = f"""
                {join_type} JOIN {right_table_alias}
                ON {join_condition}
                """
                join_conditions.append(join_expr)
                joined_tables.add(right_table)
        
        joined_cte = f"""
        {target_table}_joined AS (
            SELECT {', '.join(mappings['target_column_name'])}
            FROM {primary_table}_cte
            {' '.join(join_conditions)}
        )
        """
        cte_expressions.append(joined_cte)
        
        
        # Construct the final SQL query
        cte_sql = "WITH " + ",\n".join(cte_expressions)
        final_sql = f"""
        {cte_sql}
        SELECT {', '.join(mappings['target_column_name'])}
        FROM {target_table}_joined
        WHERE {' OR '.join(f'{col} IS NOT NULL' for col in mappings['target_column_name'] if col not in ['id', 'date'])}
        """
        
        
        return final_sql

def get_table_filter(table_filters, target_table, source_table):
    matching_filters = table_filters[
        (table_filters['target_table'] == target_table) & 
        (table_filters['source_table'] == source_table)
    ]
    
    if not matching_filters.empty:
        return matching_filters['filter_condition'].iloc[0]
    else:
        return "1=1"  # Default to no filter if no matching condition is found

# Example usage
mapping_file_path = "/Workspace/Users/mohitjuneja1983@gmail.com/etl_mapping.xlsx"
column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping, sort_order = read_mapping_files(mapping_file_path)
sql_result = perform_etl(column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping)
print(sql_result)
