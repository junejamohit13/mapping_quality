import pandas as pd

def read_mapping_files(file_path):
    # Define the expected sheet names
    expected_sheets = [
        'Column Mapping',
        'Relationships',
        'Table Filters',
        'Unpivot Operations',
        'Pivot Operations',
    ]
    
    # Read the Excel file
    try:
        excel_file = pd.ExcelFile(file_path)
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")

    # Check if all expected sheets are present
    missing_sheets = set(expected_sheets) - set(excel_file.sheet_names)
    if missing_sheets:
        raise ValueError(f"Missing sheets in Excel file: {', '.join(missing_sheets)}")

    # Read each sheet into a dictionary
    dataframes = {}
    for sheet in expected_sheets:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str)
            df = df.fillna('')
            df = df.astype(str)
            dataframes[sheet] = df
        except Exception as e:
            raise ValueError(f"Error reading sheet '{sheet}': {str(e)}")

    # Close the Excel file
    excel_file.close()

    # Return the dataframes
    return (dataframes['Column Mapping'],
            dataframes['Relationships'],
            dataframes['Table Filters'],
            dataframes['Unpivot Operations'],
            dataframes['Pivot Operations'])




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
import pandas as pd
import networkx as nx

def find_optimal_join_order(relationships, source_tables):
    G = nx.DiGraph()
    dependencies = []
    valid_joins = set()
    
    # First pass: add all joins to the graph
    for _, row in relationships.iterrows():
        if row['table_left'] in source_tables and row['table_right'] in source_tables:
            join_key = f"{row['table_left']}-{row['table_right']}"
            G.add_edge(row['table_left'], join_key)
            G.add_edge(join_key, row['table_right'])
            valid_joins.add(join_key)
            
            if pd.notna(row['dependency']):
                dependencies.append((row['dependency'], join_key))
    
    # Second pass: add all dependencies
    for dep, join_key in dependencies:
        if dep not in valid_joins:
            raise ValueError(f"Dependency {dep} not found in relationships.")
        G.add_edge(dep, join_key)
    
    if not nx.is_directed_acyclic_graph(G):
        raise ValueError("The join dependencies create a cycle.")
    
    # Check if all source tables are connected
    if set(node for node in G.nodes if '-' not in node) != set(source_tables):
        raise ValueError("Not all source tables are connected in the join graph.")
    
    # Use topological sort to get the join order
    join_order = [node for node in nx.topological_sort(G) if '-' in node]
    
    # Convert join_order to pairs of (left_table, right_table)
    join_pairs = [(left, right) for join in join_order for left, right in [join.split('-')]]
    
    # The primary table is the left table of the first join
    primary_table = join_pairs[0][0]
    
    return primary_table, join_pairs

def perform_etl(column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping):
    target_tables = column_mapping['target_table'].unique()
    result_dict = {}
    
    for target_table in target_tables:
        mappings = column_mapping[column_mapping['target_table'] == target_table]
        unpivot_ops = unpivot_mapping[unpivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        pivot_ops = pivot_mapping[pivot_mapping['target_table'].str.startswith(target_table.split('_')[0])]
        
        source_tables = set(col.split('#')[0] for col in mappings['source_columns'].str.split(',').explode() if col != '*')
        
        try:
            if len(source_tables) == 1:
                primary_table = list(source_tables)[0]
                join_order = []
            else:
                primary_table, join_order = find_optimal_join_order(relationships, source_tables)
        except ValueError as e:
            result_dict[target_table] = f"Error: {str(e)} Cannot generate query for {target_table}."
            continue
        
        cte_expressions = []
        unpivot_ctes = []
        
        for source_table in source_tables:
            if source_table in unpivot_ops['target_table'].values:
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
                        {', '.join(category_columns)},
                        {metric_column},
                        value
                    FROM (
                        SELECT 
                            {', '.join(category_columns)},
                            stack({len(metric_values)}, {', '.join(stack_args)}) AS ({metric_column}, value)
                        FROM {unpivot_op['source_table']}
                    )
                )
                """
                unpivot_ctes.append(unpivot_cte)
            else:
                table_columns = [col for col in mappings['source_columns'].str.split(',').explode() if col.startswith(f"{source_table}#")]
                unique_columns = list(dict.fromkeys(table_columns))
                column_exprs = [f"{col.replace('#', '.')} as {col.replace('#', '_')}" for col in unique_columns]
                
                table_filter = get_table_filter(table_filters, target_table, source_table)
                
                cte_expr = f"""
                {source_table}_cte AS (
                    SELECT {', '.join(column_exprs)}
                    FROM {source_table}
                    WHERE {table_filter}
                )
                """
                cte_expressions.append(cte_expr)
        
        cte_expressions.extend(unpivot_ctes)
        
        for _, pivot_op in pivot_ops.iterrows():
            pivot_column = pivot_op['pivot_column']
            value_column = pivot_op['value_column']
            category_columns = pivot_op['category_columns'].split(',')
            pivot_values = pivot_op['pivot_values'].split(',')

            pivot_expr = f"""
            {pivot_op['target_table']} AS (
                SELECT 
                    {', '.join(category_columns)},
                    {', '.join(f"MAX(CASE WHEN {pivot_column} = '{pivot_value}' THEN {value_column} END) AS {pivot_value}" for pivot_value in pivot_values)}
                FROM {pivot_op['source_table']}
                GROUP BY {', '.join(category_columns)}
            )
            """
            cte_expressions.append(pivot_expr)
        
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
                
                join_condition = join_condition.replace(f"{left_table}#", f"{left_table}_cte.")
                join_condition = join_condition.replace(f"{right_table}#", f"{right_table}_cte.")
                
                right_table_alias = f"{right_table}_cte" if right_table not in unpivot_ops['target_table'].values else right_table
                
                join_expr = f"""
                {join_type} JOIN {right_table_alias}
                ON {join_condition}
                """
                join_conditions.append(join_expr)
                joined_tables.add(right_table)
        
        column_selection = []
        for _, row in mappings.iterrows():
            source_columns = row['source_columns'].split(',')
            for source_col in source_columns:
                if '#' in source_col:
                    table, col = source_col.split('#')
                    table_alias = f"{table}_cte" if table not in unpivot_ops['target_table'].values else table
                    column_selection.append(f"{table_alias}.{col} AS {row['target_column_name']}")
                elif source_col == '':
                    column_selection.append(f"NULL AS {row['target_column_name']}")
                else:
                    column_selection.append(f"{source_col} AS {row['target_column_name']}")
        
        joined_cte = f"""
        {target_table}_joined AS (
            SELECT {', '.join(column_selection)}
            FROM {primary_table}_cte
            {' '.join(join_conditions)}
        )
        """
        
        cte_expressions.append(joined_cte)
        
        cte_sql = "WITH " + ",\n".join(cte_expressions)
        final_sql = f"""
        {cte_sql}
        SELECT {', '.join(row['target_column_name'] for _, row in mappings.iterrows())}
        FROM {target_table}_joined
        """
        
        result_dict[target_table] = final_sql
    
    return result_dict

mapping_file_path = ""
column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping = read_mapping_files(mapping_file_path)

sql_result = perform_etl(column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping)
print(sql_result)
