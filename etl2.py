import pandas as pd
import networkx as nx
from pyspark.sql.functions import coalesce, expr
import re

def read_mapping_files(file_path):
    expected_sheets = [
        'Column Mapping',
        'Relationships',
        'Table Filters',
        'Unpivot Operations',
        'Pivot Operations',
    ]
    
    try:
        excel_file = pd.ExcelFile(file_path)
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")

    missing_sheets = set(expected_sheets) - set(excel_file.sheet_names)
    if missing_sheets:
        raise ValueError(f"Missing sheets in Excel file: {', '.join(missing_sheets)}")

    dataframes = {}
    for sheet in expected_sheets:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet, dtype=str)
            if sheet == 'Column Mapping':
                print(df)
                # Don't fill NA for 'source_columns' and 'expression' in Column Mapping
                df['source_columns'] = df['source_columns'].fillna('')
                df['expression'] = df['expression'].fillna('')
            else:
                df = df.fillna('')
                df = df.astype(str)
            dataframes[sheet] = df
        except Exception as e:
            raise ValueError(f"Error reading sheet '{sheet}': {str(e)}")

    excel_file.close()

    return (dataframes['Column Mapping'],
            dataframes['Relationships'],
            dataframes['Table Filters'],
            dataframes['Unpivot Operations'],
            dataframes['Pivot Operations'])

import networkx as nx

def find_optimal_join_order(relationships, source_tables):
    G = nx.Graph()  # Use an undirected graph
    join_info = {}
    
    # Add all relationships to the graph
    for _, row in relationships.iterrows():
        G.add_edge(row['table_left'], row['table_right'])
        join_key = f"{row['table_left']}-{row['table_right']}"
        join_info[join_key] = row
        # Add reverse direction as well
        join_info[f"{row['table_right']}-{row['table_left']}"] = row
    
    # Find the minimum spanning tree that includes all source tables
    required_nodes = set(source_tables)
    for start in source_tables:
        for end in source_tables:
            if start != end:
                try:
                    path = nx.shortest_path(G, start, end)
                    required_nodes.update(path)
                except nx.NetworkXNoPath:
                    pass  # No path between these nodes, continue to the next pair
    
    subgraph = G.subgraph(required_nodes)
    mst = nx.minimum_spanning_tree(subgraph)
    
    # Perform a depth-first traversal to get the join order
    join_order = list(nx.dfs_preorder_nodes(mst, next(iter(source_tables))))
    
    # Create join pairs
    join_pairs = []
    for i in range(1, len(join_order)):
        left = join_order[i-1]
        right = join_order[i]
        path = nx.shortest_path(G, left, right)
        
        for j in range(len(path) - 1):
            path_left = path[j]
            path_right = path[j+1]
            join_key = f"{path_left}-{path_right}"
            reverse_key = f"{path_right}-{path_left}"
            
            if join_key in join_info:
                row = join_info[join_key]
                join_pairs.append((path_left, path_right, row['join_type'], row['join_condition']))
            elif reverse_key in join_info:
                row = join_info[reverse_key]
                # Swap the table references in the join condition
                join_condition = row['join_condition'].replace(path_right, 'RIGHT_TMP').replace(path_left, path_right).replace('RIGHT_TMP', path_left)
                join_pairs.append((path_left, path_right, row['join_type'], join_condition))
            else:
                raise ValueError(f"No join information found for tables {path_left} and {path_right}")
    
    primary_table = join_order[0]
    
    return primary_table, join_pairs

# Helper function to generate join conditions
def generate_join_conditions(join_pairs, table_transformations):
    join_conditions = []
    for left_table, right_table, join_type, join_condition in join_pairs:
        join_condition = join_condition.replace('#', '.')
        join_type = join_type.upper()
        
        right_table_clause = right_table
        if right_table in table_transformations:
            right_table_clause = f"{table_transformations[right_table]['cte']} AS {right_table}"
        
        join_expr = f"{join_type} JOIN {right_table_clause} ON {join_condition}"
        join_conditions.append(join_expr)
    return join_conditions



def generate_unpivot_cte(unpivot_op):
    value_column_groups = unpivot_op['value_columns'].split(';')
    metric_column = unpivot_op['metric_column']
    category_columns = unpivot_op['category_columns'].split(',')
    metric_values = unpivot_op['metric_values'].split(';')

    stack_args = []
    for metric, columns in zip(metric_values, value_column_groups):
        stack_args.extend([f"'{metric}'"] + columns.split(','))

    return f"""
    {unpivot_op['source_table_modified']} AS (
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

def generate_pivot_cte(pivot_op):
    pivot_column = pivot_op['pivot_column']
    value_column = pivot_op['value_column']
    category_columns = pivot_op['category_columns'].split(',')
    pivot_values = pivot_op['pivot_values'].split(',')

    return f"""
    {pivot_op['source_table_modified']} AS (
        SELECT 
            {', '.join(category_columns)},
            {', '.join(f"MAX(CASE WHEN {pivot_column} = '{pivot_value}' THEN {value_column} END) AS {pivot_value}" for pivot_value in pivot_values)}
        FROM {pivot_op['source_table']}
        GROUP BY {', '.join(category_columns)}
    )
    """

def parse_source_columns(source_columns_str):
    """Parse the source columns string into a list of column references."""
    return [col.strip() for col in source_columns_str.split(',') if col.strip()]

def process_column_reference(column_ref):
    """Process a single column reference (table#column)."""
    if '#' in column_ref:
        table, col = column_ref.split('#')
        return f"{table}.{col}"
    return column_ref

def generate_column_selection(mappings, primary_table, table_transformations):
    column_selection = []
    for _, row in mappings.iterrows():
        source_columns = parse_source_columns(row['source_columns'])
        expression = row.get('expression', '').strip()
        
        if not source_columns and not expression:
            column_selection.append(f"NULL AS {row['target_column_name']}")
        elif expression.startswith('func('):
            # This is a function call
            func_expr = expression[5:-1]  # Remove 'func(' and ')'
            # Replace placeholders with actual column references
            for i, col in enumerate(source_columns):
                func_expr = func_expr.replace(f'${i+1}', process_column_reference(col))
            column_selection.append(f"{func_expr} AS {row['target_column_name']}")
        elif len(source_columns) == 1:
            # Single column reference
            column_selection.append(f"{process_column_reference(source_columns[0])} AS {row['target_column_name']}")
        else:
            # Multiple columns or custom expression
            if expression:
                # Use the custom expression if provided
                for i, col in enumerate(source_columns):
                    expression = expression.replace(f'${i+1}', process_column_reference(col))
                column_selection.append(f"{expression} AS {row['target_column_name']}")
            else:
                # Concatenate multiple columns if no custom expression
                concat_expr = ' || '.join(process_column_reference(col) for col in source_columns)
                column_selection.append(f"{concat_expr} AS {row['target_column_name']}")
    
    return column_selection



def get_table_filter(table_filters, target_table, source_table):
    matching_filters = table_filters[
        (table_filters['target_table'] == target_table) & 
        (table_filters['source_table'] == source_table)
    ]
    return matching_filters['filter_condition'].iloc[0] if not matching_filters.empty else "1=1"

def perform_etl(column_mapping, relationships, table_filters, unpivot_mapping, pivot_mapping):
    target_tables = column_mapping['target_table'].unique()
    result_dict = {}
    
    for target_table in target_tables:
        mappings = column_mapping[column_mapping['target_table'] == target_table]
        
        # Filter pivot and unpivot operations for the current target table
        unpivot_ops = unpivot_mapping[unpivot_mapping['target_table'] == target_table]
        pivot_ops = pivot_mapping[pivot_mapping['target_table'] == target_table]
        
        source_tables = set(col.split('#')[0] for col in mappings['source_columns'].str.split(',').explode() if col != '*' and col != '')
        
        if len(source_tables) == 0:
            result_dict[target_table] = f"Error: No source tables found for target table {target_table}."
            continue
        
        cte_expressions = []
        table_transformations = {}
        
        # Handle unpivot operations
        for _, unpivot_op in unpivot_ops.iterrows():
            if unpivot_op['source_table_modified'] in source_tables:
                unpivot_cte = generate_unpivot_cte(unpivot_op)
                cte_expressions.append(unpivot_cte)
                table_transformations[unpivot_op['source_table_modified']] = {
                    'original': unpivot_op['source_table'],
                    'cte': unpivot_op['source_table_modified']
                }
        
        # Handle pivot operations
        for _, pivot_op in pivot_ops.iterrows():
            if pivot_op['source_table_modified'] in source_tables:
                pivot_cte = generate_pivot_cte(pivot_op)
                cte_expressions.append(pivot_cte)
                table_transformations[pivot_op['source_table_modified']] = {
                    'original': pivot_op['source_table'],
                    'cte': pivot_op['source_table_modified']
                }
        
        # Generate main query
        if len(source_tables) == 1:
            # Single table case
            source_table = list(source_tables)[0]
            table_filter = get_table_filter(table_filters, target_table, source_table)
            column_selection = generate_column_selection(mappings, source_table, table_transformations)
            
            from_clause = source_table
            if source_table in table_transformations:
                from_clause = f"{table_transformations[source_table]['cte']} AS {source_table}"
            
            main_query = f"""
            {"WITH " + ", ".join(cte_expressions) if cte_expressions else ""}
            SELECT {', '.join(column_selection)}
            FROM {from_clause}
            WHERE {table_filter}
            """
        else:
            # Multiple source tables, need to join
            try:
                primary_table, join_order = find_optimal_join_order(relationships, source_tables)
            except ValueError as e:
                result_dict[target_table] = f"Error: {str(e)} Cannot generate query for {target_table}."
                continue
            
            join_conditions = generate_join_conditions(join_order, table_transformations)
            column_selection = generate_column_selection(mappings, primary_table, table_transformations)
            
            from_clause = primary_table
            if primary_table in table_transformations:
                from_clause = f"{table_transformations[primary_table]['cte']} AS {primary_table}"
            
            # Apply filters for each table
            table_filters_list = []
            for table in source_tables:
                table_filter = get_table_filter(table_filters, target_table, table)
                if table_filter != "1=1":
                    table_filters_list.append(f"({table}.{table_filter})")
            
            where_clause = " AND ".join(table_filters_list) if table_filters_list else "1=1"
            
            main_query = f"""
            {"WITH " + ", ".join(cte_expressions) if cte_expressions else ""}
            SELECT {', '.join(column_selection)}
            FROM {from_clause}
            {' '.join(join_conditions)}
            WHERE {where_clause}
            """
        
        result_dict[target_table] = main_query
    
    return result_dict



