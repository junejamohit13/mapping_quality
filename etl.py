import pandas as pd
import importlib

def read_mapping_files(file_path):
    xls = pd.ExcelFile(file_path)
    column_mapping = pd.read_excel(xls, 'Column Mapping')
    relationships = pd.read_excel(xls, 'Relationships')
    table_filters = pd.read_excel(xls, 'Table Filters')
    unpivot_mapping = pd.read_excel(xls, 'Unpivot Operations')
    return column_mapping, relationships, table_filters, unpivot_mapping

def find_optimal_join_order(relationships, source_tables):
    import networkx as nx
    from itertools import permutations

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

def perform_etl(column_mapping, relationships, table_filters, unpivot_mapping):
    target_tables = column_mapping['target_table'].unique()
    
    for target_table in target_tables:
        mappings = column_mapping[column_mapping['target_table'] == target_table]
        unpivot_ops = unpivot_mapping[unpivot_mapping['target_table'] == target_table]
        
        source_tables = set(col.split('.')[0] for col in mappings['source_columns'].str.split(',').explode() if col != '*')
        primary_table, join_order = find_optimal_join_order(relationships, source_tables)
        
        if primary_table is None:
            return f"Not possible: The source tables for {target_table} are not fully connected."
        
        cte_expressions = []
        final_select = []
        
        # Generate initial CTEs for each source table
        for source_table in source_tables:
            table_columns = [col for col in mappings['source_columns'].str.split(',').explode() if col.startswith(f"{source_table}.")]
            unique_columns = list(dict.fromkeys(table_columns))
            column_exprs = [f"{col} as {col.replace('.', '_')}" for col in unique_columns]
            
            table_filter = table_filters[
                (table_filters['target_table'] == target_table) & 
                (table_filters['source_table'] == source_table)
            ]['filter_condition'].iloc[0] if not table_filters.empty else "1=1"
            
            cte_expr = f"""
            {source_table}_cte AS (
                SELECT {', '.join(column_exprs)}
                FROM {source_table}
                WHERE {table_filter}
            )
            """
            cte_expressions.append(cte_expr)
        
        # Generate the joined CTE
        join_conditions = []
        joined_tables = {primary_table}
        for left_table, right_table in join_order:
            if right_table not in joined_tables:
                rel = relationships[(relationships['table_left'] == left_table) & 
                                    (relationships['table_right'] == right_table) |
                                    (relationships['table_left'] == right_table) & 
                                    (relationships['table_right'] == left_table)].iloc[0]
                
                join_condition = rel['join_condition'].replace('.', '_')
                join_type = rel['join_type'].upper()
                
                join_expr = f"""
                {join_type} JOIN {right_table}_cte
                ON {join_condition}
                """
                join_conditions.append(join_expr)
                joined_tables.add(right_table)
        
        joined_cte = f"""
        {target_table}_joined AS (
            SELECT {', '.join(final_select)}
            FROM {primary_table}_cte
            {' '.join(join_conditions)}
        )
        """
        cte_expressions.append(joined_cte)
        
        # Apply unpivot operations
        current_cte = f"{target_table}_joined"
        for index, unpivot_op in unpivot_ops.iterrows():
            value_column_groups = unpivot_op['value_columns'].split(';')
            metric_column = unpivot_op['metric_column']
            category_columns = unpivot_op['category_columns'].split(',')
            metric_values = unpivot_op['metric_values'].split(';')

            stack_args = []
            for metric, columns in zip(metric_values, value_column_groups):
                stack_args.extend([f"'{metric}'"] + columns.split(','))

            unpivot_expr = f"""
            {target_table}_unpivot_{index} AS (
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
                    FROM {current_cte}
                )
            )
            """
            cte_expressions.append(unpivot_expr)
            current_cte = f"{target_table}_unpivot_{index}"
            
            # Update final_select to use the new columns
            final_select = ['id', 'date', metric_column] + category_columns

        # Construct the final SQL query
        cte_sql = "WITH " + ",\n".join(cte_expressions)
        final_sql = f"""
        {cte_sql}
        SELECT {', '.join(final_select)}
        FROM {current_cte}
        WHERE {' OR '.join(f'{col} IS NOT NULL' for col in category_columns)}
        ORDER BY id, date, {metric_column}
        """
        
        return final_sql

    # ... (rest of the function remains the same)

# Example usage
mapping_file_path = "path/to/your/mapping_file.xlsx"
column_mapping, relationships, table_filters, unpivot_mapping = read_mapping_files(mapping_file_path)
result = perform_etl(column_mapping, relationships, table_filters, unpivot_mapping)
print(result)
