
class STable:
    def __init__(self,auth_url):
        self.auth_url = auth_url
        
    def read(self,file_name):
        pass
    
    def write(self,file_name):
        pass
    

class Mapping:
    def __init__(self,table_name,mapping_file="xzxsx"):
        self.mapping_file=mapping_file
        self.table_name = table_name
    
    def get_select_query(self):
        pass
    
    def get_create_query(self):
        column_definitions = []
        for row in column_mapping:
                col_name = row['target_column']
                col_type = row['target_column_type'].upper()
                
                if col_type == 'INTEGER':
                    col_type = 'INT'
                elif col_type == 'DATETIME':
                    col_type = 'TIMESTAMP'
                
                column_definitions.append(f"{col_name} {col_type}")
            
        columns = ",\n    ".join(column_definitions)
        create_statement = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
        return create_statement
    
    def get_upsert_query(self):
        pass 
    

class Table:
    
    def __init__(self,spark,primary_key,catalog,schema_name,table_name):
        self.spark = spark
        self.primary_key = primary_key
        self.catalog = catalog 
        self.schema_name = schema_name
        self.table_name = table_name
        self.mapping = Mapping(table_name)
    

    def create(self):
        """
        create table (along with change data feed) if not exists
        """
        self.spark(self.mapping.get_create_query()) 
    
    def update(self,df):
        pass
        
    def get_data(self):
        return spark.table(f"{}")
    
