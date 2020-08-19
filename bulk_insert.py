from tqdm import tqdm

fk_declaration = " FOREIGN KEY ({0[column]}) REFERENCES {0[reference]}"
fk_name = " CONSTRAINT {0[name]}"
fk_update = " ON UPDATE {0[update]}"
fk_delete = " ON DELETE {0[delete]}"

def define_columns(k, v):
    '''Returns a string defining column names and their datatypes in SQL.
    Key:Value pairs are interpreted as column_name:datatype.
    If the key is a tuple listign multiple column names, all column names get the same datatype.
    '''
    if type(k) is tuple:
        return ", ".join("{} {}".format(col, v) for col in k)
    else: 
        return "{} {}".format(k, v)

def define_foreign_keys(fk_dict):
    """Returns a string defining foreign keys for SQL table.
    Name of the table constraint, as well as behaviour on update and delete
    are optional, like in MySQL
    """
    fk_statement = fk_declaration.format(fk_dict)
    if "name" in fk_dict:
        fk_statement = fk_name.format(fk_dict) + fk_statement
    if "update" in fk_dict:
        fk_statement = fk_statement + fk_update.format(fk_dict)
    if "delete" in fk_dict:
        fk_statement = fk_statement + fk_delete.format(fk_dict)
    return fk_statement

def define_table(table_name, table_specs):
    """Returns CREATE TABLE statement as string"""

    col_definitions = {k: v for k, v in table_specs.items() if not isinstance(v, dict)}
    fk_definitions = {k :v for k,v in table_specs.items() if isinstance(v, dict)}

    sql_code = ", ".join(define_columns(k, v) for k, v in col_definitions.items() if k is not "fk")
    if fk_definitions:  #proceed if dict is not empty
        fk_statements = [define_foreign_keys(fk) for fk in fk_definitions.values()]
        sql_code = ",".join([sql_code] + fk_statements)
    
    table_definition = "CREATE TABLE {} ({})".format(table_name, sql_code)

    return table_definition

def insert_table(cursor, reader, table_name, table_specs):
    """Bulk insert a reader object into a table."""

    ## generate sql create statement
    table_definition = define_table(table_name, table_specs)

    ## create table 
    cursor.execute(table_definition)
    
    ## generate sql insert statement
    col_definitions = {k: v for k, v in table_specs.items() if not isinstance(v, dict)}
    col_names = ", ".join(col_definitions.keys())
    val_placeholder = ("%s, " * len(col_definitions))[:-2] # remove last two characters ", "
    insert_string = "INSERT INTO {} ({}) VALUES ({})".format(table_name, col_names, val_placeholder)

    ## insert table chunkwise from reader created through pandas
    print("Insert of table " + table_name + " started...")

    for chunk in tqdm(reader):
        chunk = chunk.loc[:, col_definitions.keys()] #reduce chunk to specified columns
        values = chunk.to_records(index=False)
        values = map(tuple, values)
        values = list(values)
        cursor.executemany(insert_string, values)

    print("Insert of table " + table_name + " completed\n")

def insert_multiple_tables(cursor, reader_func, insert_instructions):
    
    for table_name, instructions in insert_instructions.items():
        
        path = instructions[0]
        column_dtype_map = instructions[1]
        reader = reader_func(path)

        insert_table(cursor, reader, table_name, column_dtype_map)

