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

def define_table(table_specs):
    """Returns a string defining a SQL table"""
    col_statement = ", ".join(define_columns(k, v) for k, v in table_specs.items() if k is not "fk")

    if "fk" in table_specs:
        fk = table_specs.pop("fk")
        fk_statement = fk_declaration.format(fk)
        if "name" in fk:
            fk_statement = fk_name.format(fk) + fk_statement
        if "update" in fk:
            fk_statement = fk_statement + fk_update.format(fk)
        if "delete" in fk:
            fk_statement = fk_statement + fk_delete.format(fk)
        col_statement = col_statement + "," + fk_statement
        
    return col_statement

def insert_table(cursor, reader, table_name, table_specs):
    """Bulk insert a reader object into a table."""

    ## generate sql create statement
    table_string = generate_table_specs(table_specs)
    table_string = "CREATE TABLE {} ({})".format(table_name, table_string)

    ## create table 
    cursor.execute(table_string)

    ## generate sql insert statement
    col_names = ", ".join(table_specs.keys())
    val_placeholder = ("%s, " * len(table_specs))[:-2] # remove last two characters ", "
    insert_string = "INSERT INTO {} ({}) VALUES ({})".format(table_name, col_names, val_placeholder)

    ## insert table chunkwise from reader created through pandas
    print("Insert of table " + table_name + " started...")

    for chunk in tqdm(reader):
        chunk = chunk.loc[:, table_specs.keys()]
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

