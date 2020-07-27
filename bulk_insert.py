from tqdm import tqdm

def insert_table(cursor, reader, table_name, table_specs):
    """Bulk insert a reader object into a table."""

    ## generate sql create statement
    col_declaration = ", ".join("{} {}".format(k, v) for k, v in table_specs.items())
    table_string = "CREATE TABLE {} ({})".format(table_name, col_declaration)

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
