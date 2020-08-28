from tqdm import tqdm
from itertools import chain

## _________________________________________________________________________________
## sql placeholders and error messages

extended_insert = "INSERT INTO {} ({}) VALUES ({})"

fk_declaration = " FOREIGN KEY ({0[column]}) REFERENCES {0[reference]}"
fk_name = " CONSTRAINT {0[name]}"
fk_update = " ON UPDATE {0[update]}"
fk_delete = " ON DELETE {0[delete]}"

fk_essential_keys = {"column", "reference"}
fk_errormsg = """Foreign key definition incomplete.
Both keys 'column' and 'reference' have to be specified
in each of the nested foreign key dictionaries."""

## _________________________________________________________________________________
## helper functions

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
    
    fk_test = fk_essential_keys.issubset(fk_dict.keys())
    assert (fk_test), fk_errormsg

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
    
    table_definition = "CREATE TABLE {} (\n{}\n)".format(table_name, sql_code)

    return table_definition

def define_insert(table_name, table_specs):
    '''Returns a list of column names AND a corresponding extended INSERT statement
    in one tuple.
    '''
    ## derive column names, ignoring keys of nested dictionaries
    col_names = [k for k, v in table_specs.items() if not isinstance(v, dict)]

    ## as multiple column names may be specified in single tuples,
    ## turn all non-tuple entries into tuples of length 1
    ## so they can be unzipped into one list of strings
    col_names = [k if isinstance(k, tuple) else(k,) for k in col_names]
    col_names = list(chain(*col_names))

    ## create column name and value placeholder strings
    n_cols = len(col_names)
    col_string = ", ".join(col_names)
    val_string = ("%s, " * n_cols)[:-2] # remove last two characters ", "

    insert_definition = extended_insert.format(table_name, col_string, val_string)
    
    return (col_names, insert_definition)

## _________________________________________________________________________________
## main functions

def insert_table(cursor, reader, table_name, table_specs):
    """Bulk / chunkwise insert a pandas reader object as one sql table."""

    ## create required sql statements and initialize table
    table_definition = define_table(table_name, table_specs)
    col_names, insert_definition = define_insert(table_name, table_specs)
    cursor.execute(table_definition)

    ## insert table chunkwise from pandas reader object
    for chunk in tqdm(reader):
        chunk = chunk.loc[:, col_names] #reduce chunk to specified columns
        values = chunk.to_records(index=False).tolist()
        cursor.executemany(insert_definition, values)

def insert_multiple_tables(cursor, insert_instructions):
    """Uses "insert_table" for multiple input tables, iterating
    over "insert_instructions" that are structured like this:
    
    insert_instructions = {
        "table_a": (path_to_a, reader_function, specs.table_a),
        "table_b": (path_to_b, reader_function, specs.table_b)
    }

    """
    for table_name, instructions in insert_instructions.items():
        fpath, reader_function, table_specs = instructions
        reader = reader_function(fpath)
        insert_table(cursor, reader, table_name, table_specs)

