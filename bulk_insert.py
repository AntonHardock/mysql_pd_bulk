from tqdm import tqdm
from itertools import chain
from datetime import datetime

fk_declaration = " FOREIGN KEY ({0[col]}) REFERENCES {0[ref]}"
fk_name = " CONSTRAINT {0[name]}"
fk_update = " ON UPDATE {0[upd]}"
fk_delete = " ON DELETE {0[del]}"

fk_essential_keys = {"col", "ref"}
fk_errormsg = """Foreign key definition incomplete.
Both keys 'col' and 'ref' have to be specified
in each of the nested foreign key dictionaries."""

## _________________________________________________________________________________
## generate sql statements to create and insert table

def parse_columns(k, v):
    '''Key, Value pairs are interpreted as column_name:datatype (and other Keywords like "PRIMARY KEY").
    If the key is a tuple listign multiple column names, all column names get the same datatype.
    '''
    if type(k) is tuple:
        return ", ".join("{} {}".format(col, v) for col in k)
    else: 
        return "{} {}".format(k, v)

def define_table(table_name, table_specs):
    """Returns CREATE TABLE string"""
    col_definitions = {k:v for k,v in table_specs.items() if not isinstance(v, dict)}
    col_definitions = ", ".join(parse_columns(k, v) for k, v in col_definitions.items())
    table_definition = "CREATE TABLE {} (\n{}\n)".format(table_name, col_definitions)
    return table_definition

def define_insert(table_name, table_specs):
    '''Returns a list of column names AND a corresponding extended INSERT string
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

    insert_definition = "INSERT INTO {} ({}) VALUES ({})".format(table_name, col_string, val_string)
    
    return (col_names, insert_definition)

## _________________________________________________________________________________
## generate sql statements to add foreign keys to existing table

fk_declaration = " FOREIGN KEY ({0[col]}) REFERENCES {0[ref]}"
fk_name = " CONSTRAINT {0[name]}"
fk_update = " ON UPDATE {0[upd]}"
fk_delete = " ON DELETE {0[del]}"

fk_essential_keys = {"col", "ref"}
fk_errormsg = """Foreign key definition incomplete.
Both keys 'col' and 'ref' have to be specified
in each of the nested foreign key dictionaries."""

def parse_foreign_keys(fk_dict):
    """Parses a dictionary with keys defining a MySQL foreign key.
    Expected keys are: 
        name (name of foreign key constraint)
        col (column name of foreign key)
        ref (reference table and column to which the foreign key refers)
        upd (behaviour on update)
        del (behaviour on delete)
    Name, upd and del are optional, like in MySQL
    """

    fk_test = fk_essential_keys.issubset(fk_dict.keys())
    assert (fk_test), fk_errormsg

    fk_statement = fk_declaration.format(fk_dict)
    if "name" in fk_dict:
        fk_statement = fk_name.format(fk_dict) + fk_statement
    if "upd" in fk_dict:
        fk_statement = fk_statement + fk_update.format(fk_dict)
    if "del" in fk_dict:
        fk_statement = fk_statement + fk_delete.format(fk_dict)
    return fk_statement

def define_foreign_keys(table_name, table_specs):
    """Returns a list where each element is a "ADD FOREIGN KEY" string.
    If no foreign keys are defined, an empty list is returned"""
    fk_definitions = {k:v for k,v in table_specs.items() if isinstance(v, dict)}

    if fk_definitions:  #proceed if dict is not empty
        fk_add_statement = "ALTER TABLE {} ADD".format(table_name)
        fk_definitions = [parse_foreign_keys(fk) for fk in fk_definitions.values()]
        fk_definitions = [fk_add_statement + fk for fk in fk_definitions]
        return fk_definitions
    else:
        return list()

def check_foreign_key(cursor, table_name, table_specs):
    """Fast foreign key integrity check after batch upload
       CAUTION: only works for INTEGER columns so far

    The referenced column from the parent table has to meet following conditions:
    a) it is a Primary Key (ensuring that it's values are unique) 
    b) all distinct values between MIN and MAX are present 
       (MAX value of Primary Key = number of rows in parent table)

   If conditions are met, integrity checking works as follows:
    get MIN and MAX from referenced PK
    ensure all distinct values between MIN and MAX are present in PK (COUNT(PK) == MAX(PK))
    finally:
        select unique values of FK column
        check if ALL values lie between MIN and MAX of PK"""
    
    fk_definitions = {k:v for k,v in table_specs.items() if isinstance(v, dict)}
    
    for fk in fk_definitions.values():
        
        fk_column = fk["col"]
        print("Fast integrity check of FK {} in table {}...".format(fk_column, table_name))

        ## parent table and referenced column 
        reference = fk["ref"]
        start = reference.find("(")
        end = reference.find(")")
        parent_table = reference[:start]
        parent_key = reference[start + 1:end]

        ## retrieve Min and Max of referenced PK 
        query_input = [parent_key, parent_table]
        pk_boundaries = "SELECT MIN({0}), MAX({0}) FROM {1}".format(*query_input)
        cursor.execute(pk_boundaries)
        pk_min, pk_max = cursor.fetchall()[0]

        ## assert that all values between Min and Max of referenced PK are present
        pk_count = "SELECT COUNT({}) FROM {}".format(parent_key, parent_table)
        cursor.execute(pk_count)
        pk_count = cursor.fetchall()[0][0]
        error_msg = "In {}, not all values are present between MIN and MAX of {}".format(parent_table, parent_key)
        assert (pk_count == pk_max), error_msg

        ## assert that all unique/distinct FK values are inside PK value range
        query_input = [fk_column, table_name, pk_min, pk_max]
        vals_offlimit = "SELECT COUNT(DISTINCT {0}) FROM {1} WHERE {0} NOT BETWEEN {2} AND {3}".format(*query_input)
        cursor.execute(vals_offlimit)
        vals_offlimit = cursor.fetchall()[0][0]
        error_msg = "Foreign key contains values that do not occur in the referenced column "
        assert (vals_offlimit == 0), error_msg

## _________________________________________________________________________________
## insert tables to MySQL Server through mysql.connector

def insert_table(cursor, reader, table_name, table_specs, fast_fk_integrity_check = False):
    """Bulk / chunkwise insert a pandas reader object as MySQL table."""

    ## create required sql statements and initialize table
    table_definition = define_table(table_name, table_specs)
    col_names, insert_definition = define_insert(table_name, table_specs)
    cursor.execute(table_definition)

    ## insert table chunkwise from pandas reader object
    for chunk in tqdm(reader):
        chunk = chunk.loc[:, col_names] #reduce chunk to specified columns
        values = chunk.to_records(index=False).tolist()
        cursor.executemany(insert_definition, values)

    ## OPTIONAL: fast FK integrity check, disables automatic checks
    if fast_fk_integrity_check:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        check_foreign_key(cursor, table_name, table_specs)

    ## add foreign keys if foreign key definitions are provided
    fk_definitions = define_foreign_keys(table_name, table_specs)
    if fk_definitions:
        print("adding foreign keys...")
        print(datetime.now())
        for fk in fk_definitions: cursor.execute(fk)
        print("foreign keys added")
        print(datetime.now())

    ## After fast FK integrity check: enable automatic FK checks again
    if fast_fk_integrity_check:
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")

def insert_multiple_tables(cursor, insert_instructions):
    """Uses "insert_table" for multiple input tables, iterating
    over "insert_instructions" that are structured like this:
    
    insert_instructions = {
        "table_a": (path_to_a, reader_function_x, tablespecs_a),
        "table_b": (path_to_b, reader_function_y, tablespecs_b)
    }
    """
    for table_name, instructions in insert_instructions.items():
        fpath, reader_function, table_specs = instructions
        reader = reader_function(fpath)
        insert_table(cursor, reader, table_name, table_specs)

