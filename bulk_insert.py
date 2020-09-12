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

    The referenced column from the parent table has to meet following conditions:
    a) datatype INT
    b) it is defined as Primary Key (ensuring that it's values are unique) 
    c) all distinct values between MIN and MAX are present 
       (such that MAX(Primary Key) = number of rows in table)

    Then integrity checking can be simplified to:
    Check if ALL values lie between MIN and MAX of Primary Key"""
    
    fk_definitions = {k:v for k,v in table_specs.items() if isinstance(v, dict)}
    
    for fk in fk_definitions.values():
        
        fk_col, ref = fk["col"], fk["ref"]
        print("Fast integrity check of FK {} in table {}...".format(fk_col, table_name))

        ## parse referenced table and column to separate variables
        start, end = ref.find("("), ref.find(")")
        ref_tab = ref[:start] #name of referenced table 
        ref_col = ref[start + 1:end] #name of referenced column
        
        ## retrieve Min and Max of referenced column
        cursor.execute("SELECT MIN({1}), MAX({1}) FROM {0}".format(ref_tab, ref_col))
        ref_min, ref_max = cursor.fetchall()[0]

        ## query num of rows in referenced table + datatype and key status of referenced column
        cursor.execute("DESCRIBE {0} {1}".format(ref_tab, ref_col))
        result = cursor.fetchall()[0]
        dtype, key_status = result[1], result[3]

        cursor.execute("SELECT COUNT(*) FROM {0}".format(ref_tab))
        ref_tab_count = cursor.fetchall()[0][0]
        
        ## assert the conditions mentioned in the docstring are met
        error_msg = "Table {0}: {1} is not integer".format(ref_tab, ref_col)
        assert(dtype == "int"), error_msg

        error_msg = "Table {0}: {1} is not a Primary Key".format(ref_tab, ref_col)
        assert(key_status == "PRI"), error_msg

        error_msg = "Table {0}: discrete values between MIN and MAX of {1} are missing".format(ref_tab, ref_col)
        assert (ref_tab_count == ref_max), error_msg

        ## Finally, check integrity of foreign key:
        query = """SELECT COUNT(DISTINCT {0}) FROM {1} 
                WHERE {0} NOT BETWEEN {2} AND {3}"""
        cursor.execute(query.format(fk_col, table_name, ref_min, ref_max))
        n_offlimit = cursor.fetchall()[0][0]
        error_msg = "Table {0}: {1} contains values unmatched by '{2}'"
        error_msg = error_msg.format(table_name, ref_col, ref)
        assert (n_offlimit == 0), error_msg
    
    if fk_definitions.values():
        print("FK integrity check completed")
    else:
        print("No foreign key definitions to check")

## _________________________________________________________________________________
## insert tables to MySQL Server through mysql.connector

def insert_table(cursor, reader, table_name, table_specs, fast_fk_check = False):
    """Bulk / chunkwise insert a pandas reader object as MySQL table.
    Foreign keys are added AFTER table creation and insertion.
    Make sure the parent keys are indexed first (usually as Primary Keys)
    
    With very large tables, integrity checks of foreign keys may take hours!
    Use fast_fk_check = True to use an abbreviated procedure, BUT ONLY IF 
    all referenced columns meet the following conditions:
        a) datatype INT
        b) already defined as Primary Key (ensuring unique values) 
        c) all distinct values between MIN and MAX are present 
           (such that MAX(Primary Key) = number of rows in table)
    Then, and only then, integrity checking can be simplified to:
        Check if all UNIQUE values of the foreign key candidate 
        fall between MIN and MAX of the Primary Key in the parent table.
    """

    ## create required sql statements and initialize table
    table_definition = define_table(table_name, table_specs)
    col_names, insert_definition = define_insert(table_name, table_specs)
    cursor.execute(table_definition)

    ## insert table chunkwise from pandas reader object
    for chunk in tqdm(reader):
        try: 
            chunk = chunk.loc[:, col_names] #reduce chunk to specified columns
        except KeyError:
            raise KeyError("Check if column names match between dataset and table_specs") 
        values = chunk.to_records(index=False).tolist()
        cursor.executemany(insert_definition, values)

    ## add foreign keys if foreign key definitions are provided
    fk_definitions = define_foreign_keys(table_name, table_specs)

    if fast_fk_check:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        check_foreign_key(cursor, table_name, table_specs)

    if fk_definitions:
        print("adding foreign keys...")
        for fk in fk_definitions:
            now = datetime.now().strftime("%H:%M:%S")
            print("Add foreign key at: " + now)
            cursor.execute(fk)
            now = datetime.now().strftime("%H:%M:%S")
            print("Foreign key added at: " + now)
        print("Foreign key constraints are now in place")

    if fast_fk_check:
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")

def insert_multiple_tables(cursor, insert_instructions, fast_fk_check = False):
    """Wrapper for multiple use of  "insert_table".
    Itereates over "insert_instructions" that need to be structured like this:
    Check docstring of "insert_table" for an explanation of
    keyword argument "fast_fk_check".

    insert_instructions = {
        "table_name_a": (path_to_a, reader_function_a, tablespecs_a),
        "table_name_b": (path_to_b, reader_function_b, tablespecs_b)
    }
    """
    for table_name, instructions in insert_instructions.items():
        fpath, reader_function, table_specs = instructions
        reader = reader_function(fpath)
        insert_table(cursor, reader, table_name, table_specs,
                     fast_fk_check = fast_fk_check)

