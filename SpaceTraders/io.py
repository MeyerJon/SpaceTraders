"""
    SpaceTraders - IO submodule
    Provides functionality for handling persistent data.

    Currently implemented using SQLite3
"""
import pandas as pd
import sqlite3, json, time, traceback

### GLOBALS ###

DATA_FOLDER = './data'
DB_PATH     = f'{DATA_FOLDER}/STDB.db'

def __init_db_conn(path=None):
    path       = path or DB_PATH
    DB_CONN   = sqlite3.connect(path)
    DB_CONN.execute('PRAGMA journal_mode=WAL;') #Use Write-Ahead-Logging to smoothen out some concurrency issues
    return DB_CONN

def _DB_CONN(path=None):
    return __init_db_conn()


### WRITING ###

def _quoted_value(val):
    """ Returns v with appropriate quotation (for use in select statements, where clauses...) """
    v = str(val)
    if isinstance(val, str):
        v = f'"{v}"'
    return v

def _table_exists(table : str):
    with _DB_CONN() as conn:
        return len(conn.execute(f'SELECT name FROM sqlite_master WHERE name="{table}";').fetchall()) > 0

def _initiate_table_from_dict(table : str, data : dict):
    """ Creates a table from a dict if it doesn't exist. """
    q = f"CREATE TABLE IF NOT EXISTS '{table}' AS\nSELECT\n"
    for k_ix, k in enumerate(data.keys()):
        q += f'{_quoted_value(data[k])} as {k}'
        if k_ix < len(data)-1: q += ",\n"

    try:
        with _DB_CONN() as conn:
            conn.execute(q)
    except Exception as e:
        print(f"[ERROR] Exception while initialising table {table}:")
        print(e)
        return False
    
    return True

def write_rows(table : str, data : list, mode='append', key : list = None):
    """ Write row to table. 
        Parameters:
            - table : table name
            - data  : list<dict> containing row data (col: val)
            - mode  : ('append', 'update'). Whether to append row or overwrite existing row. Must specify keys to update.
            - key   : if mode='update', key columns must be provided
    """
    # Sanity check: if data is a dict, wrap it in a list and consider it a single record
    if isinstance(data, dict):
        data = [data]

    # Use a separate connection that properly opens & closes for this transaction
    query = ""
    with _DB_CONN() as conn:
        try:
            if not _table_exists(table):
                _initiate_table_from_dict(table, data[0]) # This will create the table directly from the first row
                mode = "update" # Avoid duplicating this first row

            # Otherwise, an insert (& optional update) is needed
            if mode == 'update':
                # First drop existing rows
                query += f"DELETE FROM '{table}' WHERE "
                for k_ix, k in enumerate(key):
                    query += f"{k} = :{k}"
                    if k_ix < len(key)-1: query += " AND "
                query += ";"
                conn.executemany(query, data)
            
            query = f"INSERT INTO '{table}' "
            query += f"({', '.join([f'{k}' for k in data[0].keys()])}) "
            query += f"VALUES ({', '.join([f':{k}' for k in data[0].keys()])})"
            conn.executemany(query, data)
            conn.commit()

        except Exception as e:
            conn.rollback()
            log_exception(e)

            if 'syntax error' in str(e):
                print("[ERROR] Syntax error while writing row. Query:\n", query)
            else:
                print("[ERROR] Exception while writing row:")
                print(e)
            
            return False
    
    return True


def write_df(table : str, df : pd.DataFrame, mode='append', key : list = None):
    """ Write dataframe to table.
        Parameters:
            - table : table name
            - data  : dict of row data (col: val)
            - mode  : ('append', 'update'). Whether to append row or overwrite existing row. Must specify keys.
            - key   : if mode='update', key columns must be provided
    """
    try:
        with _DB_CONN() as conn:
            if mode == 'append':
                df.to_sql(table, conn, if_exists='append', index=False)
            elif mode == 'update':
                # First delete all rows for given key
                rows_to_del = df[key].drop_duplicates().to_dict(orient="records")
                # First delete existing records for key
                q = f"DELETE FROM '{table}'\nWHERE\n"
                for ix, k in enumerate(key):
                    q += f"{k} = :{k}"
                    if ix < len(key)-1:
                        q += " AND "
                try:
                    conn.executemany(q, rows_to_del)
                except sqlite3.OperationalError as e:
                    if 'no such table' in str(e):
                        pass # This is fine, since the table will be created in the insert below
                    else:
                        raise e
                # Then append dataframe to table
                serialize_nested_columns(df).to_sql(table, conn, if_exists='append', index=False)
    except Exception as e:
        print("[ERROR] Exception while writing dataframe:")
        print(e)
        log_exception(e)
        return False
    
    return True

def write_data(table : str, data, **kwargs):
    """ Writes provided data to a table.
        Parameters:
            - table : table name
            - data  : dict of row data (col: val)
            - mode  : ('append', 'update'). Whether to append row or overwrite existing row. Default = 'append'
            - key   : if mode='update', list of key columns must be provided
    """

    # Preconditions
    if 'mode' in kwargs and 'key' not in kwargs:
        print(f"[ERROR] Can't update table {table}; no key columns specified.")
        return False

    # Write with retries
    max_retries = 3
    retries     = 0
    backoff_s   = 0.5
    success = False
    while (not success) and (retries < max_retries):
        retries += 1

        try:
            if isinstance(data, (dict, list)):
                success = write_rows(table, data, **kwargs)
            elif isinstance(data, pd.DataFrame):
                success = write_df(table, data, **kwargs)
        except Exception as e:
            print(f"[ERROR] Uncaught exception while writing data to {table}:")
            print(e)
            log_exception(e)

        if not success:
            # Back off before retrying
            time.sleep(backoff_s * retries)
        
    return success

def update_records_custom(query : str):
    """ Executes a custom update query. Returns True if successfully executed. """
    if not query.startswith('UPDATE'):
        print("[ERROR] Failed to update table; not a valid UPDATE statement:\n", query)
        return False
    
    with _DB_CONN() as conn:
        try:
            conn.execute(query)
        except Exception as e:
            print("[ERROR] Exception during table update.")
            log_exception(e)
            return False

    return True

def update_records(table : str, data : dict | list, key_cols : list):
    """ Updates table based on passed data and keys. """

    if not _table_exists(table):
        print(f"[ERROR] Cannot update {table}. Table does not exist.")
        return False

    data_cols = list()
    if isinstance(data, dict):
        # Data only contains a single record
        data_cols = list(data.keys())
        data = [data] # Function assumes multi-record data, so wrap it in a list
    elif isinstance(data, list) and isinstance(data[0], dict):
        # Data contains list of records
        data_cols = list(data[0].keys())
    else:
        print(f"[ERROR] Cannot update {table}. Incompatible data passed:")
        print("       ", data)
        return False

    try:
        q = f"UPDATE '{table}'\nSET\n"
        for c in data_cols:
            if c in key_cols: continue
            q += f"\t{c} = :{c}\n"
        q += "WHERE 1=1\n"
        for k in key_cols:
            q += f"\tAND {k} = :{k}\n"
    except Exception as e:
        print(f"[ERROR] Exception when updating {table}. Cannot construct query:")
        print(e)
        log_exception(e)
        return False
    
    with _DB_CONN() as conn:
        try:
            conn.executemany(q, data)
        except Exception as e:
            print(f"[ERROR] Exception while updating {table}:")
            print(e)
            log_exception(e)
            print("QUERY\n", q)
            print("DATA\n", data)
            return False

    return True

### READING ###

def read_df(query : str):
    """ Returns the result of the given query as a DataFrame. If unsuccessful, returns False. """
    with _DB_CONN() as conn:
        # Read with retries
        data = False
        max_retries = 3
        retries     = 0
        backoff_s   = 0.5
        while (retries < max_retries):
            retries += 1

            try:
                data = pd.read_sql_query(query, conn)
                return data
            except pd.errors.DatabaseError as e:
                if 'syntax error' in str(e):
                    # Retries make no sense for syntax errors
                    print("[ERROR] Syntax error while reading:")
                    print(query)
                    log_exception(e)
                    return False
                else:
                    raise e
            except Exception as e:
                e_str = str(e)
                if 'database is locked' in e_str or 'database is busy' in e_str:
                    pass # Try again after backing off
                else:
                    print(f"[ERROR] Unhandled exception while reading data ({type(e).__name__}):")
                    print(e)
                    log_exception(e)
                    raise e
            
            # Back off before retrying
            time.sleep(backoff_s * retries)
            
    return data

def read_dict(query : str):
    """ Returns the result of the given query as a dict. If unsuccessful, returns False. """
    data = read_df(query)
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient='records')
    return False

def read_list(query : str, query_params = None):
    """ Returns list of records returned by the given query. Supports optional query parameters. If unsuccessful, returns False. """
    data = False
    with _DB_CONN() as conn:
        if query_params is not None:
            # This needs a separate case because query parameters cannot be passed as None to sqlite
            data = conn.execute(query, query_params).fetchall()
        else:
            data = conn.execute(query).fetchall()
    return data


### ERROR LOGGING ###

def log_exception(e : Exception):
    with open(DATA_FOLDER + '/error-log.txt', 'a') as f:
        f.write(str(e))
        f.write("".join(traceback.format_exception(type(e), e, e.__traceback__)))
        f.write('\n')


### UTILS ###
def parse_nested_obj(obj, obj_name="model"):
    """ Returns DataFrames for the object and its nested objects (op to one layer). """

    # Map out model keys & dependent keys
    model_keys = list()
    dep_keys = list()
    nested_dep_keys = list()
    for k in obj:
        # If the value is an object, this is a single dependent object, which needs a separate model
        if isinstance(obj[k], dict):
            dep_keys.append(k)
            
        elif isinstance(obj[k], list):
        # If the value is a list, then a 'lookup' mapping must be made
            nested_dep_keys.append(k)
        
        else:
            # If the value isn't a container, we can add this pair directly to the model
            model_keys.append(k)

    # Table from model attributes
    df_model = pd.DataFrame([{k:obj[k] for k in model_keys + dep_keys}])
    # Dependent objects
    df_dependents = dict()
    for dep in nested_dep_keys:
        dep_df = pd.DataFrame(obj[dep])
        #df_dependents.append(dep_df)
        df_dependents[dep] = dep_df

    return {obj_name: df_model, **df_dependents}

def serialize_nested_columns(df):
    """ Serialises columns that contain nested Python objects (list, dict, set, tuple) and serialises them to JSON string. Returns serialised DataFrame. """
    df_serialized = df.copy()

    def needs_serialization(series):
        # Check a few non-null samples to see if any are complex objects
        sample = series.dropna().head(10)
        return sample.apply(lambda x: isinstance(x, (list, dict, set, tuple)) or 
                                      (not isinstance(x, (str, int, float, bool)) 
                                       and not pd.isna(x))
                            ).any()

    for col in df_serialized.columns:
        if df_serialized[col].dtype == "object" and needs_serialization(df_serialized[col]):
            df_serialized[col] = df_serialized[col].apply(
                lambda x: json.dumps(x) if pd.notna(x) else None
            )

    return df_serialized