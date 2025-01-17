# uploader.py

import os
from pycelonis import get_celonis
from pycelonis.pql.pql import PQL, PQLColumn

def upload_to_celonis(object_dataframes, event_dataframes, relationship_dataframes,
                      celonis_url, celonis_token, celonis_key_type, data_pool_name,
                      data_model_name):
    # Connect to Celonis
    celonis = get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
    data_integration = celonis.data_integration

    # Get the Data Pool
    data_pool = data_integration.get_data_pools().find(data_pool_name)
    if data_pool is None:
        raise ValueError(f"Data Pool '{data_pool_name}' not found.")

    # Lists to collect SQL statements
    object_sql_statements = []
    event_sql_statements = []
    relationship_sql_statements = []
    event_related_objects = {}  # To store event types and their related object types (with exactly one related object)

    # Dictionary to keep track of table names and their IDs in Data Model
    table_name_to_id = {}

    # Upload Object Tables
    print("Uploading Object Tables...\n")
    for name, df in object_dataframes.items():
        # Table name per new naming convention
        table_name = f"o_custom_{name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM "{table_name}";'
        object_sql_statements.append(sql)
        print()

    # Upload Event Tables
    print("Uploading Event Tables...\n")
    for name, df in event_dataframes.items():
        # Table name per new naming convention
        table_name = f"e_custom_{name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM "{table_name}";'
        event_sql_statements.append(sql)
        print()

        # Find object types with exactly one related object (columns ending with '_Id')
        object_columns = [col for col in df.columns if col.endswith('_Id')]
        if object_columns:
            event_related_objects[name] = [col[:-3] for col in object_columns]  # Remove '_Id' suffix

    # Upload Relationship Tables
    print("Uploading Relationship Tables...\n")
    for key, df in relationship_dataframes.items():
        evt_name, obj_name = key
        # Table name per new naming convention
        table_name = f"r_e_{evt_name}__{obj_name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM "{table_name}";'
        relationship_sql_statements.append(sql)
        print()

    # Create Data Model
    print("Creating Data Model...\n")
    data_model = data_pool.create_data_model(data_model_name)
    print(f"Data Model '{data_model_name}' created.")

    # Add tables to Data Model and save table IDs
    print("Adding tables to Data Model and saving table IDs...\n")
    all_table_names = []
    # Collect all table names
    for name in object_dataframes.keys():
        table_name = f"o_custom_{name}"
        all_table_names.append(table_name)
    for name in event_dataframes.keys():
        table_name = f"e_custom_{name}"
        all_table_names.append(table_name)
    for key in relationship_dataframes.keys():
        evt_name, obj_name = key
        table_name = f"r_e_{evt_name}__{obj_name}"
        all_table_names.append(table_name)

    for table_name in all_table_names:
        print(f"Adding table '{table_name}' to Data Model...")
        dm_table = data_model.add_table(table_name, table_name)
        table_id = dm_table.id
        table_name_to_id[table_name] = table_id

    # Add foreign keys between relationship tables and event/object tables
    print("Adding Foreign Keys between relationship tables and event/object tables...\n")
    for key in relationship_dataframes.keys():
        evt_name, obj_name = key
        rel_table_name = f"r_e_{evt_name}__{obj_name}"
        event_table_name = f"e_custom_{evt_name}"
        object_table_name = f"o_custom_{obj_name}"
        rel_table_id = table_name_to_id[rel_table_name]
        event_table_id = table_name_to_id[event_table_name]
        object_table_id = table_name_to_id[object_table_name]

        # Add foreign key between relationship table and event table
        fk_event = data_model.create_foreign_key(
            event_table_id,
            rel_table_id,
            [("ID", "EventID")]
        )
        print(f"Foreign key added between '{rel_table_name}' and '{event_table_name}' on ('EventID' -> 'ID').")

        # Add foreign key between relationship table and object table
        fk_object = data_model.create_foreign_key(
            object_table_id,
            rel_table_id,
            [("ID", "ID")]
        )
        print(f"Foreign key added between '{rel_table_name}' and '{object_table_name}' on ('ID' -> 'ID').")

    # Add foreign keys between event tables and object tables based on direct relationships
    print("Adding Foreign Keys between event tables and object tables...\n")
    for event_name, df in event_dataframes.items():
        event_table_name = f"e_custom_{event_name}"
        event_table_id = table_name_to_id[event_table_name]
        for object_type in object_dataframes.keys():
            object_table_name = f"o_custom_{object_type}"
            object_table_id = table_name_to_id[object_table_name]
            # Check if event table has a column corresponding to the object type (or object type + '_Id')
            possible_column_names = [object_type, f"{object_type}_Id"]
            for column_name in possible_column_names:
                if column_name in df.columns:
                    # Add foreign key between event table and object table
                    fk_direct = data_model.create_foreign_key(
                        event_table_id,
                        object_table_id,
                        [(column_name, "ID")]
                    )
                    print(f"Foreign key added between '{event_table_name}' and '{object_table_name}' on ('{column_name}' -> 'ID').")
                    break  # Stop after adding the foreign key for this object type

    # Reload Data Model
    print("Reloading Data Model...\n")
    data_model.reload()
    print("Data Model reloaded successfully.")

    # Print all SQL statements at the end
    print("\nSQL Statements for Object Tables:")
    for sql in object_sql_statements:
        print(sql)
    print()

    print("SQL Statements for Event Tables:")
    for sql in event_sql_statements:
        print(sql)
    print()

    print("SQL Statements for Relationship Tables:")
    for sql in relationship_sql_statements:
        print(sql)
    print()

    # Output the list of object types with exactly one related object for each event type
    if event_related_objects:
        print("Event Types and their related Object Types (with exactly one related object):")
        for evt_name, obj_types in event_related_objects.items():
            obj_list = ', '.join(obj_types)
            print(f"- Event Type '{evt_name}' has exactly one related object of type(s): {obj_list}")
        print()
    else:
        print("No Event Types have exactly one related object.\n")

# Example usage:
if __name__ == "__main__":
    # Import the transform_ocel function from splitter.py
    from splitter import transform_ocel
    import pm4py

    ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")

    # Transform OCEL object
    object_dataframes, event_dataframes, relationship_dataframes = transform_ocel(ocel)

    # Celonis connection details
    celonis_url = "https://academic-fressnapf-rwth.eu-2.celonis.cloud/"  # Replace with your Celonis URL or set as an environment variable
    celonis_token = open("token", "r").read().strip()  # Replace with your API token or set as an environment variable
    celonis_key_type = 'USER_KEY'  # Or 'APP_KEY' depending on your key type
    data_pool_name = 'testpool2'  # Replace with your actual Data Pool name
    data_model_name = 'testdm8'  # Specify the data model name

    # Upload dataframes to Celonis and generate SQL statements
    upload_to_celonis(object_dataframes, event_dataframes, relationship_dataframes,
                      celonis_url, celonis_token, celonis_key_type, data_pool_name,
                      data_model_name)
