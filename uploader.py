# uploader.py

import pycelonis
import os


def upload_to_celonis(object_dataframes, event_dataframes, relationship_dataframes, object_relationship_dataframes,
                      celonis_url, celonis_token, celonis_key_type, data_pool_name):
    # Connect to Celonis
    celonis = pycelonis.get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
    data_integration = celonis.data_integration

    # Get the Data Pool
    data_pool = data_integration.get_data_pools().find(data_pool_name)
    if data_pool is None:
        raise ValueError(f"Data Pool '{data_pool_name}' not found.")

    # Lists to collect SQL statements
    object_sql_statements = []
    event_sql_statements = []
    relationship_sql_statements = []
    object_relationship_sql_statements = []
    event_related_objects = {}  # To store event types and their related object types (with exactly one related object)

    # Upload Object Tables
    print("Uploading Object Tables...\n")
    for name, df in object_dataframes.items():
        table_name = f"TEMP_OBJECT_{name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM {table_name};'
        object_sql_statements.append(sql)
        print()

    # Upload Event Tables
    print("Uploading Event Tables...\n")
    for name, df in event_dataframes.items():
        table_name = f"TEMP_EVT_{name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM {table_name};'
        event_sql_statements.append(sql)
        print()

        # Find object types with exactly one related object (columns ending with '_Id')
        object_columns = [col for col in df.columns if col.endswith('_Id')]
        if object_columns:
            event_related_objects[name] = [col[:-3] for col in object_columns]  # Remove '_Id' suffix

    # Upload Event-Object Relationship Tables
    print("Uploading Event-Object Relationship Tables...\n")
    for key, df in relationship_dataframes.items():
        evt_name, obj_name = key
        table_name = f"TEMP_RELATIONSHIP_{evt_name}_{obj_name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM {table_name};'
        relationship_sql_statements.append(sql)
        print()

    # Upload Object-to-Object Relationship Tables
    print("Uploading Object-to-Object Relationship Tables...\n")
    for rel_name, df in object_relationship_dataframes.items():
        table_name = f"TEMP_OBJ_REL_{rel_name}"
        print(f"Creating table '{table_name}' in Data Pool...")
        data_pool.create_table(df, table_name, force=True, drop_if_exists=True)

        # Generate SQL statement with column names enclosed in double quotes
        columns = ', '.join(f'"{col}"' for col in df.columns)
        sql = f'SELECT {columns} FROM {table_name};'
        object_relationship_sql_statements.append(sql)
        print()

    # Print all SQL statements at the end
    print("\nSQL Statements for Object Tables:")
    for sql in object_sql_statements:
        print(sql)
    print()

    print("SQL Statements for Event Tables:")
    for sql in event_sql_statements:
        print(sql)
    print()

    print("SQL Statements for Event-Object Relationship Tables:")
    for sql in relationship_sql_statements:
        print(sql)
    print()

    print("SQL Statements for Object-to-Object Relationship Tables:")
    for sql in object_relationship_sql_statements:
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
    from splitter import transform_ocel
    import pm4py

    # Read the OCEL log
    #ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")
    ocel = pm4py.read_ocel2("tests/input_data/ocel/ocel20_example.xmlocel")
    ocel = pm4py.filter_ocel_object_types(ocel, ["Purchase Order", "Invoice"])
    ocel = pm4py.filter_ocel_event_attribute(ocel, "ocel:activity", ["Create Purchase Order"])
    ocel = pm4py.read_ocel2("C:/order-management.xml")

    # Set the flag and specify the lead object type
    create_object_relations = True
    lead_object_type = 'orders'

    # Transform OCEL object
    object_dataframes, event_dataframes, relationship_dataframes, object_relationship_dataframes = transform_ocel(
        ocel,
        custom=False,
        create_object_relations=create_object_relations,
        lead_object_type=lead_object_type
    )

    # Celonis connection details
    celonis_url = "https://academic-fressnapf-rwth.eu-2.celonis.cloud/"  # Or replace with your Celonis URL
    celonis_token = open("token", "r").read().strip()  # Or replace with your API token
    celonis_key_type = 'USER_KEY'  # Or 'APP_KEY' depending on your key type
    data_pool_name = 'discardpool'

    # Upload dataframes to Celonis and generate SQL statements
    upload_to_celonis(object_dataframes, event_dataframes, relationship_dataframes, object_relationship_dataframes,
                      celonis_url, celonis_token, celonis_key_type, data_pool_name)
