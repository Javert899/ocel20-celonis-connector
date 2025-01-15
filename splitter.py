import os.path

import pandas as pd
import re

import pm4py
import shutil


def transform_ocel(ocel, custom=False, create_object_relations=False, lead_object_type=None):
    # Function to clean names: strip non-alphanumerical characters and spaces, capitalize first letter of each word
    def clean_name(name):
        name = re.sub(r'[^0-9a-zA-Z ]+', '', name)
        name = ' '.join(word.capitalize() for word in name.split())
        name = name.replace(' ', '')
        return name

    # Prepare collections
    object_dataframes = {}
    event_dataframes = {}
    relationship_dataframes = {}

    # Get unique object types and event types
    object_types = ocel.objects['ocel:type'].unique()
    event_types = ocel.events['ocel:activity'].unique()

    # Process objects for each object type
    for obj_type in object_types:
        # Clean object type name
        df_name = clean_name(obj_type)
        # Filter objects of this type
        obj_df = ocel.objects[ocel.objects['ocel:type'] == obj_type].copy()
        # Remove columns starting with 'ocel:'
        additional_columns = [col for col in obj_df.columns if not col.startswith('ocel:')]
        # Keep columns where at least one object has a non-null value
        cols_with_values = [col for col in additional_columns if obj_df[col].notnull().any()]
        # Select ID column and additional columns
        columns_to_keep = ['ocel:oid'] + cols_with_values
        obj_df = obj_df[columns_to_keep]
        # Rename 'ocel:oid' to 'ID'
        obj_df.rename(columns={'ocel:oid': 'ID'}, inplace=True)
        new_columns = {x: clean_name(x) for x in obj_df.columns}
        new_columns["ID"] = "ID"
        obj_df.rename(columns=new_columns, inplace=True)
        # Add to the collection
        object_dataframes[df_name] = obj_df

    # Process events for each event type
    for evt_type in event_types:
        # Clean event type name
        df_name = clean_name(evt_type)
        # Filter events of this type
        evt_df = ocel.events[ocel.events['ocel:activity'] == evt_type].copy()
        # Remove columns starting with 'ocel:'
        additional_columns = [col for col in evt_df.columns if not col.startswith('ocel:')]
        # Keep columns where at least one event has a non-null value
        cols_with_values = [col for col in additional_columns if evt_df[col].notnull().any()]
        # Select ID column, Time column, and additional columns
        columns_to_keep = ['ocel:eid', 'ocel:timestamp'] + cols_with_values
        evt_df = evt_df[columns_to_keep]
        # Rename 'ocel:eid' to 'ID', 'ocel:timestamp' to 'Time'
        evt_df.rename(columns={'ocel:eid': 'ID', 'ocel:timestamp': 'Time'}, inplace=True)
        new_columns = {x: clean_name(x) for x in evt_df.columns}
        new_columns["ID"] = "ID"
        evt_df.rename(columns=new_columns, inplace=True)
        # Add to the collection
        event_dataframes[df_name] = evt_df

    # Process relationships between events and objects
    relations_df = ocel.relations
    # Get unique pairs of (event type, object type)
    event_object_pairs = relations_df[['ocel:activity', 'ocel:type']].drop_duplicates()
    for idx, row in event_object_pairs.iterrows():
        evt_type = row['ocel:activity']
        obj_type = row['ocel:type']
        # Clean names
        evt_name = clean_name(evt_type)
        obj_name = clean_name(obj_type)

        # Filter relations for this pair
        rel_df = relations_df[(relations_df['ocel:activity'] == evt_type) & (relations_df['ocel:type'] == obj_type)]

        # Check if each event of this event type is related to exactly one object of this object type
        counts = rel_df.groupby('ocel:eid')['ocel:oid'].nunique()
        if counts.eq(1).all():
            # Map from event ID to object ID
            eid_to_oid = rel_df.set_index('ocel:eid')['ocel:oid']
            # Get the event dataframe
            evt_df = event_dataframes[evt_name]
            # Map event IDs to object IDs
            if custom:
                this_c_name = obj_name
            else:
                this_c_name = obj_name
            evt_df[this_c_name] = evt_df['ID'].map(eid_to_oid)
            # Update the event dataframe in the collection
            event_dataframes[evt_name] = evt_df
        else:
            # Create a dataframe for this (event type, object type) pair
            # Columns should be 'ID' (object ID), 'EventID' (event ID)
            pair_df = rel_df[['ocel:oid', 'ocel:eid']].copy()
            if custom:
                pair_df.rename(columns={'ocel:eid': 'ID', 'ocel:oid': obj_name}, inplace=True)
            else:
                pair_df.rename(columns={'ocel:oid': 'ID', 'ocel:eid': 'EventID'}, inplace=True)
            # Store under key (evt_name, obj_name)
            key = (evt_name, obj_name)
            relationship_dataframes[key] = pair_df

    # Process object-to-object relationships if the flag is set
    if create_object_relations and lead_object_type is not None:
        object_relationship_dataframes = {}

        lead_obj_name = clean_name(lead_object_type)

        # Filter relations to get lead object type relations
        lead_relations = ocel.relations[ocel.relations['ocel:type'] == lead_object_type].rename(
            columns={'ocel:oid': 'LeadObjectID'}
        )

        # Filter relations to get other object types
        other_relations = ocel.relations[ocel.relations['ocel:type'] != lead_object_type].rename(
            columns={'ocel:oid': 'OtherObjectID'}
        )

        # Merge on event ID to find object-to-object relations
        merged_relations = pd.merge(
            lead_relations[['ocel:eid', 'LeadObjectID']],
            other_relations[['ocel:eid', 'OtherObjectID', 'ocel:type']],
            on='ocel:eid'
        )


        # For each other object type
        for obj_type in merged_relations['ocel:type'].unique():
            child_obj_name = clean_name(obj_type)

            obj_type_relations = merged_relations[merged_relations['ocel:type'] == obj_type]

            # Check if each child object is related to exactly one lead object
            counts = obj_type_relations.groupby('OtherObjectID')['LeadObjectID'].nunique()
            if counts.eq(1).all():
                # Map from child object ID to parent object ID
                mapping = obj_type_relations[['OtherObjectID', 'LeadObjectID']].drop_duplicates().set_index('OtherObjectID')['LeadObjectID']

                # Get the object dataframe
                obj_df_name = clean_name(obj_type)
                obj_df = object_dataframes[obj_df_name]

                # Add parent object column
                parent_col_name = clean_name(lead_object_type)
                obj_df[parent_col_name] = obj_df['ID'].map(mapping)

                # Update the object dataframe
                object_dataframes[obj_df_name] = obj_df
            else:
                # Create object relationship dataframe
                rel_df = obj_type_relations[['LeadObjectID', 'OtherObjectID']].rename(
                    columns={'LeadObjectID': lead_obj_name, 'OtherObjectID': 'ID'}
                )
                parent_name = clean_name(lead_object_type)
                child_name = clean_name(obj_type)
                key = f"{parent_name}_{child_name}_objrelations"
                object_relationship_dataframes[key] = rel_df
    else:
        object_relationship_dataframes = {}

    return object_dataframes, event_dataframes, relationship_dataframes, object_relationship_dataframes


def dataframe_to_sql(df, output_file):
    sql_statements = []

    for index, row in df.iterrows():
        select_parts = []

        for col_name, value in row.items():
            if pd.isnull(value):
                value_str = 'NULL'
            else:
                if isinstance(value, pd.Timestamp):
                    value_str = f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
                elif isinstance(value, float) or isinstance(value, int):
                    value_str = str(value)
                else:
                    # Escape single quotes in the value
                    value_str = "'" + str(value).replace("'", "\'") + "'"

            select_parts.append(f"{value_str} AS \"{col_name}\"")

        select_statement = "SELECT\n\t" + ",\n\t".join(select_parts) + "\nFROM (SELECT 1) AS dummy\nWHERE 1=1"
        sql_statements.append(select_statement)

    # Combine the SELECT statements using UNION ALL
    full_sql = "\n\nUNION ALL\n\n".join(sql_statements)

    # Write to the output file
    with open(output_file, 'w') as f:
        f.write(full_sql)


if __name__ == "__main__":
    ocel = pm4py.read_ocel2("ContainerLogistics.json")

    # Set the flag and specify the lead object type
    create_object_relations = True
    lead_object_type = 'Container'

    object_dfs, event_dfs, relationship_dfs, object_relationship_dfs = transform_ocel(
        ocel,
        custom=True,
        create_object_relations=create_object_relations,
        lead_object_type=lead_object_type
    )

    target_folder = "target"

    if os.path.exists(target_folder):
        shutil.rmtree(target_folder)
    os.mkdir(target_folder)

    # Export object DataFrames to SQL files
    for ot, objs in object_dfs.items():
        output_file = os.path.join(target_folder, f"{ot}_objects.sql")
        dataframe_to_sql(objs, output_file)
        print(f"Exported {ot} objects to {output_file}")

    # Export event DataFrames to SQL files
    for et, evs in event_dfs.items():
        output_file = os.path.join(target_folder, f"{et}_events.sql")
        dataframe_to_sql(evs, output_file)
        print(f"Exported {et} events to {output_file}")

    # Export relationship DataFrames to SQL files
    for (evt_name, obj_name), rel in relationship_dfs.items():
        table_name = f"{evt_name}_{obj_name}_relations"
        output_file = os.path.join(target_folder, f"{table_name}.sql")
        dataframe_to_sql(rel, output_file)
        print(f"Exported {table_name} relationships to {output_file}")

    # Export object relationship DataFrames to SQL files
    for rel_name, rel_df in object_relationship_dfs.items():
        output_file = os.path.join(target_folder, f"{rel_name}.sql")
        dataframe_to_sql(rel_df, output_file)
        print(f"Exported {rel_name} relationships to {output_file}")
