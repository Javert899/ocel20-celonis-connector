import pandas as pd
import re

import pm4py


def transform_ocel(ocel):
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
            evt_df[obj_name] = evt_df['ID'].map(eid_to_oid)
            # Update the event dataframe in the collection
            event_dataframes[evt_name] = evt_df
        else:
            # Create a dataframe for this (event type, object type) pair
            # Columns should be 'ID' (object ID), 'EventID' (event ID)
            pair_df = rel_df[['ocel:oid', 'ocel:eid']].copy()
            pair_df.rename(columns={'ocel:oid': 'ID', 'ocel:eid': 'EventID'}, inplace=True)
            # Store under key (evt_name, obj_name)
            key = (evt_name, obj_name)
            relationship_dataframes[key] = pair_df

    return object_dataframes, event_dataframes, relationship_dataframes


if __name__ == "__main__":
    ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")
    object_dfs, event_dfs, relationship_dfs = transform_ocel(ocel)
    print(len(object_dfs))
    print(len(event_dfs))
    print(len(relationship_dfs))

    for ot, objs in object_dfs.items():
        print(ot)
        print(objs)

    for et, evs in event_dfs.items():
        print(et)
        print(evs)

    for rt, rel in relationship_dfs.items():
        print(rt)
        print(rel)
