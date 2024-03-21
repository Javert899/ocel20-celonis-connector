import pm4py
import networkx as nx
import re
import uuid
from pm4py.objects.ocel.obj import OCEL
from pm4py.objects.ocel.util import ocel_to_dict_types_rel, ocel_type_renaming
from copy import deepcopy
from typing import Dict, Optional


celonis_url = "CELONIS URL"
celonis_token = "CELONIS_TOKEN"
celonis_key_type = "USER_KEY" # or USER_KEY if it does not work

data_pool_name = "DATA POOL NAME"
data_model_name = "DATA MODEL NAME"

namespace = "custom"

space_name = "SPACE_NAME"
package_name = "PACKAGE_NAME"

coerce_data_types_to_string = False
insert_flattened_table_per_ot = False
insert_knowledge_model = False

recorded = set()

data_pool = None
data_model = None

names_stripper_match = re.compile(r'[^0-9a-zA-Z]+')


def names_stripper(X: str, max_len: int = 100) -> str:
    X = X.split(" ")
    i = 0
    while i < len(X):
        X[i] = X[i].capitalize()
        i = i + 1
    X = "".join(X)
    stru = names_stripper_match.sub('', X).strip()
    if len(stru) > max_len:
        stru = stru[:100]
    return stru


def __rename_types_from_maps(ocel: OCEL, event_types_map: Optional[Dict[str, str]], object_types_map: Optional[Dict[str, str]]) -> OCEL:
    ret_ocel = deepcopy(ocel)
    if event_types_map is not None:
        ret_ocel.events[ocel.event_activity] = ret_ocel.events[ocel.event_activity].map(event_types_map)
        ret_ocel.relations[ocel.event_activity] = ret_ocel.relations[ocel.event_activity].map(event_types_map)
    if object_types_map is not None:
        ret_ocel.objects[ocel.object_type_column] = ret_ocel.objects[ocel.object_type_column].map(object_types_map)
        ret_ocel.relations[ocel.object_type_column] = ret_ocel.relations[ocel.object_type_column].map(object_types_map)
        ret_ocel.object_changes[ocel.object_type_column] = ret_ocel.object_changes[ocel.object_type_column].map(object_types_map)
    return ret_ocel


def remove_spaces_non_alphanumeric_characters_from_types(ocel: OCEL) -> OCEL:
    object_types = ocel.objects[ocel.object_type_column].value_counts().to_dict()
    event_types = ocel.events[ocel.event_activity].value_counts().to_dict()
    object_types_map = {x: names_stripper(x) for x in object_types}
    event_types_map = {x: names_stripper(x) for x in event_types}
    return __rename_types_from_maps(ocel, event_types_map, object_types_map)


def add_e2o(df, et, ot):
    df.rename(columns={"ocel:eid": "EventID", "ocel:oid": "ObjectID", "ocel:activity": "EventType", "ocel:type": "ObjectType"}, inplace=True)
    name = "r_e_"+namespace+"_"+et+"_"+ot
    recorded.add((et, ot))
    try:
        data_pool.create_table(df, name)
    except:
        data_pool.create_table(df, name, force=True, drop_if_exists=True)
    tab = data_model.add_table(name, name)
    tables_dict[name] = tab.id
    print("inserted "+name)
    data_model.create_foreign_key(tables_dict["e_"+namespace+"_"+et], tables_dict[name], [("ID", "EventID")])
    data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+ot], tables_dict[name], [("ID", "ObjectID")])
    print("created foreign keys for "+name)


ocel0 = pm4py.read_ocel2("tests/input_data/ocel/ocel20_example.xmlocel")
#print(ocel0)
#ocel0 = pm4py.filter_ocel_object_types(ocel0, ["Purchase Order", "Invoice"])
#ocel0 = pm4py.filter_ocel_event_attribute(ocel0, "ocel:activity", ["Create Purchase Order"])

#ocel0 = pm4py.read_ocel2("ContainerLogistics (3).xml")

ocel = remove_spaces_non_alphanumeric_characters_from_types(ocel0)
print(ocel)

lead_ot = input("Insert the lead object type -> ")

activities = set(ocel.relations[ocel.relations["ocel:type"] == lead_ot]["ocel:activity"].unique())

ocel = pm4py.filter_ocel_event_attribute(ocel, "ocel:activity", activities)
print(ocel)

if len(ocel.events) == 0 or len(ocel.objects) == 0 or len(ocel.relations) == 0:
    raise Exception("incorrect lead type selected!")

dct = ocel_to_dict_types_rel.apply(ocel)

graph = nx.DiGraph()

all_events = list(dct["ev_types"].items())
all_objects = list(dct["obj_types"].items())
all_e2o = list(dct["e2o"].items())
all_o2o = list(dct["o2o"].items())

all_events = sorted(all_events, key=lambda x: (len(x[1]), x[0]), reverse=True)
all_objects = sorted(all_objects, key=lambda x: (len(x[1]), x[0]), reverse=True)
all_e2o = sorted(all_e2o, key=lambda x: (1 if x[0][1] == lead_ot else 0, len(x[1]), x[0]), reverse=True)
all_o2o = sorted(all_o2o, key=lambda x: (1 if x[0][0] == lead_ot else 0, 1 if x[0][1] == lead_ot else 0, len(x[1]), x[0]), reverse=True)

for name0, df in all_events:
    name = "e_" + namespace + "_" + name0

    graph.add_node(name, original=("event", name0))

for name0, df in all_objects:
    name = "o_"+namespace+"_"+name0

    graph.add_node(name, original=("object", name0))

for name0, df in all_e2o:
    new_graph = graph.copy()

    et = name0[0]
    ot = name0[1]

    name = "r_e_" + namespace + "_" + et + "_" + ot

    new_graph.add_node(name, original=("e2o", et, ot))
    new_graph.add_edge("e_"+namespace+"_"+et, name, edge_type="e2o")
    new_graph.add_edge("o_"+namespace+"_"+ot, name, edge_type="e2o")

    if False:
        try:
            cycle = nx.find_cycle(new_graph)
            #print(cycle)
            #print(name)
        except:
            #traceback.print_exc()
            graph = new_graph
    else:
        graph = new_graph

graph = nx.Graph(graph)

for name0, df in all_o2o:
    new_graph = graph.copy()

    ot1 = name0[0]
    ot2 = name0[1]

    name = "r_o_"+namespace+"_"+ot1+"_"+ot2

    new_graph.add_node(name, original=(ot1, ot2))
    new_graph.add_edge("o_"+namespace+"_"+ot1, name, edge_type="o2o")
    new_graph.add_edge("o_"+namespace+"_"+ot2, name, edge_type="o2o")

    try:
        cycle = nx.find_cycle(new_graph)
        #print(cycle)
        print(name)
    except:
        #traceback.print_exc()
        graph = new_graph

nodes = [graph.nodes[n]["original"] for n in graph.nodes]
allowed_events0 = set(n[1] for n in nodes if n[0] == "event")
allowed_events = set()
for u, v, attributes in graph.edges(data=True):
    source = v.split("_")[-2]
    if source in allowed_events0:
        if attributes["edge_type"] == "e2o":
            allowed_events.add(source)
allowed_objects0 = set(n[1] for n in nodes if n[0] == "object")
allowed_objects = set()
for u, v, attributes in graph.edges(data=True):
    target = v.split("_")[-1]
    if target in allowed_objects0:
        if attributes["edge_type"] == "e2o":
            allowed_objects.add(target)
allowed_e2o = set((n[1], n[2]) for n in nodes if n[0] == "e2o" and n[1] in allowed_events and n[2] in allowed_objects)
allowed_o2o = set((n[1], n[2]) for n in nodes if n[0] == "o2o" and n[1] in allowed_objects and n[2] in allowed_objects)

print(allowed_events)
print(allowed_objects)
print(allowed_e2o)
print(allowed_o2o)

import pycelonis

celonis = pycelonis.get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
data_integration = celonis.data_integration

try:
    data_pool = data_integration.get_data_pools().find(data_pool_name)
except:
    data_pool = data_integration.create_data_pool(data_pool_name)
    pass

try:
    data_model = data_pool.get_data_models().find(data_model_name)
    data_model.delete()
except:
    pass
data_model = data_pool.create_data_model(data_model_name)

tables_dict = {}

for name0, df in all_events:
    if name0 in allowed_events:
        event_attributes = [x for x in df.columns if not x.startswith("ocel:")]

        df.rename(columns={"ocel:eid": "ID", "ocel:activity": "Type", "ocel:timestamp": "Time"}, inplace=True)

        if coerce_data_types_to_string:
            for x in event_attributes:
                df[x] = df[x].astype('string')

        name = "e_"+namespace+"_"+name0
        try:
            data_pool.create_table(df, name)
        except:
            data_pool.create_table(df, name, force=True, drop_if_exists=True)
        tab = data_model.add_table(name, name)
        tables_dict[name] = tab.id
        print("inserted "+name)

for name0, df in all_objects:
    if name0 in allowed_objects:
        object_attributes = [x for x in df.columns if not x.startswith("ocel:")]

        df.rename(columns={"ocel:oid": "ID", "ocel:type": "Type"}, inplace=True)

        if coerce_data_types_to_string:
            for x in object_attributes:
                df[x] = df[x].astype('string')

        name = "o_"+namespace+"_"+name0
        try:
            data_pool.create_table(df, name)
        except:
            data_pool.create_table(df, name, force=True, drop_if_exists=True)
        tab = data_model.add_table(name, name)
        tables_dict[name] = tab.id
        print("inserted "+name)

        if insert_flattened_table_per_ot:
            ocel_flattening = pm4py.ocel_flattening(ocel, name0)
            ocel_flattening.rename(columns={"case:concept:name": "ID", "concept:name": "Type", "time:timestamp": "Time"})

            if coerce_data_types_to_string:
                for x in [y for y in ocel_flattening.columns if not y in {"ID", "Type", "Time"}]:
                    ocel_flattening[x] = ocel_flattening[x].astype('string')

            name = "flattened_"+name0
            try:
                data_pool.create_table(df, name)
            except:
                data_pool.create_table(df, name, force=True, drop_if_exists=True)
            tab = data_model.add_table(name, name)
            tables_dict[name] = tab.id

            print("inserted "+name)
            data_model.create_foreign_key(tables_dict[name], tables_dict["o_"+namespace+"_"+name0], [("ID", "ID")])
            print("created foreign keys for "+name)

for name0, df in all_o2o:
    if name0 in allowed_o2o:
        df.rename(columns={"ocel:oid": "SourceObjectID", "ocel:type": "SourceObjectType", "ocel:oid_2": "TargetObjectID", "ocel:type_2": "TargetObjectType", "ocel:qualifier": "Qualifier"}, inplace=True)

        ot1 = name0[0]
        ot2 = name0[1]

        name = "r_o_"+namespace+"_"+ot1+"_"+ot2
        try:
            data_pool.create_table(df, name)
        except:
            data_pool.create_table(df, name, force=True, drop_if_exists=True)
        tab = data_model.add_table(name, name)
        tables_dict[name] = tab.id
        print("inserted "+name)
        data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+ot1], tables_dict[name], [("ID", "SourceObjectID")])
        data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+ot2], tables_dict[name], [("ID", "TargetObjectID")])
        print("created foreign keys for "+name)

last_df = None
for name0, df in all_e2o:
    if name0 in allowed_e2o:
        last_df = df
        add_e2o(df, name0[0], name0[1])

if insert_flattened_table_per_ot:
    for ot in allowed_objects:
        data_model.create_process_configuration(activity_table_id=tables_dict["flattened_"+ot], case_table_id=tables_dict["o_"+namespace+"_"+ot], case_id_column="ID", activity_column="Type", timestamp_column="Time")
        print("created process configuration for "+ot)

data_model.reload()

if insert_knowledge_model:
    variable_id = str(uuid.uuid4())
    variable_id = "var"+re.sub(r'[^\w\s]', '', variable_id)

    know_model_id = str(uuid.uuid4())
    know_model_id = "know"+re.sub(r'[^\w\s]', '', know_model_id)

    try:
        space = celonis.studio.get_spaces().find(space_name)
    except:
        space = celonis.studio.create_space(space_name)

    try:
        package = space.get_packages().find(package_name)
        package.delete()
    except:
        pass

    package = space.create_package(package_name)

    data_model_variable = package.create_variable(key=variable_id,
                                                  value=data_model.id,
                                                  type_="DATA_MODEL")

    event_logs = [{"id": ot, "displayName": ot, "pql": "PROJECT_ON_OBJECT(\"o_"+namespace+"_"+ot+"\").\"Type\""} for ot in allowed_objects]

    content = {
        "kind" : "BASE",
        "metadata" : {"key":know_model_id, "displayName":"Knowledge Model"},
        "dataModelId" : "${{"+variable_id+"}}",
        "eventLogsMetadata": {
            "eventLogs": event_logs
        }
    }

    knowledge_model = package.create_knowledge_model(content)
