import pycelonis
import pm4py

celonis_url = "CELONIS URL"
celonis_token = "CELONIS TOKEN"
celonis_key_type = "USER_KEY" # or USER_KEY if it does not work

data_pool_name = "200 - OCEL import"
data_model_name = "trial data model"
namespace = "custom"

celonis = pycelonis.get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
data_integration = celonis.data_integration
data_pool = data_integration.get_data_pools().find(data_pool_name)
try:
    data_model = data_pool.get_data_models().find(data_model_name)
    data_model.delete()
except:
    pass
data_model = data_pool.create_data_model(data_model_name)


ocel = pm4py.read_ocel2("tests/input_data/ocel/ocel20_example.xmlocel")

#ocel = pm4py.filter_ocel_object_types(ocel, ["Purchase Order", "Invoice"])
#ocel = pm4py.filter_ocel_event_attribute(ocel, "ocel:activity", ["Insert Invoice"])
print(ocel)

from pm4py.objects.ocel.util import ocel_to_dict_types_rel, ocel_type_renaming

ocel = ocel_type_renaming.remove_spaces_non_alphanumeric_characters_from_types(ocel)

dct = ocel_to_dict_types_rel.apply(ocel)

tables_dict = {}

for name0, df in dct["ev_types"].items():
    event_attributes = [x for x in df.columns if not x.startswith("ocel:")]

    df.rename(columns={"ocel:eid": "ID", "ocel:activity": "Type", "ocel:timestamp": "Time"}, inplace=True)

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

for name0, df in dct["obj_types"].items():
    object_attributes = [x for x in df.columns if not x.startswith("ocel:")]

    df.rename(columns={"ocel:oid": "ID", "ocel:type": "Type"}, inplace=True)

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

for name0, df in dct["o2o"].items():
    df.rename(columns={"ocel:oid": "SourceObjectID", "ocel:type": "SourceObjectType", "ocel:oid_2": "TargetObjectID", "ocel:type_2": "TargetObjectType", "ocel:qualifier": "Qualifier"}, inplace=True)
    name = "r_o_"+namespace+"_"+name0[0]+"_"+name0[1]
    try:
        data_pool.create_table(df, name)
    except:
        data_pool.create_table(df, name, force=True, drop_if_exists=True)
    tab = data_model.add_table(name, name)
    tables_dict[name] = tab.id
    print("inserted "+name)
    data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+name0[0]], tables_dict[name], [("ID", "SourceObjectID")])
    data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+name0[1]], tables_dict[name], [("ID", "TargetObjectID")])
    print("created foreign keys for "+name)

for name0, df in dct["e2o"].items():
    df.rename(columns={"ocel:eid": "EventID", "ocel:oid": "ObjectID", "ocel:activity": "EventType", "ocel:type": "ObjectType"}, inplace=True)
    name = "r_e_"+namespace+"_"+name0[0]+"_"+name0[1]
    try:
        data_pool.create_table(df, name)
    except:
        data_pool.create_table(df, name, force=True, drop_if_exists=True)
    tab = data_model.add_table(name, name)
    tables_dict[name] = tab.id
    print("inserted "+name)
    data_model.create_foreign_key(tables_dict["e_"+namespace+"_"+name0[0]], tables_dict[name], [("ID", "EventID")])
    data_model.create_foreign_key(tables_dict["o_"+namespace+"_"+name0[1]], tables_dict[name], [("ID", "ObjectID")])
    print("created foreign keys for "+name)

data_model.reload()
