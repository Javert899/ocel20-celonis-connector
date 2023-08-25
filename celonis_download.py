import pycelonis
import pandas as pd
from typing import Dict, Optional, Any
from io import BytesIO
from pycelonis.pql.pql import PQL, PQLColumn
from pycelonis.service.integration.service import ExportType
from pycelonis.ems.data_integration.data_pool import DataModel
import pm4py
from pm4py.objects.ocel.obj import OCEL

celonis_url = "CELONIS URL"
celonis_token = "CELONIS TOKEN"
celonis_key_type = "USER_KEY" # or USER_KEY if it does not work

data_pool_name = "200 - OCEL import"
data_model_name = "trial data model"


def download_all_tables(data_model: DataModel, parameters: Optional[Dict[Any, Any]] = None) -> Dict[str, pd.DataFrame]:
    """
    Retrieves the tables of a data model in Celonis
    Parameters
    --------------
    data_model
        Celonis data model
    Returns
    -------------
    dct
        Dictionary associating each name to a dataframe extracted from the data model
    """
    if parameters is None:
        parameters = {}

    dct = {}
    tables = data_model.get_tables()
    for table in tables:
        print("downloading", table.name)
        table2 = data_model.get_table(table.id)
        columns = table2.get_columns()
        buffer = BytesIO()
        excluded_columns = set()
        for i in range(2):
            try:
                pql = PQL()
                for col in columns:
                    if not col.name.startswith("_"):
                        fname = "\"" + table2.name + "\".\"" + col.name + "\""
                        if fname not in excluded_columns:
                            pql.add(PQLColumn(query=fname, name=col.name))
                dexp = data_model.create_data_export(query=pql, export_type=ExportType.PARQUET)
                dexp.wait_for_execution()
                chunks = dexp.get_chunks()
                for chunk in chunks:
                    buffer.write(chunk.read())
                df = pd.read_parquet(buffer)
                dct[table.name] = df
                break
            except Exception as e:
                e = str(e)
                cols = e.split(" is missing")
                for c in cols:
                    c = c.split("Column ")[-1]
                    excluded_columns.add(c)
    return dct


celonis = pycelonis.get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
data_integration = celonis.data_integration
data_pool = data_integration.get_data_pools().find(data_pool_name)
data_model = data_pool.get_data_models().find(data_model_name)
tables = data_model.get_tables()
foreign_keys = data_model.get_foreign_keys()
tables_dict = {}
for tab in tables:
    tables_dict[tab.id] = tab.name
fk_dict = {}
for fk in foreign_keys:
    source_table_name = tables_dict[fk.source_table_id]
    target_table_name = tables_dict[fk.target_table_id]
    if source_table_name not in fk_dict:
        fk_dict[source_table_name] = []
    if target_table_name not in fk_dict:
        fk_dict[target_table_name] = []
    fk_dict[source_table_name].append((target_table_name, fk.columns[0].source_column_name, fk.columns[0].target_column_name))
    fk_dict[target_table_name].append((source_table_name, fk.columns[0].target_column_name, fk.columns[0].source_column_name))
dataframes = download_all_tables(data_model)

events = []
objects = []
relations = []
o2o = []

for tab, dataframe in dataframes.items():
    if tab.startswith("e_"):
        prefix = tab.split("e_")[1].split("_")[0]
        activity = tab.split("e_"+prefix+"_")[1]
        id_column = fk_dict[tab][0][1]
        timestamp_column = [x for x in dataframe.columns if "time" in x.lower() or "date" in x.lower()][0]
        dataframe = dataframe.rename(columns={timestamp_column: "ocel:timestamp", id_column: "ocel:eid"})
        dataframe["ocel:activity"] = activity
        events.append(dataframe)
    elif tab.startswith("o_"):
        prefix = tab.split("o_")[1].split("_")[0]
        object_type = tab.split("o_"+prefix+"_")[1]
        id_column = fk_dict[tab][0][1]
        dataframe = dataframe.rename(columns={id_column: "ocel:oid"})
        dataframe["ocel:type"] = object_type
        objects.append(dataframe)
    elif tab.startswith("r_e_"):
        prefix = tab.split("r_e_")[1].split("_")[0]
        fks = fk_dict[tab]
        event_table = None
        event_id = None
        object_table = None
        object_id = None
        for fk in fks:
            if fk[0].startswith("e_"):
                event_table = fk[0]
                event_id = fk[1]
            elif fk[0].startswith("o_"):
                object_table = fk[0]
                object_id = fk[1]
        activity = event_table.split("e_"+prefix+"_")[1]
        object_type = object_table.split("o_"+prefix+"_")[1]
        columns = {event_id: "ocel:eid", object_id: "ocel:oid"}
        qualifiers = [x for x in dataframe.columns if "qualifier" in x.lower()]
        if qualifiers:
            columns[qualifiers[0]] = "ocel:qualifier"
        dataframe = dataframe.rename(columns=columns)
        dataframe["ocel:activity"] = activity
        dataframe["ocel:type"] = object_type
        relations.append(dataframe)
    elif tab.startswith("r_o_"):
        prefix = tab.split("r_o_")[1].split("_")[0]
        fks = fk_dict[tab]
        source_object_table = fks[0][0]
        source_object_id = fks[0][1]
        target_object_table = fks[1][0]
        target_object_id = fks[1][1]
        source_object_type = source_object_table.split("o_"+prefix+"_")[1]
        target_object_type = target_object_table.split("o_"+prefix+"_")[1]
        columns = {source_object_id: "ocel:oid", target_object_id: "ocel:oid_2"}
        qualifiers = [x for x in dataframe.columns if "qualifier" in x.lower()]
        if qualifiers:
            columns[qualifiers[0]] = "ocel:qualifier"
        dataframe = dataframe.rename(columns=columns)
        o2o.append(dataframe)

events = pd.concat(events)
objects = pd.concat(objects)
relations = pd.concat(relations)
if o2o:
    o2o = pd.concat(o2o)
else:
    o2o = None

ocel = OCEL(events=events, objects=objects, relations=relations, o2o=o2o)
pm4py.write_ocel(ocel, "prova.jsonocel")
pm4py.write_ocel2(ocel, "prova.xml")
