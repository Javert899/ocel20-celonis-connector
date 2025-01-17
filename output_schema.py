import pycelonis
import os

# Celonis connection details
celonis_url = "https://staff-pads.eu-1.celonis.cloud/"  # Or replace with your Celonis URL
celonis_token = open("token", "r").read().strip()  # Or replace with your API token
celonis_key_type = 'USER_KEY'  # Or 'APP_KEY' depending on your key type
data_pool_name = 'OrderManagement DM New'

celonis = pycelonis.get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
data_integration = celonis.data_integration
data_pool = data_integration.get_data_pools().find(data_pool_name)

data_model = data_pool.get_data_models().find("perspective_custom_OrderManagement")

tables_ids = {}

for table in data_model.get_tables():
    tables_ids[table.id] = table.name

for table in data_model.get_tables():
    columns = []
    for col in table.get_columns():
        columns.append(col.name)
    print(table.name, columns)

for fk in data_model.get_foreign_keys():
    print("source_table = ", tables_ids[fk.source_table_id], "target_table = ", tables_ids[fk.target_table_id], "source_column_name = ", fk.columns[0].source_column_name, "target_column_name = ", fk.columns[0].target_column_name)
