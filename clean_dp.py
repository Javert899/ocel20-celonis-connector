from pycelonis import get_celonis

celonis_url = "https://academic-fressnapf-rwth.eu-2.celonis.cloud/"  # Replace with your Celonis URL or set as an environment variable
celonis_token = open("token", "r").read().strip()  # Replace with your API token or set as an environment variable
celonis_key_type = 'USER_KEY'  # Or 'APP_KEY' depending on your key type
data_pool_name = 'OCPM Data Pool'  # Replace with your actual Data Pool name

celonis = get_celonis(celonis_url, api_token=celonis_token, key_type=celonis_key_type)
data_integration = celonis.data_integration

data_pool = data_integration.get_data_pools().find(data_pool_name)

data_models = list(data_pool.get_data_models())

for dp in data_models:
    print("deleting", dp.id, dp.name)
    dp.delete()

tables = list(data_pool.get_tables())

statements = []
for tab in tables:
    if tab.name.startswith("c_") or tab.name.startswith("e_custom") or tab.name.startswith("o_custom") or tab.name.startswith("r_"):
        statements.append("DROP TABLE \""+tab.name+"\";")
        #tab.delete()

print("\n".join(statements))
