import os
import json
from google.cloud import datastore, secretmanager
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("flow")
parser.add_argument("-gp", "--gcp_project", dest="gcp_project", help="GCP Project ID (Required)")
parser.add_argument("-ck", "--config_key", dest="config_key", default=None, help="Datastore Config Key (Required)")
parser.add_argument("-sa", "--start_after", dest="start_after", default=None, help="Start After Key (Optional)")
parser.add_argument("-dp", "--dump_file", dest="dump_file", help="Dump filename (Required)")
args = parser.parse_args()

os.environ["GCP_PROJECT"] = args.gcp_project
os.environ["CONFIG_KEY"] = args.config_key

gcp_project = os.environ.get("GCP_PROJECT")
config_key = os.environ.get("CONFIG_KEY")
config_kind = f"{config_key.split('/')[0]}"
config_name = f"{config_key.split('/')[1]}"

datastore_client = datastore.Client(gcp_project)
task_key = datastore_client.key(config_kind, config_name)
entity = datastore_client.get(key=task_key)
# for key, value in entity.items() :
#     os.environ[key] = value

if entity.get("credentials_secret_id") is not None :
    credentials_secret_id = entity.get("credentials_secret_id")
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": credentials_secret_id})
    payload = response.payload.data.decode("UTF-8")
    payload = payload.replace("\'", "\"")
    secrets = json.loads(payload)
    # for key, value in json_payload.items() :
    #     os.environ[key] = value


# gcp common variables
gcp_project = entity.get("gcp_project", None)
logger_name = entity.get("logger_name", "object-transfer-tool")
mysql_table = entity.get("mysql_table")
insert_query = entity.get("insert_query")
mysql_config = secrets.get("mysql_config")

