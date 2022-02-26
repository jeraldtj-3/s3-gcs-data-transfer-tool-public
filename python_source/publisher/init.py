import os
import json
from google.cloud import datastore, secretmanager
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("flow")
parser.add_argument("-gp", "--gcp_project", dest="gcp_project", default=None, help="GCP Project ID")
parser.add_argument("-ck", "--config_key", dest="config_key", default=None, help="Datastore Config Key (Required)")
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

# aws common variables
aws_region = entity.get("aws_region")
s3_bucket = entity.get("s3_bucket")
s3_root_prefix = entity.get("s3_root_prefix", "")
storage_class = entity.get("storage_class", "STANDARD")

# listObjectsBoto3
last_modified_before = entity.get("last_modified_before", datetime.datetime.utcnow() + datetime.timedelta(days=1))
last_modified_after = entity.get("last_modified_after", datetime.datetime.min)        

# batchPublisher
pubsub_topic_id = entity.get("pubsub_topic_id", None)
max_messages_per_batch = entity.get("max_messages_per_batch", 10)
num_single_message = entity.get("num_single_message", 10)

# queryMySQL
pull_query = entity.get("pull_query")
update_query = entity.get("update_query")

# secrets
aws_key_id = secrets.get("aws_key_id", None)
aws_key_secret = secrets.get("aws_key_secret", None)
mysql_config = secrets.get("mysql_config")
