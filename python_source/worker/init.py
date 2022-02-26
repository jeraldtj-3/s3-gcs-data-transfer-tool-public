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
gcs_bucket = entity.get("gcs_bucket")
gcs_root_prefix = entity.get("gcs_root_prefix")
mysql_table = entity.get("mysql_table")

# aws common variables
aws_region = entity.get("aws_region")
s3_bucket = entity.get("s3_bucket")
s3_root_prefix = entity.get("s3_root_prefix")
storage_class = entity.get("storage_class", "STANDARD")
cloudfront_url_host_pre = entity.get("cloudfront_url_host_pre", None)
replace_key_parts = entity.get("replace_key_parts", None)

# rsaSigner
url_signer_private_key_secret_id = entity.get("url_signer_private_key_secret_id", None)

# getURLSigner
url_signer_key_pair_id = entity.get("url_signer_key_pair_id", None)
signed_url_expire_seconds = entity.get("signed_url_expire_seconds", 3600*24*7)

# s3SizeCheck
last_modified_before = entity.get("last_modified_before", datetime.datetime.utcnow() + datetime.timedelta(days=1))
last_modified_after = entity.get("last_modified_after", datetime.datetime.min)        

# subscriber
pubsub_subscription_id = entity.get("pubsub_subscription_id")
num_messages = entity.get("num_messages", 1)

# syncCheck, rsync
start_query = entity.get("start_query")
end_query = entity.get("end_query")

# secrets
aws_key_id = secrets.get("aws_key_id", None)
aws_key_secret = secrets.get("aws_key_secret", None)
mysql_config = secrets.get("mysql_config")

# objectTransferMemory
status_ok = entity.get("status_ok", [200])

# publishMessage
error_pubsub_topic_id = entity.get("error_pubsub_topic_id", None)