import init
import boto3
from modules import getURLSigner, subscriber, objectTransferMemory, syncCheck, rsync, publishMessage, logger, runQuery
from google.cloud import storage, pubsub_v1
from time import sleep
import mysql.connector as mysql
import traceback

# Steps
# 1. Get directory list dump using awscli
# 2. Load the dump to MySQL

def cdnTransferEntry () :
    storage_client = storage.Client()
    bucket_client = storage_client.bucket(init.gcs_bucket)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(init.gcp_project, init.error_pubsub_topic_id)

    if init.url_signer_key_pair_id is not None :
        url_signer = getURLSigner ()

    while True :
        json_message = subscriber ()
        if json_message == "NO_MESSAGES" :
            print("Empty queue...")
            sleep(20)
            continue
        
        for message in json_message :
            for object_key in message :
                s3_blob_key = str(object_key)
                gcs_blob_key = f"{init.gcs_root_prefix}{object_key.replace(init.s3_root_prefix, '')}"
                if init.replace_key_parts is not None :
                    for key, value in init.replace_key_parts.items() :
                        object_key = object_key.replace(key, value)
                if init.url_signer_key_pair_id is not None :
                    blob_url = f"{init.cloudfront_url_host_pre}{object_key}{url_signer}"
                try :
                    return_code, return_message = objectTransferMemory (
                        s3_blob_key=s3_blob_key, 
                        gcs_blob_key=gcs_blob_key,
                        blob_url=blob_url, 
                        bucket_client=bucket_client
                    )
                except :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[s3_blob_key]]
                    )
                    temp_dict = {
                        "message": "worker | cdnTransferEntry | objectTransferMemory function error",
                        "error": traceback.format_exc()
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)
                if return_code != 0 :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[s3_blob_key]]
                    )
                    temp_dict = {
                        "message": f"worker | cdnTransferEntry | error code {return_code}",
                        "error": return_message
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)


def syncCheckEntry () :
    storage_client = storage.Client()
    bucket_client = storage_client.bucket(init.gcs_bucket)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(init.gcp_project, init.error_pubsub_topic_id)
    db_client = mysql.connect(**init.mysql_config)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=init.aws_key_id,
        aws_secret_access_key=init.aws_key_secret,
        region_name=init.aws_region,
    )
    while True :
        json_message = subscriber ()
        if json_message == "NO_MESSAGES" :
            print("Empty queue...")
            sleep(20)
            continue
        
        for message in json_message :
            for dir_key in message :
                try :
                    return_code, return_message = syncCheck (
                        s3_prefix=f"{init.s3_root_prefix}{dir_key}",
                        gcs_prefix=f"{init.gcs_root_prefix}{dir_key}",
                        dir_key=dir_key,
                        db_client=db_client,
                        s3_client=s3,
                        gcs_client=bucket_client
                    )
                except :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[dir_key]]
                    )
                    runQuery (
                        db_client=db_client,
                        query=f"""UPDATE MYSQL_TABLE SET sync_check_in_progress = FALSE WHERE dir_key = '{dir_key}'"""
                    )
                    temp_dict = {
                        "message": "worker | syncCheckEntry | syncCheck function error",
                        "error": traceback.format_exc()
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)
                if return_code != 0 :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[dir_key]]
                    )
                    runQuery (
                        db_client=db_client,
                        query=f"""UPDATE MYSQL_TABLE SET sync_check_in_progress = FALSE WHERE dir_key = '{dir_key}'"""
                    )
                    temp_dict = {
                        "message": f"worker | syncCheckEntry | error code {return_code}",
                        "error": return_message
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)


def rsyncEntry () :
    storage_client = storage.Client()
    bucket_client = storage_client.bucket(init.gcs_bucket)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(init.gcp_project, init.error_pubsub_topic_id)
    db_client = mysql.connect(**init.mysql_config)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=init.aws_key_id,
        aws_secret_access_key=init.aws_key_secret,
        region_name=init.aws_region,
    )
    while True :
        json_message = subscriber ()
        if json_message == "NO_MESSAGES" :
            print("Empty queue...")
            sleep(20)
            continue
        
        for message in json_message :
            for dir_key in message :
                try :
                    return_code, return_message = rsync (
                        prefix=f"{init.s3_root_prefix}{dir_key}",
                        db_client=db_client,
                        s3_client=s3,
                        gcs_client=bucket_client
                    )
                except :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[dir_key]]
                    )
                    runQuery (
                        db_client=db_client,
                        query=f"""UPDATE MYSQL_TABLE SET rsync_in_progress = FALSE WHERE dir_key = '{dir_key}'"""
                    )
                    temp_dict = {
                        "message": "worker | rsyncEntry | rsync function error",
                        "error": traceback.format_exc()
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)
                if return_code != 0 :
                    publishMessage (
                        publisher = publisher, topic_path=topic_path,
                        message_payload = [[dir_key]]
                    )
                    runQuery (
                        db_client=db_client,
                        query=f"""UPDATE MYSQL_TABLE SET rsync_in_progress = FALSE WHERE dir_key = '{dir_key}'"""
                    )
                    temp_dict = {
                        "message": f"worker | rsyncEntry | error code {return_code}",
                        "error": return_message
                    }
                    logger(log_struct=temp_dict, severity="ERROR")
                    print(temp_dict)


if __name__ == '__main__' :
    if init.flow == "cdnTransfer" :
        cdnTransferEntry ()
    if init.flow == "syncCheck" :
        syncCheckEntry ()
    if init.flow == "rsync" :
        rsyncEntry ()