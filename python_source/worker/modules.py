import json
from google.cloud.pubsub_v1 import publisher
import rsa
import io
import os
import uuid
import pycurl
import datetime
from botocore.signers import CloudFrontSigner
from google.cloud import logging, secretmanager, pubsub_v1
from google.cloud.exceptions import GoogleCloudError
from google.api_core import retry
import time
from subprocess import Popen, PIPE
import init


logging_client = logging.Client(init.gcp_project)
def logger(**kwargs):
    log_struct = kwargs.get("log_struct")
    severity = kwargs.get("severity")

    logger_name = init.logger_name
    logger = logging_client.logger(logger_name)
    logger.log_struct(
        log_struct,
        severity = severity
    )
    print("Wrote logs to {}.".format(logger.name))


def rsaSigner(message):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": init.url_signer_private_key_secret_id})
    private_key = response.payload.data.decode("UTF-8")
    # private_key = open(PRIVATE_KEY_PATH, 'r').read()
    return rsa.sign(message, rsa.PrivateKey.load_pkcs1(private_key.encode('utf8')), 'SHA-1')  # CloudFront requires SHA-1 hash


def getURLSigner () :
    cf_signer = CloudFrontSigner(init.url_signer_key_pair_id, rsaSigner)
    expires = int(time.time() + init.signed_url_expire_seconds)
    policy = {}
    policy['Statement'] = [{}]
    policy['Statement'][0]['Resource'] = init.cloudfront_url_host_pre + '*'
    policy['Statement'][0]['Condition'] = {}
    policy['Statement'][0]['Condition']['DateLessThan'] = {}
    policy['Statement'][0]['Condition']['DateLessThan']['AWS:EpochTime'] = expires
    policy = json.dumps(policy)
    signed_url = cf_signer.generate_presigned_url('', policy=policy)
    return signed_url


def objectTransferMemory (**kwargs) :
    s3_blob_key = kwargs.get("s3_blob_key")
    gcs_blob_key = kwargs.get("gcs_blob_key")
    blob_url = kwargs.get("blob_url")
    bucket_client = kwargs.get("bucket_client")

    buffer = io.BytesIO()
    headers = io.BytesIO()
    # initialize pycurl
    c = pycurl.Curl()
    c.setopt(c.URL, blob_url)
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(c.WRITEHEADER, headers)
    c.perform()
    res_code = c.getinfo(pycurl.RESPONSE_CODE)
    if c.errstr() != "" :
        c.close()
        return 1, c.errstr()
    if res_code not in init.status_ok :
        c.close()
        return 2, f"return status code {str(res_code)}"

    header_list = headers.getvalue().decode().split('\n')
    # get user metadata
    user_metadata = {}
    for line in header_list :
        if line.find('x-amz-meta-') == 0 :
            value = line.replace('x-amz-meta-', '')
            key = value.split(':')[0]
            value = value.replace(key + ': ', '')
            # value = value.replace(' ', '')
            value = value.replace('\r', '')
            user_metadata[key] = value
    # get system metadata
    system_metadata = {}
    system_metadata_list = ["content-type", "content-length", "cache-control", "content-disposition", "content-encoding", "content-language", "Content-Type", "Content-Length", "Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "etag", "Etag"]
    for metadata in system_metadata_list :
        for line in header_list :
            if line.find(metadata) == 0 :
                value = line.replace(metadata + ': ', '')
                # value = value.replace(' ', '')
                value = value.replace('\r', '')
                system_metadata[metadata] = value
                break

    # intiliaze object handler and configure metadata
    blob = bucket_client.blob(gcs_blob_key)
    if user_metadata != {} :
        blob.metadata = user_metadata
    if 'content-type' in system_metadata :
        blob.content_type = system_metadata['content-type']
    if 'content-encoding' in system_metadata :
        blob.content_encoding = system_metadata['content-encoding']
    if 'content-disposition' in system_metadata :
        blob.content_disposition = system_metadata['content-disposition']
    if 'cache-control' in system_metadata :
        blob.cache_control = system_metadata['cache-control']
    if 'content-language' in system_metadata :
        blob.content_language = system_metadata['content-language'] 
    if 'Content-Type' in system_metadata :
        blob.content_type = system_metadata['Content-Type']
    if 'Content-Encoding' in system_metadata :
        blob.content_encoding = system_metadata['Content-Encoding']
    if 'Content-Disposition' in system_metadata :
        blob.content_disposition = system_metadata['Content-Disposition']
    if 'Cache-Control' in system_metadata :
        blob.cache_control = system_metadata['Cache-Control']
    if 'Content-Language' in system_metadata :
        blob.content_language = system_metadata['Content-Language']
    if 'etag' in system_metadata :
        blob.metadata = {'x-goog-source-etag': system_metadata["etag"]}
    if 'Etag' in system_metadata :
        blob.metadata = {'x-goog-source-etag': system_metadata["Etag"]}
    blob.storage_class = init.storage_class

    try :
        blob.upload_from_file(io.BytesIO(buffer.getvalue()))
    except GoogleCloudError as e :
        return 3, e.message
    
    return 0, "SUCCESS"

def objectTransferDisk (**kwargs) :
    s3_blob_key = kwargs.get("s3_blob_key")
    gcs_blob_key = kwargs.get("gcs_blob_key")
    blob_url = kwargs.get("blob_url")
    bucket_client = kwargs.get("bucket_client")
    
    # generate random file name
    file_path = str(uuid.uuid4())

    # open file handler
    file = open(file_path, "wb+")

    headers = io.BytesIO()
    # initialize pycurl
    c = pycurl.Curl()
    c.setopt(c.URL, blob_url)
    c.setopt(c.WRITEDATA, file)
    c.setopt(c.WRITEHEADER, headers)
    c.perform()
    c.close()
    file.close()

    header_list = headers.getvalue().decode().split('\n')
    # get user metadata
    user_metadata = {}
    for line in header_list :
        if line.find('x-amz-meta-') == 0 :
            value = line.replace('x-amz-meta-', '')
            key = value.split(':')[0]
            value = value.replace(key + ': ', '')
            # value = value.replace(' ', '')
            value = value.replace('\r', '')
            user_metadata[key] = value
    # get system metadata
    system_metadata_list = ["content-type", "content-length", "cache-control", "content-disposition", "content-encoding", "content-language", "Content-Type", "Content-Length", "Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "etag", "Etag"]
    system_metadata = {}
    for metadata in system_metadata_list :
        for line in header_list :
            if line.find(metadata) == 0 :
                value = line.replace(metadata + ': ', '')
                # value = value.replace(' ', '')
                value = value.replace('\r', '')
                system_metadata[metadata] = value
                break

    # intiliaze object handler and configure metadata
    blob = bucket_client.blob(gcs_blob_key)
    if user_metadata != {} :
        blob.metadata = user_metadata
    if 'content-type' in system_metadata :
        blob.content_type = system_metadata['content-type']
    if 'content-encoding' in system_metadata :
        blob.content_encoding = system_metadata['content-encoding']
    if 'content-disposition' in system_metadata :
        blob.content_disposition = system_metadata['content-disposition']
    if 'cache-control' in system_metadata :
        blob.cache_control = system_metadata['cache-control']
    if 'content-language' in system_metadata :
        blob.content_language = system_metadata['content-language'] 
    if 'Content-Type' in system_metadata :
        blob.content_type = system_metadata['Content-Type']
    if 'Content-Encoding' in system_metadata :
        blob.content_encoding = system_metadata['Content-Encoding']
    if 'Content-Disposition' in system_metadata :
        blob.content_disposition = system_metadata['Content-Disposition']
    if 'Cache-Control' in system_metadata :
        blob.cache_control = system_metadata['Cache-Control']
    if 'Content-Language' in system_metadata :
        blob.content_language = system_metadata['Content-Language'] 
    if 'etag' in system_metadata :
        blob.metadata = {'x-goog-source-etag': system_metadata["etag"]}
    if 'Etag' in system_metadata :
        blob.metadata = {'x-goog-source-etag': system_metadata["Etag"]}
    blob.storage_class = init.storage_class

    # upload object
    blob.upload_from_filename(file_path)

    # remove downloaded file
    os.remove(file_path)


def gcsSizeCheck (**kwargs) :
    prefix = f"{kwargs.get('prefix')}"
    storage_client = kwargs.get('storage_client')
    
    # Note: Client.list_blobs requires at least package version 1.17.0.
    # blobs = storage_client.list_blobs(config.GCS_BUCKET, prefix=prefix, delimiter="/")
    blobs = storage_client.list_blobs(init.gcs_bucket, prefix=prefix)
    total_size = 0
    for blob in blobs:
        total_size += blob.size
    return total_size


def s3SizeCheck (**kwargs) :
    prefix = f"{kwargs.get('prefix')}"
    s3_client = kwargs.get("s3_client")

    # print(prefix)
    list_objects = s3_client.list_objects_v2(Bucket=init.s3_bucket, Prefix=prefix)
    next_token = list_objects.get("NextContinuationToken")
    total_size = 0
    if "Contents" in list_objects :
        for content in list_objects["Contents"]:
            if content["Size"] <= 0:
                continue
            # filter code
            if content["StorageClass"] == init.storage_class and content['LastModified'] < init.last_modified_before and content['LastModified'] > init.last_modified_after:
                total_size += content["Size"]
    while next_token is not None:
        list_objects = s3_client.list_objects_v2(
            Bucket=init.s3_bucket, Prefix=prefix, ContinuationToken=next_token
        )
        next_token = list_objects.get("NextContinuationToken")
        if "Contents" in list_objects :
            for content in list_objects["Contents"]:
                if content["Size"] <= 0:
                    continue
                # filter code
                if content["StorageClass"] == init.storage_class and content['LastModified'] < init.last_modified_before and content['LastModified'] > init.last_modified_after:
                    total_size += content["Size"]
    
    return total_size


def subscriber () :
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(init.gcs_bucket, init.pubsub_subscription_id)
    json_message = []
    with subscriber :
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": init.num_messages},
            retry=retry.Retry(deadline=300),
        )
        if not response.received_messages:
            return "NO_MESSAGES"
        ack_ids = []
        for received_message in response.received_messages:
            # print(f"Received: ", received_message.message.data.decode())
            json_message.append(json.loads(received_message.message.data.decode()))
            ack_ids.append(received_message.ack_id)
        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
    
    return json_message


def syncCheck (**kwargs) :
    db_client = kwargs.get("db_client")
    dir_key = kwargs.get("dir_key")
    s3_dir_key = kwargs.get("s3_prefix")
    gcs_dir_key = kwargs.get("gcs_prefix")

    mycursor = db_client.cursor()
    sql = init.start_query
    val = (datetime.datetime.utcnow(), str(dir_key))
    mycursor.execute(sql, val)
    db_client.commit()
    if mycursor.rowcount == 0 :
        return 1, "SYNC_CLASH"

    s3_size = s3SizeCheck (prefix=s3_dir_key, s3_client=kwargs.get("s3_client"))
    gcs_size = gcsSizeCheck (prefix=gcs_dir_key, storage_client=kwargs.get("gcs_client"))

    sql = init.end_query
    val = (s3_size, gcs_size, datetime.datetime.utcnow(), "NO_ERROR", dir_key)
    mycursor.execute(sql, val)
    db_client.commit()

    print('Processed:', dir_key, s3_size, gcs_size)
    return 0, "SUCCESS"


def rsync (**kwargs) :
    db_client = kwargs.get("db_client")
    dir_key = kwargs.get("dir_key")
    s3_dir_key = kwargs.get("s3_prefix")
    gcs_dir_key = kwargs.get("gcs_prefix")

    mycursor = db_client.cursor()

    sql = init.start_query
    val = (datetime.datetime.utcnow(), str(dir_key))
    mycursor.execute(sql, val)
    db_client.commit()
    if mycursor.rowcount == 0 :
        return 1, "SYNC_CLASH"
        
    s3_size = s3SizeCheck (prefix=s3_dir_key, s3_client=kwargs.get("s3_client"))
    gcs_size = gcsSizeCheck (prefix=gcs_dir_key, storage_client=kwargs.get("gcs_client"))

    if s3_size != gcs_size :
        
        s3_url  = "s3://" + init.s3_bucket + s3_dir_key
        gcs_url = "gs://" + init.gcs_bucket + gcs_dir_key
        command = "gsutil -m rsync -r " + s3_url + " " + gcs_url
        print(command)
        p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = p.communicate()
        # utf_error = stderr.decode("utf-8")
        # print(utf_error)
        utf_output = stdout.decode("utf-8")
        # print(utf_output)
        if utf_output != "" :
            return 2, utf_output
        gcs_size = gcsSizeCheck (prefix=gcs_dir_key, storage_client=kwargs.get("gcs_client"))

    sql = init.end_query
    val = (s3_size, gcs_size, datetime.datetime.utcnow(), "NO_ERROR", dir_key)
    mycursor.execute(sql, val)
    db_client.commit()

    print('Processed:', dir_key, s3_size, gcs_size)
    return 0, "SUCCESS"


def publishMessage (**kwargs) :
    publisher = kwargs.get("publisher")
    topic_path = kwargs.get("topic_path")
    data = json.dumps(kwargs.get("message_payload"))
    data = data.encode("utf-8")
    future = publisher.publish(
        topic_path, data
    )
    print(future.result())

def runQuery (**kwargs) :
    mycursor = kwargs.get("db_client").cursor()

    sql = kwargs.get("query")
    mycursor.execute(sql)
    kwargs.get("db_client").commit()