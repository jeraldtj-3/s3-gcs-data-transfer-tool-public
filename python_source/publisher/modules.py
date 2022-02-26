import boto3
import json
from concurrent import futures
from google.cloud import logging, pubsub_v1
import init
from time import sleep

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


def listObjectsBoto3 (**kwargs):
    prefix = kwargs.get("prefix", "")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=init.aws_key_id,
        aws_secret_access_key=init.aws_key_secret,
        region_name=init.aws_region,
    )
    list_objects = s3.list_objects_v2(Bucket=init.s3_bucket, Prefix=prefix)
    next_token = list_objects.get("NextContinuationToken")
    total_size = 0
    total_objects = 0
    if "Contents" in list_objects :
        for content in list_objects["Contents"]:
            if content["Size"] <= 0:
                continue
            # filter code
            temp_dict = {}
            print(content["StorageClass"], content["Key"])
            if content["StorageClass"] == init.storage_class and content["LastModified"].astimezone() < init.last_modified_before and content['LastModified'].astimezone() > init.last_modified_after :
                total_size += content["Size"]/(1024*1024)
                total_objects += 1
                yield content["Key"]
    while next_token is not None:
        list_objects = s3.list_objects_v2(Bucket=init.s3_bucket, Prefix=prefix, ContinuationToken=next_token)
        next_token = list_objects.get("NextContinuationToken")
        if "Contents" in list_objects :
            for content in list_objects["Contents"]:
                if content["Size"] <= 0:
                    continue
                # filter code
                print(content["StorageClass"], content["Key"])
                if content["StorageClass"] == s3.storage_class and content["LastModified"].astimezone() < init.last_modified_before and content['LastModified'].astimezone() > init.last_modified_after :
                    total_size += content["Size"]/(1024*1024)
                    total_objects += 1
                    yield content["Key"]

    print(f"Total objects size: {total_size}, Total object count: {total_objects}")


def batchPublisher (**kwargs) :
    message_source = kwargs.get("message_source", [])

    total_objects = 0
    total_directories = 0

    # Configure the batch to publish as soon as there are max_messages_per_batch messages
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=init.max_messages_per_batch,  # default 100
    )
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(init.gcp_project, init.pubsub_topic_id)
    publish_futures = []

    # Resolve the publish future in a separate thread.
    def callback(future: pubsub_v1.publisher.futures.Future) -> None:
        message_id = future.result()
        print("Message ID:", message_id)

    total_directories += 1
    total_dir_objects = 0
    single_batch = []
    for message in message_source :
        total_objects += 1
        total_dir_objects += 1
        print("Published:", message)
        single_batch.append(message)
        if len(single_batch) >= init.num_single_message :
            data = json.dumps(single_batch)
            data = data.encode("utf-8")
            publish_future = publisher.publish(topic_path, data)
            # Non-blocking. Allow the publisher client to batch multiple messages.
            publish_future.add_done_callback(callback)
            publish_futures.append(publish_future)
            single_batch = []
    if len(single_batch) > 0:
        data = json.dumps(single_batch)
        data = data.encode("utf-8")
        publish_future = publisher.publish(topic_path, data)
        # Non-blocking. Allow the publisher client to batch multiple messages.
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published messages with batch settings to {topic_path}.")
    print("Total directories: ", total_directories, "Total objects: ", total_objects)


def queryMySQL (**kwargs) :
    db_client = kwargs.get("db_client")
    mycursor = db_client.cursor()

    sql = init.pull_query
    mycursor.execute(sql)
    result = mycursor.fetchall()

    if result is None :
        yield "EMPTY_LIST"
    if len(result) == 0 :
        yield "EMPTY_LIST"

    sql = init.update_query
    val = result
    # mycursor.execute(sql)
    mycursor.executemany(sql, val)
    db_client.commit()
    # print(mycursor.rowcount)

    # temp_list = []
    for x in result:
        yield x[0]
        # temp_list.append(x[0])
