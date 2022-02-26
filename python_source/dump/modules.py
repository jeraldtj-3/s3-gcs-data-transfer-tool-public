from google.cloud import logging
import boto3
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


def filterDirList () :
    start_after = init.start_after
    dump_file = init.dump_file

    with open(dump_file, "r") as f :
        dir_list = f.readlines()

    if start_after is None :
        for line in dir_list :
            line = line.replace("PRE", "")
            line = line.replace("\r", "")
            line = line.replace("\n", "")
            line = line.replace("\t", "")
            line = line.replace(" ", "")
            if line.find("archive-") == 0 :
                pass
            else :
                yield line

    if start_after is not None :
        ans = False
        for line in dir_list :
            line = line.replace("PRE", "")
            line = line.replace("\r", "")
            line = line.replace("\n", "")
            line = line.replace("\t", "")
            line = line.replace(" ", "")
            if line.find("archive-") == 0 :
                pass
            else :
                if not ans :
                    if line == start_after :
                        ans = True
                if ans :
                    # print(line)
                    yield line


def uploadDumpToMySQL (**kwargs) :
    dump_list = kwargs.get("dump_list", [])
    db_client = kwargs.get("db_client")

    mycursor = db_client.cursor()
    count = 0
    for dir_key in dump_list :
        sql = f"""SELECT dir_key FROM {init.mysql_table} WHERE dir_key = '{dir_key}'"""
        mycursor.execute(sql)
        result = mycursor.fetchall()

        if len(result) > 0 :
            # print(result)
            continue

        count += 1
        sql = init.insert_query
        val = (dir_key)
        print("insert:", dir_key)
        mycursor.execute(sql, val)
        if count > 100 :
            db_client.commit()
            print(mycursor.rowcount, "records inserted.")
            count = 0

    db_client.commit()
    print(mycursor.rowcount, "records inserted.")


def getDirListBoto3 () :
    s3 = boto3.client(
        "s3",
        aws_access_key_id=init.aws_key_id,
        aws_secret_access_key=init.aws_key_secret,
        region_name=init.aws_region,
    )
    list_objects = s3.list_objects_v2(Bucket=init.s3_bucket, Prefix=init.s3_root_prefix, Delimiter='/')
    next_token = list_objects.get("NextContinuationToken")
    total_dir_count = 0
    for content in list_objects.get('CommonPrefixes', []):
        total_dir_count += 1
        yield content.get('Prefix')
    while next_token is not None:
        list_objects = s3.list_objects_v2(Bucket=init.s3_bucket, Prefix=init.s3_root_prefix, ContinuationToken=next_token, Delimiter='/')
        next_token = list_objects.get("NextContinuationToken")
        for content in list_objects.get('CommonPrefixes', []):
            total_dir_count += 1
            yield content.get('Prefix')
    print ("Total listed directories:", str(total_dir_count))

