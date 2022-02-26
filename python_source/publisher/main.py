from time import sleep
import mysql.connector as mysql
from modules import batchPublisher, listObjectsBoto3, queryMySQL
import init


def publishObjectsList () :
    db_client = mysql.connect(**init.mysql_config)
    while True :
        for dir_key in queryMySQL (db_client=db_client) :
            if dir_key == "EMPTY_LIST" :
                sleep(20)
                continue
            batchPublisher (message_source=listObjectsBoto3(prefix=f"{init.s3_root_prefix}{dir_key}"))


def message_source () :
    db_client = mysql.connect(**init.mysql_config)
    while True :
        for dir_key in queryMySQL (db_client=db_client) :
            if dir_key == "EMPTY_LIST" :
                sleep(20)
                continue
            yield dir_key

def publishDirList () :
    batchPublisher (message_source=message_source())


if __name__ == '__main__' :
    if init.flow == "publishObjectsList" :
        publishObjectsList ()
    if init.flow == "publishDirList" :
        publishDirList ()