import init
from modules import filterDirList, uploadDumpToMySQL
import mysql.connector as mysql

# Steps
# 1. Get directory list dump using awscli
# 2. Load the dump to MySQL

def dumpToMySQL () :
    uploadDumpToMySQL (dump_list=filterDirList(), db_client=mysql.connect(**init.mysql_config))


if __name__ == '__main__' :
    if init.flow == "dumpToMySQL" :
        dumpToMySQL ()