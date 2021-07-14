#!/usr/bin/env python3

from loguru import logger
from zipfile import ZipFile
from main import process

import os
import mysql.connector
import math

def connect():
    db_info_file = open(os.environ['NGAFID_DB_INFO'], "r")
    db_info_file.readline()

    user_line = db_info_file.readline()
    user_line = user_line[user_line.index("'") + 1 :]
    user = user_line[: user_line.index("'")]

    name_line = db_info_file.readline()
    name_line = name_line[name_line.index("'") + 1 :]
    name = name_line[: name_line.index("'")]
    
    host_line = db_info_file.readline()
    host_line = host_line[host_line.index("'") + 1 :]
    host = host_line[: host_line.index("'")]

    pw_line = db_info_file.readline()
    pw_line = pw_line[pw_line.index("'") + 1 :]
    pw = pw_line[: pw_line.index("'")]

    return mysql.connector.connect(
        host=host,
        database=name,
        user=user,
        password=pw
    )

def bin_search(li, name):
    length = len(li)
    piv = math.floor(length / 2)

    if length > 1:
        if name > li[piv]:
            return bin_search(li[piv:length], name)
        elif name < li[piv]:
            return bin_search(li[0:piv], name)
        else:
            return name == li[piv]
    else:
        return name == li[piv]



if __name__ == "__main__":
    logger.info('Connecting to NGAFID db')
    connection = connect()

    cursor = connection.cursor()
    cursor.execute("SELECT id, filename FROM flights WHERE maintenance_probability < 0 AND airframe_id = (SELECT id FROM airframes WHERE airframe = 'Cessna 172S')")

    c172_files = []
    rs = cursor.fetchall() 
    for r in rs:
        print(r[1])
        c172_files.append(r[1])

    c172_files.sort()

    cursor.execute("SELECT fleet_id, uploader_id, id, filename FROM uploads");

    rs = cursor.fetchall()
    
    zip_files = []

    for r in rs:
        dir_prefix = os.environ['NGAFID_DATA_FOLDER'] + "/archive/"
        zip_path = dir_prefix + str(r[0]) + "/" + str(r[1]) + "/" + str(r[2]) + "__" + r[3]
        zip_files.append(ZipFile(zip_path, "r"))

    # TODO: create dir for unzipped csvs
    os.mkdir("csvs")

    count = 0
    for zipfile in zip_files:
        for info in zipfile.infolist():
            if bin_search(c172_files, info.filename):
                logger.info("Extracting: " + info.filename)
                zipfile.extract(member = info, path = "csvs")
                count = count + 1


    logger.info("Found " + str(count) + " C172 flights to process")
    process("csvs")


