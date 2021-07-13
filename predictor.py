#!/usr/bin/env python3

from loguru import logger
from zipfile import ZipFile
import os
import mysql.connector

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

if __name__ == "__main__":
    logger.info('Connecting to NGAFID db')
    connection = connect()

    cursor = connection.cursor()
    # cursor.execute("SELECT id, filename FROM flights WHERE maintenance_probability < 0 AND airframe_id = (SELECT id FROM airframes WHERE airframe = 'Cessna 172S')")

    cursor.execute("SELECT fleet_id, uploader_id, id, filename FROM uploads");

    rs = cursor.fetchall()
    
    zip_files = []

    for r in rs:
        dir_prefix = os.environ['NGAFID_DATA_FOLDER'] + "/archive/"
        zip_path = dir_prefix + str(r[0]) + "/" + str(r[1]) + "/" + str(r[2]) + "__" + r[3]
        zip_files.append(ZipFile(zip_path, "r"))

    for zipfile in zip_files:
        zipfile.printdir()
