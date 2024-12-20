import logging
import sys
from db2whmigratetocos.db2wh_db2_utilities import db2wh_pyodbc_connection
from db2whmigratetocos.queries import LIST_SCHEMAS
import unittest
import json,codecs

with open('tests/content.json', 'r') as file:
    data = json.load(file)
    vdata = data['valid_inputs']
    invdata = data['invalid_inputs']

def conn_manually(user: str, password: str, hostname: str, port: str, database: str, cmd: str):
    cnxn = db2wh_pyodbc_connection(user, password, hostname, port, database, False)
    conn = cnxn.cursor()
    conn.execute(cmd)
    rows = conn.fetchall()
    cnxn.close()
    return rows

def test_data_json(db_out : dict, file_name: str):
    json_str = json.dumps(db_out)
    with open(file_name, "w") as final:
	    json.dump(json_str, final)