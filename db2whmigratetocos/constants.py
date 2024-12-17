
"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""

VERSION_NAME = "0.0.1"
PACKAGE_NAME = "db2whmigratetocos-"+VERSION_NAME+"-py3-none-any.whl"
STATUS_TABLE_HEADER = ["BatchId", "JobId", "Table", "Schema",
                       "Status", "Source", "Destination", "Time Taken - seconds"]
STATUS_TABLE_HEADER_ACTIVE_RUNS = ["BatchId", "JobId", "Table", "Schema",
                                   "Status", "Source", "Destination", "Progress"]
TABLESPACE_CSV_COLUMNS = ['tablespace',
                          'tablename', 'schema', 'size', 'storage']
SCHEMA_CSV_COLUMNS = ["schema", "tablename", "size"]
COPY_OPTIONS = ["COPY_USE_OTA","NO_STATS","ALLOW_READ_ACCESS","USE_ADC"]

