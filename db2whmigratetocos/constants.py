
"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""

VERSION_NAME = "0.0.1"
PACKAGE_NAME = "db2whmigratetocos-"+VERSION_NAME+"-py3-none-any.whl"
TABLESPACE_CSV_COLUMNS = ['Tablespace', 'Storage', 'Tablename', 'Schema']
COPY_OPTIONS = ["COPY_USE_OTA","NO_STATS","ALLOW_READ_ACCESS","USE_ADC"]

# Don't change the order
PHASES = ("INIT", "COPY", "REPLAY", "SWAP")

PHASES_MAP = {
    "A": "Cancel", "C": "Copy", "I": "Init", "L": "Cleanup", "M": "Move", "R": "Replay",
    "S": "Swap", "V": "Verify" 
}
