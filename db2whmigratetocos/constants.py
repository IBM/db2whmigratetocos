
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

ADM_MOVE_UTL_CONFIGS = ("COMMIT_AFTER_N_ROWS", "DEEPCOMPRESSION_SAMPLE", "COPY_ARRAY_SIZE",
                        "COPY_INDEXSCHEMA", "COPY_INDEXNAME", "REPLAY_MAX_ERR_RETRIES",
                        "REPLAY_THRESHOLD", "REORG_USE_TEMPSPACE", "SWAP_MAX_RETRIES")
