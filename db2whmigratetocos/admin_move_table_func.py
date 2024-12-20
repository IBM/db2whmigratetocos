#!/usr/bin/env python3

"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import datetime
import json
import subprocess
import logging

from db2whmigratetocos.queries import GET_THE_ROW_COUNT, GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY, RUNSTATS_FOR_TABLE


logger = logging.getLogger(__name__)
ADM_MOVE_TABLE_CMD_DB2WOC = "CALL SYSPROC.ADMIN_MOVE_TABLE('{SCHEMANAME}','{TABLENAME}','{DEST_TBSPACE}','{INDEX_TBSPACE}','{DEST_TBSPACE}','','','','','{COPY_OPTS}','{OPTION}')"
ADM_MOVE_TABLE_PHASE_ERROR_STATE = "SQL2104N"
ADM_MOVE_TABLE_CLEANUP_ERROR_STATE = "SQL2105N"
ADM_MOVE_TABLE_FIND_PHASE = "SELECT VALUE FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='STATUS' AND TABNAME='{TABLENAME}' AND TABSCHEMA='{SCHEMANAME}' WITH UR"
ADM_MOVE_TABLE_STRUCK_PHASE = "SELECT TABNAME FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='TARGET' AND VALUE='{TABLENAME}' AND TABNAME='{TABLENAME}' AND TABSCHEMA='{SCHEMANAME}' WITH UR'"


def check_home_path():
    """_summary_

    Returns:
        _type_: _description_
    """
    try:
        home = run_command("echo $HOME")
        return home
    except Exception as e:
        print(e)


def run_command(command):
    """_summary_

    Args:
        command (_type_): _description_

    Returns:
        _type_: _description_
    """
    result = subprocess.check_output(command, shell=True, text=True)
    return result


def define_logger_file(log_file_name):
    """_summary_

    Args:
        log_file_name (_type_): _description_

    Returns:
        _type_: _description_
    """

    log_file_handler = logging.FileHandler(
        log_file_name, mode="a", encoding="utf-8")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(log_file_handler)
    return log_file_handler


def get_connection_string(user: str, password: str, hostname: str, port: str, database: str,dsn:str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_

    Returns:
        _type_: _description_
    """

    if dsn is not None:
        driver = "Driver={"+dsn+"};"
    else:
        home_path = check_home_path()
        driver = "Driver={"+home_path.strip() + \
            "/db2_cli_odbc_driver/odbc_cli/clidriver/lib/libdb2o.so};"
    database = "Database="+database+";"
    hostname = "Hostname="+hostname+";"
    port = "Port="+port+";"
    uid = "Uid="+user+";"
    password = "Pwd="+password+";"
    security = "Security=ssl;"
    protocol = "Protocol=TCPIP;"
    con_str = driver+database+hostname+port+uid+password+security+protocol
    return con_str


def db2wh_pyodbc_connection(user: str, password: str, hostname: str, port: str, database: str,dsn:str) -> bool:
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_

    Returns:
        bool: _description_
    """
    try:
        import pyodbc
        connection_string = get_connection_string(
            user, password, hostname, port, database,dsn)
        cnxn = pyodbc.connect(connection_string)
        return cnxn
    except Exception as e:
        print(e)

# admin_move_table_functions


def find_adm_status_for_struck_table(user: str, password: str, hostname: str, port: str, database: str, tablename: str,dsn:str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablename (str): _description_

    Returns:
        _type_: _description_
    """
    import pyodbc
    try:
        connection_string = get_connection_string(
            user, password, hostname, port, database,dsn)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_STRUCK_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            return item[0]
    except Exception as e:
        print(e)


def find_adm_status_to_retry(user: str, password: str, hostname: str, port: str, database: str, tablename: str, schemaname: str, src_tbspace: str, dest_tbspace: str, report_file_name: str,dsn:str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablename (str): _description_
        schemaname (str): _description_
        src_tbspace (str): _description_
        dest_tbspace (str): _description_
        report_file_name (str): _description_

    Returns:
        _type_: _description_
    """
    import pyodbc
    try:
        table_phase = " "
        connection_string = get_connection_string(
            user, password, hostname, port, database,dsn)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename,SCHEMANAME=schemaname))
        rows = conn.fetchall()
        for item in rows:
                table_phase = item[0]
                return table_phase
    except Exception as e:
        print(e)


def cancel_terminate_admin_move_table(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, tablename: str, phase: str, src_tbspace: str, dest_tbspace: str,dsn:str,index_tbspace:str,copy_opts:str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_
        tablename (str): _description_
        phase (str): _description_
        src_tbspace (str): _description_
        dest_tbspace (str): _description_
    """
    try:
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database,dsn)
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME=schemaname, TABLENAME=tablename,
                     OPTION=phase, SOURCE_TBSPACE=src_tbspace, DEST_TBSPACE=dest_tbspace,INDEX_TBSPACE=index_tbspace,COPY_OPTS=copy_opts))
        rows = conn.fetchall()
        logger.info(phase)
        logger.info(rows)
    except Exception as e:
        print(e)


def adm_move_table_phase(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, tablename: str, phase: str, src_tbspace: str, dest_tbspace: str, report_file_name,dsn:str,index_tbspace:str,copy_opts:str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_
        tablename (str): _description_
        phase (str): _description_
        src_tbspace (str): _description_
        dest_tbspace (str): _description_
        report_file_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        status = " "
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database,dsn)
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME=schemaname, TABLENAME=tablename,
                     OPTION=phase, SOURCE_TBSPACE=src_tbspace, DEST_TBSPACE=dest_tbspace,INDEX_TBSPACE=index_tbspace,COPY_OPTS=copy_opts))
        rows = conn.fetchall()
        if rows is not None:
            logger.info(phase)
            logger.info(rows)
            log_for_the_phase = parse_adm_move_table_by_phase(rows, phase)
            log_for_the_phase['SQL'] = ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME=schemaname, TABLENAME=tablename,
                                                                        OPTION=phase, SOURCE_TBSPACE=src_tbspace, DEST_TBSPACE=dest_tbspace,INDEX_TBSPACE=index_tbspace,COPY_OPTS=copy_opts)
            if log_for_the_phase['STATUS'] == 'COMPLETE':
                log_for_the_phase['COPY_TOTAL_ROWS'] = get_the_rows_moved_in_admin_move_table(schemaname, tablename, user, password, hostname, port, database,dsn)
            with open(report_file_name, 'r+', encoding='utf-8') as file:
                file_data = json.load(file)
                file_data["phase_logs"].append(log_for_the_phase)
                file_data["status"] = log_for_the_phase["STATUS"]
                file.seek(0)
                json.dump(file_data, file, indent=6)
            for item in rows:
                if item[0] == 'INIT_START':
                    init_start = item[1]
                if item[0] == 'STATUS':
                    status = item[1]
                if item[0] == 'CLEANUP_END':
                    cleanup_end = item[1]
            return status
        else:
            status = find_adm_status_to_retry(user, password, hostname, port, database,tablename,schemaname,src_tbspace,dest_tbspace,report_file_name,dsn)
            print("status if none")
            print(status)
            if status is not None:
              return status
            else:
               return "NONE"
    except Exception as e:
        x, y = e.args
        if ADM_MOVE_TABLE_PHASE_ERROR_STATE in y:
            logger.info(x)
            logger.error("Error: %s", ADM_MOVE_TABLE_PHASE_ERROR_STATE)
            log_for_the_phase = {
                "STATUS": "INPROGRESS",
                "ERROR_CODE": ADM_MOVE_TABLE_PHASE_ERROR_STATE,
                "MESSAGE": "Kindly check if the table is already in movement, cancel if needed"
            }
            with open(report_file_name, 'r+', encoding='utf-8') as file:
                file_data = json.load(file)
                file_data["phase_logs"].append(log_for_the_phase)
                file_data["status"] = log_for_the_phase["STATUS"]
                file.seek(0)
                json.dump(file_data, file, indent=6)
            return "INPROGRESS"
        else:
            print(e)
            logger.info(e)
            logger.error("Error: %s", e)
            log_for_the_phase = {
                "STATUS": "ERROR",
                "ERROR_CODE": str(e),
            }
            with open(report_file_name, 'r+', encoding='utf-8') as file:
                file_data = json.load(file)
                file_data["phase_logs"].append(log_for_the_phase)
                file_data["status"] = "ERROR"
                file.seek(0)
                json.dump(file_data, file, indent=6)
            return "COMPLETE"

        # if ADM_MOVE_TABLE_CLEANUP_ERROR_STATE in y:
        #          status = find_adm_status_for_a_table(user,password,hostname,port,database,tablename,schemaname,src_tbspace,dest_tbspace)
        #          print("Error: " + ADM_MOVE_TABLE_CLEANUP_ERROR_STATE)
        #          print("Cleaning up the failed movement and retrying the movement from INIT")
        #          if status != "COMPLETE" or "CLEANUP":
        #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"TERM",src_tbspace,dest_tbspace, report_file_name)
        #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"CANCEL",src_tbspace,dest_tbspace, report_file_name)
        #             adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace)


def parse_adm_move_table_by_phase(rows: any, phase: str):
    """_summary_

    Args:
        rows (any): _description_
        phase (str): _description_

    Returns:
        _type_: _description_
    """
    init_start = " "
    init_end = " "
    init_opts = " "
    copy_start = " "
    copy_end = " "
    copy_opts = " "
    swap_start = " "
    swap_end = " "
    cleanup_start = " "
    cleanup_end = " "
    if phase == "INIT":
        for item in rows:
            if item[0] == 'INIT_START':
                init_start = item[1]
            if item[0] == 'INIT_END':
                init_end = item[1]
            if item[0] == 'INIT_OPTS':
                init_opts = item[1]
        init_phase_details = {
            "STATUS": phase,
            "INIT_START": init_start,
            "INIT_END": init_end,
            "INIT_OPTS": init_opts
        }
        return init_phase_details
    if phase == "COPY":
        for item in rows:
            if item[0] == 'COPY_START':
                copy_start = item[1]
            if item[0] == 'COPY_END':
                copy_end = item[1]
            if item[0] == 'COPY_OPTS':
                copy_opts = item[1]
        copy_phase_details = {
            "STATUS": phase,
            "COPY_START": copy_start,
            "COPY_END": copy_end,
            "COPY_OPTS": copy_opts
        }
        return copy_phase_details
    if phase == "REPLAY":
        for item in rows:
            replay_phase_details = {
                "STATUS": "REPLAY"
            }
        return replay_phase_details
    if phase == "SWAP":
        for item in rows:
            if item[0] == 'SWAP_START':
                swap_start = item[1]
            if item[0] == 'SWAP_END':
                swap_end = item[1]
            if item[0] == 'CLEANUP_START':
                cleanup_start = item[1]
            if item[0] == 'CLEANUP_END':
                cleanup_end = item[1]
            if item[0] == 'INIT_START':
                init_start = item[1]
        swap_phase_details = {
            "STATUS": "COMPLETE",
            "SWAP_START": swap_start,
            "SWAP_END": swap_end,
            "CLEANUP_START": cleanup_start,
            "CLEANUP_END": cleanup_end,
        }
        return swap_phase_details


def adm_move_table_ops_db2woc(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, tablename: str, status: str, src_tbspace: str, dest_tbspace: str, report_file_name: str, log_file_name: str,dsn:str,index_tbspace:str,copy_opts:str,runstats:bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_
        tablename (str): _description_
        status (str): _description_
        src_tbspace (str): _description_
        dest_tbspace (str): _description_
        report_file_name (str): _description_
        log_file_name (str): _description_
    """
    log_file_handler = define_logger_file(log_file_name)
    init_start = ""
    cleanup_end = ""
    while status != "COMPLETE":
        logger.info("INIT Phase for {TABLENAME}".format(TABLENAME=tablename))
        if status == "INIT":
            status = adm_move_table_phase(
                user, password, hostname, port, database, schemaname, tablename, "INIT", src_tbspace, dest_tbspace, report_file_name,dsn,index_tbspace,copy_opts)     
        if status == "COPY":
            logger.info("COPY Phase for {TABLENAME}".format(
                TABLENAME=tablename))
            status = adm_move_table_phase(
                user, password, hostname, port, database, schemaname, tablename, "COPY", src_tbspace, dest_tbspace, report_file_name,dsn,index_tbspace,copy_opts)
            status = find_adm_status_to_retry(user, password, hostname, port, database,tablename,schemaname,src_tbspace,dest_tbspace,report_file_name,dsn)
        if status == "REPLAY":
            logger.info("REPLAY Phase for {TABLENAME}".format(
                TABLENAME=tablename))
            status = adm_move_table_phase(
                user, password, hostname, port, database, schemaname, tablename, "REPLAY", src_tbspace, dest_tbspace, report_file_name,dsn,index_tbspace,copy_opts)
            logger.info("SWAP Phase for {TABLENAME}".format(
                TABLENAME=tablename))
            status = adm_move_table_phase(
                user, password, hostname, port, database, schemaname, tablename, "SWAP", src_tbspace, dest_tbspace, report_file_name,dsn,index_tbspace,copy_opts)
            status = find_adm_status_to_retry(user, password, hostname, port, database,tablename,schemaname,src_tbspace,dest_tbspace,report_file_name,dsn)
            if status == "COMPLETE":
                logger.info("Movement COMPLETE for {TABLENAME}".format(TABLENAME=tablename))
                if runstats is True:
                    logger.info("RUNSTATS for the table {TABLENAME} is triggered".format(TABLENAME=tablename)) 
                    trigger_runstats_for_table(schemaname, tablename,user, password, hostname, port, database,dsn)
                    logger.info("RUNSTATS for the table {TABLENAME} is completed ".format(TABLENAME=tablename)) 
                logger.removeHandler(log_file_handler)
                break
            else:
                logger.info("Error in SWAP Phase for  {TABLENAME}".format(
                    TABLENAME=tablename))
                logger.removeHandler(log_file_handler)
                break
        if status == "INPROGRESS":
            print()
            if "USE_ADC" in copy_opts:
              adc="--use-adc"
            else:
             adc = "--no-use-adc"
            print("The migration for the table is already in progress")
            print("Run the following command to cancel the exisitng run")
            print('''db2whmigratetocos cancel --schema-name {SCHEMANAME} --table-name {TABLENAME} --src-tablespace {SRC_TABLESPACE} --dest-tablespace {DEST_TABLESPACE} --user-id {USER}  --password '{PASSWORD}'  --hostname {HOSTNAME} --index-tbspace {INDEX_TABSPACE} {ADC} --log-file-name {LOG_FILE}  --report-file-name {REPORT_FILE}'''.
                  format(SCHEMANAME = schemaname,TABLENAME=tablename,SRC_TABLESPACE=src_tbspace,DEST_TABLESPACE=dest_tbspace,USER=user,PASSWORD=password,HOSTNAME=hostname,INDEX_TABSPACE=index_tbspace,LOG_FILE=log_file_name,REPORT_FILE=report_file_name,ADC=adc))
            print("Initiating the move for the next table to move if present ")
            print()
            logger.removeHandler(log_file_handler)
            break
        if status == None or status == "None" or status == "NONE":
            logger.removeHandler(log_file_handler)
            print("Initiating the move for the next table to move if present ")
            break
        if status == "ERROR":
            print("Please check the above error")
            logger.removeHandler(log_file_handler)
            print("Initiating the move for the next table to move if present ")
            break   
    logger.removeHandler(log_file_handler) 
   

def get_the_rows_moved_in_admin_move_table(schemaname, tablename, user_id, password, hostname, port, database,dsn):
    """_summary_

    Args:
        schemaname (_type_): _description_
        tablename (_type_): _description_
        user_id (_type_): _description_
        password (_type_): _description_
        hostname (_type_): _description_
        port (_type_): _description_
        database (_type_): _description_

    Returns:
        _type_: _description_
    """
    cnxn = db2wh_pyodbc_connection(
        user_id, password, hostname, port, database,dsn)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]


def get_the_rows_after_admin_move_table(schemaname, tablename, user_id, password, hostname, port, database,dsn):
    """_summary_

    Args:
        schemaname (_type_): _description_
        tablename (_type_): _description_
        user_id (_type_): _description_
        password (_type_): _description_
        hostname (_type_): _description_
        port (_type_): _description_
        database (_type_): _description_

    Returns:
        _type_: _description_
    """
    cnxn = db2wh_pyodbc_connection(
        user_id, password, hostname, port, database,dsn)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def trigger_runstats_for_table(schemaname, tablename, user_id, password, hostname, port, database,dsn):
    """_summary_

    Args:
        schemaname (_type_): _description_
        tablename (_type_): _description_
        user_id (_type_): _description_
        password (_type_): _description_
        hostname (_type_): _description_
        port (_type_): _description_
        database (_type_): _description_
    Returns:
        _type_: _description_
    """
    cnxn = db2wh_pyodbc_connection(
        user_id, password, hostname, port, database,dsn)
    conn = cnxn.cursor()
    print("RUNSTATS for the {TABLENAME} is triggered".format(TABLENAME=tablename))
    conn.execute(RUNSTATS_FOR_TABLE.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    print("RUNSTATS for the {TABLENAME} is completed".format(TABLENAME=tablename))
    cnxn.close()