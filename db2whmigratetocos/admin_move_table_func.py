#!/usr/bin/env python3

"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import json
import logging
import subprocess
import sys
import time

from rich.console import Console

from db2whmigratetocos.constants import PHASES
from db2whmigratetocos.queries import (ADM_MOVE_ACTIVE_UTILITY,
                                       GET_THE_ROW_COUNT,
                                       GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY,
                                       RUNSTATS_FOR_TABLE)

logger = logging.getLogger(__name__)

ADM_MOVE_TABLE_CMD_DB2WOC = "CALL SYSPROC.ADMIN_MOVE_TABLE('{SCHEMANAME}','{TABLENAME}','{DEST_TBSPACE}','{INDEX_TBSPACE}','{DEST_TBSPACE}','','','','','{COPY_OPTS}','{OPERATION}')"
ADM_MOVE_TABLE_PHASE_ERROR_STATE = "SQL2104N"
ADM_MOVE_TABLE_CLEANUP_ERROR_STATE = "SQL2105N"
ADM_MOVE_TABLE_FIND_PHASE = "SELECT VALUE FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='STATUS' AND TABNAME='{TABLENAME}' AND TABSCHEMA='{SCHEMANAME}' WITH UR"
ADM_MOVE_TABLE_STRUCK_PHASE = "SELECT TABNAME FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='TARGET' AND VALUE='{TABLENAME}' AND TABNAME='{TABLENAME}' AND TABSCHEMA='{SCHEMANAME}' WITH UR'"

console = Console()

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


def get_connection_string(connection_details):
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

    home_path = check_home_path()
    driver = "Driver={"+home_path.strip() + "/db2_cli_odbc_driver/odbc_cli/clidriver/lib/libdb2o.so};"
    _dsn = "DSN=" + connection_details["dsn"] + ";" if connection_details["dsn"] is not None else ""

    database = "Database=" + connection_details["database"] + ";"
    hostname = "Hostname=" + connection_details["hostname"] + ";"
    port = "Port=" + connection_details["port"] + ";"
    uid = "Uid=" + connection_details["user_id"] + ";"
    password = "Pwd=" + connection_details["password"] + ";"
    security = "Security=ssl;" if connection_details["enable_ssl"] else ""
    protocol = "Protocol=TCPIP;"
    con_str = driver+_dsn+database+hostname+port+uid+password+security+protocol+"Authentication=SERVER;"+"SSLClientKeystoredb=/ssl/keystore.kdb;"+"SSLClientKeyStash=/ssl/keystore.sth;"
    return con_str


def db2wh_pyodbc_connection(connection_details) -> bool:
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
        connection_string = get_connection_string(connection_details)
        cnxn = pyodbc.connect(connection_string)
        return cnxn
    except Exception as e:
        print(e)

# admin_move_table_functions


def find_adm_status_for_struck_table(connection_details, tablename: str):
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
        connection_string = get_connection_string(connection_details)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_STRUCK_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            return item[0]
    except Exception as e:
        print(e)


def find_adm_status_to_retry(connection_details, table):
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
        table_phase = ""
        connection_string = get_connection_string(connection_details)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()

        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(
            TABLENAME=table["tablename"], SCHEMANAME=table["schema"])
        )

        rows = conn.fetchall()

        for item in rows:
            table_phase = item[0]
            return table_phase

    except Exception as e:
        print(e)


def cancel_terminate_admin_move_table(
        connection_details, schemaname, tablename, phase, src_tbspace, dest_tbspace, index_tbspace,
        copy_opts
):
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
        cnxn = db2wh_pyodbc_connection(connection_details)
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME=schemaname, TABLENAME=tablename,
                     OPTION=phase, SOURCE_TBSPACE=src_tbspace, DEST_TBSPACE=dest_tbspace,INDEX_TBSPACE=index_tbspace,COPY_OPTS=copy_opts))
        rows = conn.fetchall()
        logger.info(phase)
        logger.info(rows)
    except Exception as e:
        print(e)


def adm_move_table_phase(
        connection_details, table, dest_tbspace, index_tbspace, report_file, log_file, copy_opts,
        phase
):
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
        schemaname = table["schema"]
        tablename = table["tablename"]
        src_tbspace = table["tablespace"]

        cnxn = db2wh_pyodbc_connection(connection_details)
        conn = cnxn.cursor()

        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(
            SCHEMANAME=schemaname, TABLENAME=tablename, OPERATION=phase, SOURCE_TBSPACE=src_tbspace,
            DEST_TBSPACE=dest_tbspace, INDEX_TBSPACE=index_tbspace, COPY_OPTS=copy_opts
        ))

        rows = conn.fetchall()
        status = ""

        if rows:
            logger.info(phase)
            logger.info(rows)
            log_for_the_phase = parse_adm_move_table_by_phase(rows, phase)

            log_for_the_phase['SQL'] = ADM_MOVE_TABLE_CMD_DB2WOC.format(
                SCHEMANAME=schemaname, TABLENAME=tablename, OPERATION=phase,
                SOURCE_TBSPACE=src_tbspace, DEST_TBSPACE=dest_tbspace, INDEX_TBSPACE=index_tbspace,
                COPY_OPTS=copy_opts
            )

            if log_for_the_phase['STATUS'] == 'COMPLETE':
                log_for_the_phase['COPY_TOTAL_ROWS'] = get_the_rows_moved_in_admin_move_table(
                    connection_details, schemaname, tablename
                )

            with open(report_file, 'r+', encoding='utf-8') as file:
                file_data = json.load(file)
                file_data["phase_logs"].append(log_for_the_phase)
                file_data["status"] = log_for_the_phase["STATUS"]
                file.seek(0)
                json.dump(file_data, file, indent=6)

            status = log_for_the_phase['STATUS']

        return status

    except Exception as e:
        x, y = e.args

        logger.info(x)
        logger.error("Error Code: %s", y)
        log_for_the_phase = {
            "STATUS": "Error",
            "ERROR_CODE": y,
            "MESSAGE": x
        }

        with open(report_file, 'r+', encoding='utf-8') as file:
            file_data = json.load(file)
            file_data["phase_logs"].append(log_for_the_phase)
            file_data["status"] = log_for_the_phase["STATUS"]
            file.seek(0)
            json.dump(file_data, file, indent=6)

        return "ERROR"


def parse_adm_move_table_by_phase(rows: any, phase: str):
    """_summary_

    Args:
        rows (any): _description_
        phase (str): _description_

    Returns:
        _type_: _description_
    """
    phase_keys = {
        "INIT": ["STATUS", "INIT_START", "INIT_END", "INIT_OPTS"],
        "COPY": ["STATUS", "COPY_START", "COPY_END", "COPY_OPTS"],
        "REPLAY": ["STATUS"],
        "SWAP": ["STATUS", "SWAP_START", "SWAP_END", "CLEANUP_START", "CLEANUP_END"],
    }
    phase_details = {}

    if phase in phase_keys:
        phase_details = {"PHASE": phase}

        for row in rows:
            ke, va = row

            if ke in phase_keys[phase]:
                phase_details[ke] = va

    return phase_details

def is_migration_active(connection_details, schema, tablename):
    cnxn = db2wh_pyodbc_connection(connection_details)
    conn = cnxn.cursor()
    conn.execute(ADM_MOVE_ACTIVE_UTILITY.format(TABLENAME=tablename, SCHEMA=schema))
    rows = conn.fetchall()
    return bool(rows)

def adm_move_table_ops_db2woc(
        connection_details, table, phase, dest_tbspace, index_tbspace, copy_opts, runstats,
        report_file, log_file
):
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
    define_logger_file(log_file)
    tablename = table["tablename"]
    schema = table["schema"]

    if phase == "CLEANUP":
        logger.info("Executing 'CLEANUP' operation for %s.%s", schema, tablename)
        console.print("Executing 'CLEANUP' operation for %s.%s", schema, tablename)

        status = adm_move_table_phase(
            connection_details, table, dest_tbspace, index_tbspace, report_file, log_file,
            copy_opts, phase
        )

        if status in ("ERROR", ""):
            logger.error("Error during %s phase.", phase)
            sys.exit(1)

    else:
        for phs in PHASES[PHASES.index(phase):]:
            logger.info("Executing '%s' operation for %s.%s", phs, schema, tablename)
            console.print(f"Executing '{phs}' operation for '{schema}.{tablename}'")

            status = adm_move_table_phase(
                connection_details, table, dest_tbspace, index_tbspace, report_file, log_file,
                copy_opts, phs
            )

            if status in ("ERROR", ""):
                logger.error("Unexpected error during %s phase.", phase)
                sys.exit(1)

            # Keep checking for migration status until the phase is completed
            while is_migration_active(connection_details, schema, tablename):
                time.sleep(10)

    logger.info("Migration complete for %s.%s", schema, tablename)

    if runstats:
        logger.info("Executing RUNSTATS for %s.%s", schema, tablename)
        trigger_runstats_for_table(connection_details, schema, tablename)

def get_the_rows_moved_in_admin_move_table(connection_details, schema, table):
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
    cnxn = db2wh_pyodbc_connection(connection_details)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT.format(TABLENAME=table, SCHEMANAME=schema))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]


def get_the_rows_after_admin_move_table(connection_details, schemaname, tablename):
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
    cnxn = db2wh_pyodbc_connection(connection_details)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def trigger_runstats_for_table(connection_details, schema, tablename):
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
    cnxn = db2wh_pyodbc_connection(connection_details)
    conn = cnxn.cursor()

    print("RUNSTATS for the {SCHEMA}.{TABLENAME} is triggered".format(
        SCHEMA=schema, TABLENAME=tablename
    ))

    conn.execute(RUNSTATS_FOR_TABLE.format(SCHEMANAME=schema, TABLENAME=tablename))
    rows = conn.fetchall()

    print("RUNSTATS for the {SCHEMA}.{TABLENAME} is completed".format(
        SCHEMA=schema, TABLENAME=tablename
    ))

    cnxn.close()
