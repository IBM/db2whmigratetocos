"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import csv
import json
from pathlib import Path
import subprocess
import os
import sys
import uuid
import math
from collections import deque, defaultdict
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd
from rich.table import Table
from rich.console import Console
from db2whmigratetocos.admin_move_table_func import adm_move_table_ops_db2woc
from db2whmigratetocos.constants import SCHEMA_CSV_COLUMNS, TABLESPACE_CSV_COLUMNS
from db2whmigratetocos.queries import ADM_MOVE_ACTIVE_UTILITY, ADM_MOVE_STATUS, ADM_MOVE_TABLE_FIND_PHASE, GET_OBJECTSPACE_USING_SGNAME, GET_STORAGE_PATH_DEFINED_IN_INSTANCE, GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY, GET_USER_CREATED_INDEX, LIST_ALL_TABLES, LIST_SCHEMAS, LIST_TABLES_IN_SCHEMA, LIST_TABLES_IN_TSPACE, LIST_TBSPACE_BY_TABNAME, LIST_TBSPACES, TAB_SIZE, GET_THE_ROW_COUNT, ADM_MOVE_TABLE_FIND_TARGET_TABLE, LIST_PARENT_TABLES, CREATE_TABLESPACE


console = Console()


# os_ functions
def check_home_path() -> str:
    """
    Checks the hoem path of the instance

    Returns:
        _type_: string 
    """
    try:
        home = run_command("echo $HOME")
        return home
    except subprocess.CalledProcessError as e:
        print(e)


def run_command(command: str) -> str:
    """
    runs the command as a subprocess in the shell
    used for running the os commands

    Args:
        command (str): _description_

    Returns:
        _type_: the string output
    """
    result = subprocess.check_output(command, shell=True, text=True)
    return result


# db2 utility functions

def get_tablespaces_in_block_and_cos(connection_details):
    """
    Get the list tablespaces in the block storage and COS

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_

    Returns:
        _type_: list of tablespaces
    """
    try:
        user_tablespaces_list = []
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACES)
        rows = conn.fetchall()
        cnxn.close()

        sys_tablespaces = ["SYS", "TS4CONSOLE", "TS4MONITOR", "BIGSQLCATUTILITY", "TEMP", "TMP"]

        for item in rows:
            ts = item[0].strip()

            if not any(tablespace in ts for tablespace in sys_tablespaces):
                user_tablespaces_list.append(ts)

        return user_tablespaces_list
    except Exception as e:
        print(e)


def get_schema_in_instance(connection_details: dict):
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
    try:
        user_schemas_list = []
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_SCHEMAS)
        rows = conn.fetchall()
        cnxn.close()

        exclude_schemas = {
            "SYS", "NULL", "TS4", "SQL", "IBMPDQ", "DEFAULT", "IBM_RTMON", "IBMCONSOLE"
        }

        for item in rows:
            schema = item[0]

            if not any(
                schema for exclude_schema in exclude_schemas if exclude_schema in schema
            ):
                user_schemas_list.append(schema.strip())

        return user_schemas_list

    except Exception as e:
        print(e)


def get_tables_under_schema_in_db2woc(connection_details: dict, schemaname: str, detail: bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        tables_in_schema = []
        total_estimate_size = 0

        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_SCHEMA.format(SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()

        table_cnt = len(rows)

        with console.status(""):
            for item in rows:
                tablename, tablespace = item[0], item[1]
                table_details = {"tablename": tablename, "tablespace": tablespace}

                if detail:
                    est_size = tab_size_by_table_name(connection_details, schemaname, tablename)
                    total_estimate_size += int(est_size)
                    table_details["size"] = str(est_size)

                tables_in_schema.append(table_details)

        return total_estimate_size, tables_in_schema, table_cnt
    except Exception as e:
        print(e)

def get_tables_under_schem_notabsize_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, dsn:str, enable_ssl: bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        tables_in_schema = []
        total_estimate_size = 0
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False, dsn, enable_ssl)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_SCHEMA.format(SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = len(rows)
        for item in rows:
            tables_in_schema.append([item[0]])
        return table_cnt, tables_in_schema
    except Exception as e:
        print(e)



def tab_size_by_table_name(connection_details: dict, schemaname: str, tablename: str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        schemaname (str): _description_
        tablename (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(TAB_SIZE.format(TABSCHEMA=schemaname, TABNAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            return int(item[0])+int(item[1])+int(item[2])+int(item[3])+int(item[4])
    except Exception as e:
        print(e)


def get_tables_under_tablespace_in_db2woc(connection_details:dict, tablespace: str, detail: bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablespace (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        tables_in_tablespace: List[Dict] = []
        table_cnt = 0
        total_estimate_size = 0

        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()

        with console.status(""):
            for item in rows:
                tablename, schema = item[0], item[1]

                if "SYS" not in schema:
                    table_cnt += 1
                    table_details = {"tablename": tablename, "schema": schema}

                    if detail:
                        est_size = tab_size_by_table_name(connection_details, schema, tablename)
                        total_estimate_size += int(est_size)
                        table_details["size"] = str(est_size)

                    tables_in_tablespace.append(table_details)

        return total_estimate_size, tables_in_tablespace, table_cnt
    except Exception as e:
        print(e)

def get_parent_table(table, schema, object_space_list, fetch_details, cursor):
    # return list of tablespace tablename schema size and storage of parent table of table, schema
    parent_tables = []

    cursor.execute(LIST_PARENT_TABLES.format(TABNAME=table, SCHEMANAME=schema))
    rows = cursor.fetchall()

    for tablename, schema_name in rows:
        cursor.execute(LIST_TBSPACE_BY_TABNAME.format(TABNAME=tablename, SCHEMANAME=schema_name))
        tablespace = cursor.fetchone()[0]
        storage = "COS" if tablespace in object_space_list else "Block-Storage"

        table_details = {
            "tablename": tablename, "schema": schema_name, "tablespace": tablespace,
            "storage": storage
        }

        if fetch_details:
            cursor.execute(TAB_SIZE.format(TABSCHEMA=schema_name, TABNAME=tablename))
            data = cursor.fetchone()
            size = str(data[0] + data[1] + data[2] + data[3] + data[4])
            table_details["size"] = size

        parent_tables.append(table_details)

    return parent_tables

def get_tables_parent_tables(
        tables_list: List[tuple], object_space_list, fetch_details: bool, connection_details: dict
):

    import pyodbc

    connection_string = get_connection_string(connection_details)
    conn = pyodbc.connect(connection_string)
    cur = conn.cursor()

    visited = {(d["schema"], d["tablename"]) for d in tables_list}
    results = list(tables_list)
    queue = deque(tables_list)

    # Collect parents of tables iteratively
    while queue:
        table_details: dict = queue.popleft()

        parents: List[tuple] = get_parent_table(
            table_details["tablename"], table_details["schema"], object_space_list, fetch_details,
            cur
        )

        for parent_details in parents:
            sch_tab = (parent_details["schema"], parent_details["tablename"])

            if sch_tab not in visited:
                visited.add(sch_tab)
                results.append(parent_details)

                # add the parent table to queue to check its dependencies
                queue.append(parent_details)

    return results

def get_tables_under_tablespace_no_tabsize_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str, dsn:str, enable_ssl: bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablespace (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        table_names_in_tablespace = []
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False, dsn, enable_ssl)
        conn = cnxn.cursor()
        table_cnt = 0
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()
        with console.status(""):
            for item in rows:
                if "SYS" not in item[1]:
                    if str(item[0]).endswith('t') is False:
                        table_cnt = table_cnt + 1
                        table_names_in_tablespace.append(
                            [item[0], item[1]])
        return table_names_in_tablespace, table_cnt
    except Exception as e:
        print(e)


def get_tables_cnt_under_tablespaces(connection_details: dict, tablespace: str):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablespace (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = 0
        with console.status(""):
            for item in rows:
                if "SYS" not in item[1]:
                    table_cnt = table_cnt + 1
        return table_cnt
    except Exception as e:
        print(e)


def get_tabname_schemaname_under_tablespace_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str, dsn:str, enable_ssl: bool):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        tablespace (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        table_names_in_tablespace = []
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False, dsn, enable_ssl)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()
        with console.status(""):
            for item in rows:
                if "SYS" not in item[1]:
                    table_names_in_tablespace.append([item[0], item[1]])
        return table_names_in_tablespace
    except Exception as e:
        print(e)


def get_tbpsace_name_for_table(connection_details, tablename, schemaname):
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
    try:
        tablespace_name = ""
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACE_BY_TABNAME.format(TABNAME=tablename, SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()

        for item in rows:
            tablespace_name = item[0].strip()

        return tablespace_name

    except Exception as e:
        print(e)

# pyodbc connection fucntions


def get_connection_string(connection_details: dict):
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        dsn (str): _description_
        enable_ssl 

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


def db2wh_pyodbc_connection(connection_details: dict, test_con: bool) -> bool:
    """_summary_

    Args:
        user (str): _description_
        password (str): _description_
        hostname (str): _description_
        port (str): _description_
        database (str): _description_
        test_con (bool): _description_

    Returns:
        bool: _description_
    """
    import pyodbc
    try:
        connection_string = get_connection_string(connection_details)
        cnxn = pyodbc.connect(connection_string)
        if test_con:
            try:
                conn = cnxn.cursor()
                conn.execute(LIST_TBSPACES)
                conn.fetchall()

                console.print(
                    f"Connected to the Instance - {connection_details['hostname']}"
                )

                print("Test Connection Successful\n")
                return True
            except Exception as e:
                print(e)
        else:
            return cnxn
    except Exception as e:
        print(e)

# logs functions


def generate_uuid():
    """_summary_

    Returns:
        _type_: _description_
    """
    generated_id = uuid.uuid4()
    return str(generated_id).split("-", maxsplit=1)[0]


def check_if_logs_path_exist_else_create(log_directory_base_path:str):
    """_summary_

    Returns:
        _type_: _description_
    """
    try:
        path = log_directory_base_path
        is_exist = os.path.exists(path)
        if is_exist:
            return path
        else:
            os.makedirs(path, exist_ok=True)
            return path
    except Exception as e:
        print(e)


def create_log_directory_for_migration_run(log_directory_base_path,directory_name: str):
    """_summary_

    Args:
        directory_name (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        directory_path = check_if_logs_path_exist_else_create(log_directory_base_path)
        migration_sub_directory = str(
            directory_path)+"/"+directory_name.strip()
        os.makedirs(migration_sub_directory, exist_ok=True)
        return migration_sub_directory
    except Exception as e:
        print(e)


def create_a_log_directory_for_a_batch(log_directory_base_path:str):
    """_summary_
    """
    log_directory_name = ""
    c = datetime.now()
    current_time = c.strftime('%d%m%Y-%H%M%S')
    directory_name = "batch-"+str(current_time)
    log_directory_name = create_log_directory_for_migration_run(log_directory_base_path,directory_name)
    return log_directory_name


def create_files_for_migration(files: list[Path]):
    """_summary_

    Args:
        directory_name (str): _description_
        file_name (str): _description_

    Returns:
        _type_: _description_
    """
    for fl in files:
        if fl.suffix == ".json":
            fl.write_text("{}")

        if fl.suffix == ".log":
            fl.write_text("")


def unzip_the_adm_script():
    """_summary_
    """
    print()
    print("unziping the driver package")
    try:
        find_whl = run_command("find db2whmigratetocos-0.2-py3-none-any.whl")
        if find_whl.strip() == "db2whmigratetocos-0.2-py3-none-any.whl":
            unzip_out = run_command(
                "unzip ./db2whmigratetocos-0.2-py3-none-any.whl 'db2whmigratetocos/admin_move_table_func.py' -d .")
            print(unzip_out)
        else:
            print(".whl file not found..aborting")
    except Exception as e:
        print(e)


def get_json_format_for_migration_run(table_details, phase, dest_tbspace, migration_job_id):
    """_summary_

    Args:
        schemaname (str): _description_
        tablename (str): _description_
        status (str): _description_
        src_tbspace (str): _description_
        dest_tbspace (str): _description_
        migration_job_id (str): _description_

    Returns:
        _type_: _description_
    """
    migration_meta_data = {
        "migration_job_id": migration_job_id,
        "source_tablespace": table_details["tablespace"],
        "destination_tablespace": dest_tbspace,
        "status": "REQUESTED TO " + phase,
        "table_name": table_details["tablename"],
        "schema_name": table_details["schema"],
        "phase_logs": [],
    }
    return migration_meta_data


def find_adm_status_by_tablename(user: str, password: str, hostname: str, port: str, database: str, tablename: str, dsn: str, enable_ssl: bool):
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
        table_phase = " "
        connection_string = get_connection_string(
            user, password, hostname, port, database, dsn, enable_ssl)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        for item in rows:
            table_phase = item[0]
            return table_phase
    except Exception as e:
        print(e)


def render_table(columns_key_map, data, limit=75):
    table = Table()

    for column, _ in columns_key_map:
        table.add_column(column, justify="center")

    for count, row in enumerate(data):
        if count >= limit:
            break

        row_values = [row.get(ke, "") for _, ke in columns_key_map]
        table.add_row(*row_values)

    console.print(table)


# status utilities

def print_table_row(tables) -> Table:
    """_summary_

    Args:
        tables (_type_): _description_

    Returns:
        Table: _description_
    """
    tb_table = Table()
    tb_table.add_column("Tablespace", justify="center")
    tb_table.add_column("Table count", justify="center")
    for tablespace in tables:
        tb_table.add_row(tablespace[0], str(tablespace[1]))
    return tb_table


def add_latest_migration(all_migration_details: List[Dict], table_migration_data: Dict):
    key_fields = ["source_tablespace", "destination_tablespace", "table_name", "schema_name"]

    for i, migration_detail in enumerate(all_migration_details):

        if all(migration_detail[key] == table_migration_data[key] for key in key_fields):
            new_time = datetime.strptime(table_migration_data["batch_id"], "%Y%m%d-%H%M%S")
            existing_time = datetime.strptime(migration_detail["batch_id"], "%Y%m%d-%H%M%S")

            if new_time > existing_time:
                all_migration_details[i] = table_migration_data
            return

    all_migration_details.append(table_migration_data)


def list_migration_runs(migration_batches: List[Path]):
    """_summary_

    Args:
        migration_batches (_type_): _description_
    """
    active_migration_job_details = []
    completed_migration_job_details = []

    for batch in migration_batches:
        tables_migration_report = batch.glob('*.json')

        if not tables_migration_report:
            continue

        for table_migration_report in tables_migration_report:
            with open(table_migration_report, "r", encoding="utf-8") as f:
                table_migration_data = json.load(f)
            table_migration_data["batch_id"] = batch.name

            if table_migration_data['status'] != "COMPLETE":
                add_latest_migration(active_migration_job_details, table_migration_data)

            else:
                add_latest_migration(completed_migration_job_details, table_migration_data)

    return active_migration_job_details, completed_migration_job_details


def parse_the_json_files_for_status(migration_job_details: list, user_id: str, password: str, hostname: str, port: str, database: str, table_header: list, active: bool, dsn:str, enable_ssl: bool) -> Table:
    """_summary_

    Args:
        migration_job_details (_type_): _description_

    Returns:
        Table: _description_
    """
    tb_table = Table()
    for table_column_name in table_header:
        tb_table.add_column(table_column_name, justify="center")
    for details in migration_job_details:
        init_time = " "
        end_time = " "
        init_bool = False
        end_bool = False
        phase_name = ""
        time_taken = "-"
        init_start = " "
        cleanup_end = " "
        original_rows = 0
        target_rows = 0
        progress = 0
        if len(details['phase_logs']) > 0:
            for phase in details['phase_logs']:
                if phase['STATUS'] != 'COMPLETE' and phase['STATUS'] != 'INPROGRESS':
                    phase_name = find_adm_status_by_tablename(
                        user_id, password, hostname, port, database, str(details['table_name']), dsn, enable_ssl)
                else:
                    phase_name = details['status']
                if phase['STATUS'] == "INIT":
                    init_time = phase['INIT_START']
                    init_bool = True
                if phase['STATUS'] == "COMPLETE":
                    end_time = phase['CLEANUP_END']
                    end_bool = True
                if init_bool and end_bool:
                    init_start = datetime.strptime(
                        init_time, "%Y-%m-%d-%H.%M.%S.%f")
                    cleanup_end = datetime.strptime(
                        end_time, "%Y-%m-%d-%H.%M.%S.%f")
                    time_taken = str(
                        int((cleanup_end - init_start).total_seconds()))
        else:
            phase_name = details['status']
        error = "Yes" if details['status'] == "ERROR" else "No"
        if active is True:
            if phase_name != "COMPLETE" and "REQUESTED TO" not in phase_name:
                target_table_name = get_the_original_tablename_from_admin_move_table(
                    details['table_name'], user_id, password, hostname, port, database, dsn, enable_ssl)
                target_rows = get_the_rows_moved_in_admin_move_table(
                    details['schema_name'], target_table_name, user_id, password, hostname, port, database, dsn, enable_ssl)
                original_rows = get_the_rows_moved_in_admin_move_table_using_count( details['schema_name'], details['table_name'], user_id, password, hostname, port, database, dsn, enable_ssl)
                if original_rows == 0 or original_rows is None:
                    original_rows = get_the_rows_moved_in_admin_move_table(
                    details['schema_name'], details['table_name'], user_id, password, hostname, port, database, dsn, enable_ssl)
                if target_rows is not None and original_rows is not None and int(original_rows) != 0:
                  if target_rows <=original_rows:
                    progress = str(math.ceil((100 - ((int(original_rows) - int(target_rows))/int(original_rows)) * 100))) + " %"
                  else:
                    progress = "TABLE_WRITE - Target "+ str(target_rows)
                tb_table.add_row(str(details['batch_id']), str(details['migration_job_id']), str(details['table_name']), details['schema_name'],
                                 phase_name, error, details['source_tablespace'], details['destination_tablespace'], str(progress))
        else:
            if phase_name == 'COMPLETE':
                tb_table.add_row(str(details['batch_id']), str(details['migration_job_id']), str(
                    details['table_name']), details['schema_name'], phase_name, error, details['source_tablespace'], details['destination_tablespace'], time_taken)
    return tb_table


# move utilities
def round_robin_counter(n):
    index = -1

    def next_index():

        nonlocal index
        index = (index + 1) % n
        return index

    return next_index


def table_migration_status(connection_details, table_details):
    tablename = table_details["tablename"]
    schema = table_details["schema"]

    cnxn = db2wh_pyodbc_connection(connection_details, False)
    conn = cnxn.cursor()
    conn.execute(ADM_MOVE_STATUS.format(SCHEMA=schema, TABLENAME=tablename))
    rows = conn.fetchall()

    if not rows:
        return ("not_started", None)

    # check for values COMPLETE, COMPLETE_WITH_WARNINGS
    phase = rows[0][0]
    if "complete" in phase.lower():
        return ("completed", None)

    conn.execute(ADM_MOVE_ACTIVE_UTILITY.format(TABLENAME=tablename, SCHEMA=schema))
    rows = conn.fetchall()
    cnxn.close()

    if not rows:
        return ("resume", phase)

    return ("in_progress", phase)


def move_table(
        connection_details, table, dest_tbspace, index_tbspace, rr_callback, runstats, copy_opts,
        log_directory_path
):
    """_summary_

    Args:
        schema (_type_): _description_
        tablename (_type_): _description_
        source_tablespace (_type_): _description_
        dest_tbspace (_type_): _description_
        log_directory_name (_type_): _description_
        user_id (_type_): _description_
        password (_type_): _description_
        hostname (_type_): _description_
        port (_type_): _description_
        database (_type_): _description_
    """
    selected_dest_tbspace = dest_tbspace[rr_callback()]
    status, phase = table_migration_status(connection_details, table)

    if status == "in_progress":
        console.log(
            f"Migration already in progress. Table: {table['tablename']}, "
            f"Schema: {table['schema']}, Phase: {phase}"
        )
        return

    if status == "completed":
        if table["tablespace"] == selected_dest_tbspace:
            console.log(
                "Source and destination tablespace are same. "
                f"Table: {table['tablename']}, Schema: {table['schema']}, "
                f"Tablespace: {selected_dest_tbspace}"
            )
            return

        phase = "INIT"

    elif status == "not_started":
        phase = "INIT"

    else:
        console.print(
            f"Detected an incomplete table migration. Resuming from phase {phase}. "
            f"Table: {table['tablename']}, Schema: {table['schema']}"
        )

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    batch_dir = log_directory_path / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    migration_job_id = generate_uuid()

    report_file = (
        batch_dir / f"{migration_job_id}-{table['schema']}-{table['tablename']}.json"
    )
    log_file = (
        batch_dir / f"{migration_job_id}-{table['schema']}-{table['tablename']}.log"
    )

    migration_meta_data = get_json_format_for_migration_run(
        table, phase, selected_dest_tbspace, migration_job_id
    )

    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(migration_meta_data, f, indent=6)

    console.print("Migration ID " + migration_job_id)
    console.print(
        f"Table : '{table['tablename']}', Schema: '{table['schema']}', "
        f"Source Tablespace: '{table['tablespace']}'"
    )
    console.print(f"Logs: {log_file}, Report: {report_file}")

    # TODO: work on index_tbspace

    adm_move_table_ops_db2woc(
        connection_details, table, phase, selected_dest_tbspace, index_tbspace, copy_opts, runstats,
        report_file, log_file
    )


def validate_input_objects(
        input_objects: List[str], available_objects: List[str], object_type: str
):

    validated_objects = [
        obj.strip()
        for obj in input_objects
        if obj.strip() in available_objects
    ]

    invalid_objects = [
        obj.strip()
        for obj in input_objects
        if obj.strip() not in available_objects
    ]

    if invalid_objects:
        print(f"Invalid {object_type}: {', '.join(invalid_objects)}\n")

    return validated_objects


def validate_the_input_db2_objects(input_list, valid_list, obj_name):
    """_summary_

    Args:
        input_list (_type_): _description_
        valid_list (_type_): _description_
        obj_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    invalid_list = []
    validated_list = []
    for obj in input_list:
        if obj.strip() not in valid_list:
            invalid_list.append(obj)
    if len(invalid_list) > 0:
        print(f"skipping invalid {obj_name}")
        print(invalid_list)
        for obj in valid_list:
            if obj in invalid_list:
                input_list.remove(obj)
        validated_list = input_list
    else:
        validated_list = input_list
    return validated_list


def validate_and_get_df_from_the_csv(csv_path):
    """_summary_

    Args:
        item (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_columns = reader.fieldnames
        unavailable_columns = [col for col in TABLESPACE_CSV_COLUMNS if col not in csv_columns]

        if unavailable_columns:
            console.print(f"Missing columns in CSV: {', '.join(unavailable_columns)}")
            sys.exit(1)

        data = [
            {ke.lower(): va for ke, va in row.item()}
            for row in reader
        ]

        return data


def validate_tables(connection_details: dict, migration_tables: List[Dict]):
    cnxn = db2wh_pyodbc_connection(connection_details, False)
    conn = cnxn.cursor()
    conn.execute(LIST_ALL_TABLES)
    rows = conn.fetchall()
    cnxn.close()

    all_tables = {
        tuple(v.strip().lower() for v in row)
        for row in rows
    }

    valid_tables = [
        table_details for table_details in migration_tables
        if (
            table_details.get("tablespace", "").strip().lower,
            table_details.get("schema", "").strip().lower,
            table_details.get("tablename", "").strip().lower
        ) in all_tables
    ]

    return valid_tables

def filter_migration_tables(
        connection_details, migration_tables, skip_tbspace, skip_schema, object_tablespaces
):
    available_tablespaces = get_tablespaces_in_block_and_cos(connection_details)
    available_schemas = get_schema_in_instance(connection_details)
    valid_migration_tables = []
    object_tablespace_tables = []
    invalid_tablespace_tables = []
    skip_tablespace_tables = []
    skip_schema_tables = []
    invalid_schema_tables = []

    columns_key_map = [
        ("Tablespace", "tablespace"), ("Schema", "schema"), ("Tablename", "tablename")
    ]

    for table_details in migration_tables:
        ts = table_details.get("tablespace")
        sch = table_details.get("schema")

        if ts in object_tablespaces:
            object_tablespace_tables.append(table_details)
            continue

        if ts not in available_tablespaces:
            invalid_tablespace_tables.append(table_details)
            continue

        if sch not in available_schemas:
            invalid_schema_tables.append(table_details)
            continue

        if ts in skip_tbspace:
            skip_tablespace_tables.append(table_details)
            continue

        if sch in skip_schema:
            skip_schema_tables.append(table_details)
            continue

        valid_migration_tables.append(table_details)

    def print_filtered_tables(msg, data):
        if data:
            console.print(msg)
            render_table(columns_key_map, data, len(data))

    print_filtered_tables("Following tables are already in object tablespace:", object_tablespace_tables)
    print_filtered_tables("Following tables are having invalid tablespace:", invalid_tablespace_tables)
    print_filtered_tables("Following tables are having invalid schema:", invalid_schema_tables)
    print_filtered_tables("Following tables are having skip tablespace:", skip_tablespace_tables)
    print_filtered_tables("Following tables are having skip tablespace:", skip_schema_tables)

    return valid_migration_tables


def print_export_tables_in_block_and_cos(tablespace_list, export_csv):
    """_summary_

    Args:
        tablespace_list (_type_): _description_
        export_csv (_type_): _description_
    """
    tbs_block = []
    tbs_cos = []
    tbs_block_table = Table(show_footer=False)
    tbs_cos_table = Table(show_footer=False)
    tbs_block_table.add_column(
        "TABLESPACES in Block", justify="center", no_wrap=True)
    tbs_cos_table.add_column(
        "TABLESPACES in COS", justify="center", no_wrap=True)
    for row in tablespace_list:
        if "OBJ" in row:
            tbs_cos_table.add_row(str(row))
            tbs_cos.append(str(row))
        else:
            tbs_block_table.add_row(str(row))
            tbs_block.append(str(row))
    console.print(tbs_block_table)
    console.print(tbs_cos_table)
    if export_csv is True:
        console.print(
            "Exporting the tablespace list into CSV")
        df_blk = pd.DataFrame(
            tbs_block, columns=["tablespace"])
        df_cos = pd.DataFrame(
            tbs_cos, columns=["tablespace"])
        blk_filename = "tbspaces-in-block-"+datetime.now().isoformat()+".csv"
        cos_filename = "tbspaces-in-cos-"+datetime.now().isoformat()+".csv"
        df_blk.to_csv(blk_filename, index=False)
        df_cos.to_csv(cos_filename, index=False)
        console.print(
            "The tablespaces in block can be found in " + blk_filename)
        console.print(
            "The tablespaces in cos can be found in " + cos_filename)


def export_the_data_as_csv(tables):
    console.print("Exporting the data into CSV")
    df = pd.DataFrame(tables)
    filename = "db2whmigratetocos-tables-list-" + datetime.now().isoformat()+".csv"
    df.to_csv(filename, index=False)
    print(f"Data saved to CSV file: {filename}")
    return filename


def get_the_original_tablename_from_admin_move_table(tablename, user_id, password, hostname, port, database, dsn, enable_ssl):
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
    import pyodbc
    connection_string = get_connection_string(
        user_id, password, hostname, port, database, dsn, enable_ssl)
    cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
    conn = cnxn.cursor()
    conn.execute(ADM_MOVE_TABLE_FIND_TARGET_TABLE.format(
        TABLENAME=tablename))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]


def get_the_rows_moved_in_admin_move_table(schemaname, tablename, user_id, password, hostname, port, database, dsn, enable_ssl: bool):
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
        user_id, password, hostname, port, database, False, dsn, enable_ssl)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))

    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def get_the_rows_moved_in_admin_move_table_using_count(schemaname, tablename, user_id, password, hostname, port, database, dsn, enable_ssl: bool):
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
        user_id, password, hostname, port, database, False, dsn, enable_ssl)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def get_list_of_objectspaces(connection_details):
    """

    Returns:
        _type_: _description_
    """
    try:
        object_space_list = []
        cnxn = db2wh_pyodbc_connection(connection_details, False)
        conn = cnxn.cursor()
        conn.execute(GET_STORAGE_PATH_DEFINED_IN_INSTANCE)
        rows = conn.fetchall()
        for item in rows:
            if  "DB2REMOTE" in item[1]:
                conn.execute(GET_OBJECTSPACE_USING_SGNAME.format(SGNAME=item[0]))
                rows = conn.fetchall()
                for item in rows:
                    object_space_list.append(item[0])
                cnxn.close()
                return object_space_list
        return []
    except Exception as e:
        print(e)


def create_tablespace(user_id: str, password: str, hostname: str, port: str, database: str, dsn:str, enable_ssl: bool, tbspaces: list) -> list:
    """Creates tablespaces if are not already available.

    Parameters
    ----------
    user_id : str
        _description_
    password : str
        _description_
    hostname : str
        _description_
    port : str
        _description_
    database : str
        _description_
    dsn : str
        _description_
    enable_ssl : bool
        _description_
    """
    try:
        cnxn = db2wh_pyodbc_connection(
        user_id, password, hostname, port, database, False, dsn, enable_ssl
    )

        conn = cnxn.cursor()
        conn.execute(GET_STORAGE_PATH_DEFINED_IN_INSTANCE)
        rows = conn.fetchall()

        storage_group = next((row[0] for row in rows if "DB2REMOTE" in row[1].upper()), None)

        if storage_group is None:
            console.print("No storage group is pointing to COS.")
            sys.exit()

        conn.execute(GET_OBJECTSPACE_USING_SGNAME.format(SGNAME=storage_group))
        rows = conn.fetchall()

        available_tbspaces = [tup[0].upper() for tup in rows]
        nos_avl_tbspaces = len(available_tbspaces)

        unavailable_tbspaces = list(set(tbspaces) - set(available_tbspaces))

        if not unavailable_tbspaces:
            console.print(f"All the tablespaces '{', '.join(tbspaces)}' are available")
            return []

        if nos_avl_tbspaces == 16:

            console.print(
                f"The number of tablespaces in storage group '{storage_group}' are 16. "
                f"New tablespaces '{', '.join(tbspaces)}' can not be created."
            )

            sys.exit()

        console.print("Creating tablespaces that are not available.")

        for idx, tbspace in enumerate(unavailable_tbspaces):

            if nos_avl_tbspaces == 16:

                console.print(
                    f"The number of tablespaces in storage group '{storage_group}' are 16. "
                    f"Tablespaces '{', '.join(unavailable_tbspaces[idx:])}' can not be created. "
                    f"Tables will be moved to tablespaces '{', '.join(unavailable_tbspaces[:idx])}'"
                )

                return unavailable_tbspaces[idx:]

            conn.execute(CREATE_TABLESPACE.format(TABLESPACE=tbspace, STORAGE_GROUP=storage_group))
            cnxn.commit()
            nos_avl_tbspaces += 1

        return []

    except Exception as e:
        print(e)



def check_for_user_created_indexes(user_id, password, hostname, port, database,tablename,schemaname, dsn:str, enable_ssl: bool):
    import pyodbc
    try:
        connection_string = get_connection_string(
            user_id, password, hostname, port, database, dsn, enable_ssl)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(GET_USER_CREATED_INDEX.format(TABLENAME=tablename,SCHEMANAME=schemaname))
        rows = conn.fetchall()
        index = False
        if len(rows) > 0:
           for item in rows:
               if ("SYS" not in item[1] or "IBM" not in item[1]) and "REG" in item[3]:
                   index= True
        if index:
           return True
        else:
           return False
    except Exception as e:
        print(e)

