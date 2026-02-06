"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import csv
import json
import math
import os
import subprocess
import sys
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd
from rich.console import Console
from rich.table import Table

from db2whmigratetocos.admin_move_table_func import adm_move_table_ops_db2woc
from db2whmigratetocos.constants import PHASES_MAP, TABLESPACE_CSV_COLUMNS
from db2whmigratetocos.queries import (ADM_MOVE_ACTIVE_UTILITY,
                                       ADM_MOVE_STATUS,
                                       ADM_MOVE_TABLE_FIND_PHASE,
                                       ADM_MOVE_TABLE_FIND_TARGET_TABLE,
                                       GET_OBJECTSPACE_USING_SGNAME,
                                       GET_STORAGE_PATH_DEFINED_IN_INSTANCE,
                                       GET_THE_ROW_COUNT,
                                       GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY,
                                       GET_USER_CREATED_INDEX,
                                       LIST_PARENT_TABLES, LIST_SCHEMAS,
                                       LIST_TABLES_IN_SCHEMA,
                                       LIST_TABLES_IN_TSPACE,
                                       LIST_TBSPACE_BY_TABNAME, LIST_TBSPACES,
                                       SYSTOOLS_ADMIN_MOVE_TABLE, TAB_SIZE,
                                       TABLE_DETAILS)

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

def get_all_tablespaces(connection_details):
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
                user_tablespaces_list.append(ts.strip())

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
                tablename, tablespace = item[0].strip(), item[1].strip()
                table_details = {"tablename": tablename, "tablespace": tablespace}

                if detail:
                    est_size = tab_size_by_table_name(connection_details, schemaname, tablename)
                    total_estimate_size += int(est_size)
                    table_details["size"] = str(est_size)

                tables_in_schema.append(table_details)

        return total_estimate_size, tables_in_schema, table_cnt
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
                tablename, schema = item[0].strip(), item[1].strip()

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
    parent_tables = []

    cursor.execute(LIST_PARENT_TABLES.format(TABNAME=table, SCHEMANAME=schema))
    rows = cursor.fetchall()

    for tablename, schema_name in rows:
        tablename = tablename.strip()
        schema_name = schema_name.strip()

        cursor.execute(LIST_TBSPACE_BY_TABNAME.format(TABNAME=tablename, SCHEMANAME=schema_name))
        tablespace = cursor.fetchone()[0].strip()
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

        if not parents:
            table_details["independent"] = True

        else:
            table_details["independent"] = False

            for parent_details in parents:
                sch_tab = (parent_details["schema"], parent_details["tablename"])

                if sch_tab not in visited:
                    visited.add(sch_tab)
                    results.append(parent_details)

                    # add the parent table to queue to check its dependencies
                    queue.append(parent_details)

    return results


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


def db2wh_pyodbc_connection(connection_details: dict, test_con: bool = False) -> bool:
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


def get_migration_meta_data(table_details, phase, dest_tbspace, migration_job_id):
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
        "tablename": table_details["tablename"],
        "schema": table_details["schema"],
        "phase_logs": [],
    }
    return migration_meta_data


def find_adm_status_by_tablename(connection_details, tablename):
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
        table_phase = ""
        connection_string = get_connection_string(connection_details)
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

def add_latest_migration(all_migration_details: List[Dict], table_migration_data: Dict):
    key_fields = ["source_tablespace", "destination_tablespace", "tablename", "schema"]

    for i, migration_detail in enumerate(all_migration_details):

        if all(migration_detail[key] == table_migration_data[key] for key in key_fields):
            new_time = datetime.strptime(table_migration_data["batch_id"], "%Y%m%d-%H%M%S")
            existing_time = datetime.strptime(migration_detail["batch_id"], "%Y%m%d-%H%M%S")

            if new_time > existing_time:
                all_migration_details[i] = table_migration_data
            return

    all_migration_details.append(table_migration_data)


def list_migration_runs(migration_batches: List[Path], active_runs: bool):
    """_summary_

    Args:
        migration_batches (_type_): _description_
    """
    migration_jobs = []

    for batch in migration_batches:
        tables_migration_report = batch.glob('*.json')

        if not tables_migration_report:
            continue

        for table_migration_report in tables_migration_report:
            with open(table_migration_report, "r", encoding="utf-8") as f:
                table_migration_data = json.load(f)
            table_migration_data["batch_id"] = batch.name

            if active_runs:
                if not any(
                    st in table_migration_data.get("status", "").lower()
                    for st in ("complete", "error")
                ):
                    add_latest_migration(migration_jobs, table_migration_data)

            else:
                add_latest_migration(migration_jobs, table_migration_data)

    return migration_jobs


def parse_the_json_files_for_status(connection_details, migration_jobs: List[Dict]) -> List[Dict]:
    """_summary_

    Args:
        migration_job_details (_type_): _description_

    Returns:
        Table: _description_
    """
    result = []

    for job_details in migration_jobs:
        tablename = job_details["tablename"]
        schema = job_details["schema"]

        if job_details["phase_logs"]:

            for phase in job_details["phase_logs"]:
                if phase['STATUS'] != 'COMPLETE' and phase['STATUS'] != 'INPROGRESS':
                    phase_name = find_adm_status_by_tablename(connection_details, tablename)
        else:
            phase_name = job_details['status']

        row = {
            "batch_id": job_details.get("batch_id", ""),
            "migration_job_id": job_details.get("migration_job_id", ""),
            "schema": job_details.get('schema', ""),
            "tablename": job_details.get('tablename', ""),
            "source_tablespace": job_details.get("source_tablespace", ""),
            "destination_tablespace": job_details.get("destination_tablespace", ""),
            "phase_name": phase_name,
            "error": "Yes" if job_details.get("status", "").lower() == "error" else "No",
        }

        if phase_name != "COMPLETE":
            target_table_name = get_the_original_tablename_from_admin_move_table(
                connection_details, tablename
            )
            target_rows = get_the_rows_moved_in_admin_move_table(
                connection_details, schema, target_table_name
            )
            original_rows = get_the_rows_moved_in_admin_move_table_using_count(
                connection_details, schema, tablename
            )

            if original_rows == 0 or original_rows is None:
                original_rows = get_the_rows_moved_in_admin_move_table(
                    connection_details, schema, tablename
                )

            if target_rows is not None and original_rows is not None and int(original_rows) != 0:
                if target_rows <= original_rows:
                    progress = str(
                        math.ceil(
                            (100 - ((int(original_rows) - int(target_rows))/int(original_rows)) * 100)
                        )
                    ) + "%"

                else:
                    progress = "TABLE_WRITE - Target " + str(target_rows)

                row.update({"progress": progress})

        result.append(row)

    return result

# move utilities
def comma_string_to_list(delimited_string: str):
    if not delimited_string:
        return []

    return delimited_string.split(",")

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

    conn.execute(SYSTOOLS_ADMIN_MOVE_TABLE)
    rows = conn.fetchall()

    # When SYSTOOLS.ADMIN_MOVE_TABLE is not available
    if not rows:
        return ("not_started", None)

    conn.execute(ADM_MOVE_STATUS.format(SCHEMA=schema, TABLENAME=tablename))
    rows = conn.fetchall()

    # When no record for table exist in SYSTOOLS.ADMIN_MOVE_TABLE
    if not rows:
        return ("not_started", None)

    # check for values COMPLETE, COMPLETE_WITH_WARNINGS
    phase = rows[0][0]
    if "complete" in phase.lower():
        return ("completed", None)

    conn.execute(ADM_MOVE_ACTIVE_UTILITY.format(TABLENAME=tablename, SCHEMA=schema))
    rows = conn.fetchall()
    cnxn.close()

    # When no record for table exist in SYSPROC.MON_GET_UTILITY
    if not rows:
        return ("resume", phase)

    # When record for table exist in SYSPROC.MON_GET_UTILITY
    return ("in_progress", PHASES_MAP[rows[0][0]])


def move_table(
        connection_details, table, dest_tbspace, index_tbspace, rr_callback, runstats, copy_opts,
        log_directory_path, end_time
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
    if end_time and end_time <= datetime.now(tz=timezone.utc):
        print(end_time)
        console.print(
            f"Time limit {end_time} reached. "
            f"Skipping '{table['schema']}.{table['tablename']}'"
        )
        return

    rr_index = rr_callback()
    selected_dest_tbspace = dest_tbspace[rr_index]
    status, phase = table_migration_status(connection_details, table)

    if status == "in_progress":
        console.print(
            f"Migration already in progress for table '{table['schema']}.{table['tablename']}'. "
            f"The current phase is '{phase}'."
        )
        return

    if status in ("completed", "not_started"):
        if table["tablespace"] == selected_dest_tbspace:
            console.log(
                "Source and destination tablespace are same ({selected_dest_tbspace}) for table "
                f"'{table['schema']}.{table['tablename']}'."
            )
            return

        phase = "INIT"

    else:
        console.print(
            f"Detected an incomplete table migration for table "
            f"'{table['schema']}.{table['tablename']}'. Resuming from the phase {phase}"
        )

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    batch_dir = log_directory_path / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    migration_job_id = generate_uuid()
    report_file = batch_dir / f"{migration_job_id}-{table['schema']}-{table['tablename']}.json"
    log_file = batch_dir / f"{migration_job_id}-{table['schema']}-{table['tablename']}.log"

    migration_meta_data = get_migration_meta_data(
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

    if not index_tbspace:
        idx_exists, idx_tb_space = check_for_user_created_indexes(
            connection_details, table['tablename'], table['schema']
        )

        if idx_exists:
            selected_index_tbspace = idx_tb_space
        else:
            selected_dest_tbspace = table["tablespace"]

    else:
        selected_index_tbspace = index_tbspace[rr_index]


    adm_move_table_ops_db2woc(
        connection_details, table, phase, selected_dest_tbspace, selected_index_tbspace, copy_opts,
        runstats, report_file, log_file
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


def get_data_from_csv(csv_path):
    """_summary_

    Args:
        item (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_columns = [csv_col.upper() for csv_col in reader.fieldnames]
        unavailable_columns = [
            col for col in TABLESPACE_CSV_COLUMNS if col.upper() not in csv_columns
        ]

        if unavailable_columns:
            console.print(f"Missing columns in CSV: {', '.join(unavailable_columns)}")
            sys.exit(1)

        data = [
            {ke.strip().lower(): va.strip().upper() for ke, va in row.items() if ke and va}
            for row in reader
        ]

        return data


def validate_tables(connection_details: dict, migration_tables: List[Dict]):
    tablespaces = set()
    schemas = set()
    tablenames = set()

    for td in migration_tables:
        tablespaces.add(td["tablespace"])
        schemas.add(td["schema"])
        tablenames.add(td["tablename"])

    tablespaces = ", ".join(f"'{ts}'" for ts in (tablespaces))
    schemas = ", ".join(f"'{sc}'" for sc in (schemas))
    tablenames = ", ".join(f"'{tn}'" for tn in (tablenames))

    cnxn = db2wh_pyodbc_connection(connection_details, False)
    conn = cnxn.cursor()
    conn.execute(
        TABLE_DETAILS.format(TBSPACES=tablespaces, TABSCHEMAS=schemas, TABNAMES=tablenames)
    )
    rows = {(ts.strip(), sc.strip(), tn.strip()) for ts, sc, tn in conn.fetchall()}
    cnxn.close()

    valid_tables, invalid_tables = [], []
    for td in migration_tables:
        (
            valid_tables
            if (td["tablespace"], td["schema"], td["tablename"]) in rows
            else invalid_tables
        ).append(td)

    if invalid_tables:
        invalid_tables = ", ".join(f"{td['schema']}.{td['tablename']}" for td in invalid_tables)
        console.print(f"Invalid tables: {invalid_tables}")

    return valid_tables

def filter_migration_tables(
        connection_details, migration_tables, skip_tbspace, skip_schema, object_tablespaces
):
    available_tablespaces = get_all_tablespaces(connection_details)
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

    console.print("Filtering out invalid table.")

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
            console.print()

    print_filtered_tables("Following tables are already in object tablespace:", object_tablespace_tables)
    print_filtered_tables("Following tables are having invalid/unsupported tablespace:", invalid_tablespace_tables)
    print_filtered_tables("Following tables are having invalid/unsupported schema:", invalid_schema_tables)
    print_filtered_tables("Following tables are having skip tablespace:", skip_tablespace_tables)
    print_filtered_tables("Following tables are having skip schema:", skip_schema_tables)

    return valid_migration_tables


def export_the_data_as_csv(data: List[Dict]):
    console.print("Exporting the data into CSV")
    fieldnames = list(data[0].keys())
    filename = Path(f"db2whmigratetocos-tables-list-{datetime.now(timezone.utc).isoformat()}.csv")

    with open(filename, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    console.print(f"Data saved to CSV file: {filename.resolve()}")


def get_the_original_tablename_from_admin_move_table(connection_details, tablename):
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
    connection_string = get_connection_string(connection_details)

    cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
    conn = cnxn.cursor()
    conn.execute(ADM_MOVE_TABLE_FIND_TARGET_TABLE.format(TABLENAME=tablename))
    rows = conn.fetchall()
    cnxn.close()

    for item in rows:
        return item[0]


def get_the_rows_moved_in_admin_move_table(connection_details, schema, tablename):
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
    conn.execute(GET_THE_ROW_COUNT.format(TABLENAME=tablename, SCHEMANAME=schema))
    rows = conn.fetchall()
    cnxn.close()

    for item in rows:
        return item[0]

def get_the_rows_moved_in_admin_move_table_using_count(connection_details, schema, tablename):
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
        TABLENAME=tablename, SCHEMANAME=schema
    ))
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


def check_for_user_created_indexes(connection_details, tablename, schemaname):
    import pyodbc
    try:
        connection_string = get_connection_string(connection_details)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(GET_USER_CREATED_INDEX.format(TABLENAME=tablename,SCHEMANAME=schemaname))
        rows = conn.fetchall()

        for row in rows:
            return (True, row[0])

        return (False, None)

    except Exception as e:
        print(e)
