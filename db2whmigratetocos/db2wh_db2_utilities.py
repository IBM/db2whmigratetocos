"""

    Copyright IBM Corp. 2024  All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import csv
import json
import subprocess
import os
import sys
import uuid
import math
from datetime import datetime
import pandas as pd
from rich.table import Table
from rich.console import Console
from db2whmigratetocos.admin_move_table_func import adm_move_table_ops_db2woc
from db2whmigratetocos.constants import SCHEMA_CSV_COLUMNS, TABLESPACE_CSV_COLUMNS
from db2whmigratetocos.queries import ADM_MOVE_TABLE_FIND_PHASE, GET_OBJECTSPACE_USING_SGNAME, GET_STORAGE_PATH_DEFINED_IN_INSTANCE, GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY, GET_USER_CREATED_INDEX, LIST_SCHEMAS, LIST_TABLES_IN_SCHEMA, LIST_TABLES_IN_TSPACE, LIST_TBSPACE_BY_TABNAME, LIST_TBSPACES, TAB_SIZE, GET_THE_ROW_COUNT, ADM_MOVE_TABLE_FIND_TARGET_TABLE


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

def get_tablespaces_in_block_and_cos(user: str, password: str, hostname: str, port: str, database: str,dsn:str):
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
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACES)
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if "SYS" not in item[0] and "TS4CONSOLE" not in item[0] and "TS4MONITOR" not in item[0]   and "BIGSQLCATUTILITY" not in item[0] and "TEMP" not in item[0] and "TMP" not in item[0]:
                user_tablespaces_list.append(item[0])
        return user_tablespaces_list
    except Exception as e:
        print(e)


def get_schema_in_instance(user: str, password: str, hostname: str, port: str, database: str,dsn:str):
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
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(LIST_SCHEMAS)
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if "SYS" not in item[0] and "NULL" not in item[0] and "TS4" not in item[0] and "SQL" not in item[0] and "IBMPDQ" not in item[0] and "DEFAULT" not in item[0] and  "IBM_RTMON" not in item[0] and "IBMCONSOLE" not in item[0]:
                user_schemas_list.append(item[0].strip())
        return user_schemas_list
    except Exception as e:
        print(e)


def get_tables_under_schema_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, schemaname: str,dsn:str):
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
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_SCHEMA.format(SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = len(rows)
        for item in rows:
            if str(item[0]).endswith('t') is False: 
                est_size = " "
                est_size = tab_size_by_table_name(
                    user, password, hostname, port, database, schemaname, item[0],dsn)
                total_estimate_size += int(est_size)
                tables_in_schema.append([item[0], est_size])
        return table_cnt, total_estimate_size, tables_in_schema
    except Exception as e:
        print(e)

def get_tables_under_schem_notabsize_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, schemaname: str,dsn:str):
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
            user, password, hostname, port, database, False,dsn)
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



def tab_size_by_table_name(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, tablename: str,dsn:str):
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
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(TAB_SIZE.format(TABSCHEMA=schemaname, TABNAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            return int(item[0])+int(item[1])+int(item[2])+int(item[3])+int(item[4])+int(item[5])
    except Exception as e:
        print(e)


def get_tables_under_tablespace_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str,dsn:str):
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
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        table_cnt = 0
        total_estimate_size = 0
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()
        with console.status(""):
            for item in rows:
                if "SYS" not in item[1]:
                    if str(item[0]).endswith('t') is False:
                        table_cnt = table_cnt + 1
                        est_size = tab_size_by_table_name(
                            user, password, hostname, port, database, item[1], item[0],dsn)
                        total_estimate_size += int(est_size)
                        table_names_in_tablespace.append(
                            [item[0], item[1], est_size])
        return total_estimate_size, table_names_in_tablespace, table_cnt
    except Exception as e:
        print(e)

def get_tables_under_tablespace_no_tabsize_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str,dsn:str):
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
            user, password, hostname, port, database, False,dsn)
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


def get_tables_cnt_under_tablespaces(user: str, password: str, hostname: str, port: str, database: str, tablespace: str,dsn:str):
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
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = 0
        with console.status(""):
            for item in rows:
                if "SYS" not in item[1]:
                    if str(item[0]).endswith('t') is False:
                        table_cnt = table_cnt + 1
        return table_cnt
    except Exception as e:
        print(e)


def get_tabname_schemaname_under_tablespace_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str,dsn:str):
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
            user, password, hostname, port, database, False,dsn)
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


def get_tbpsace_name_for_table(user: str, password: str, hostname: str, port: str, database: str, tablename: str, schemaname: str,dsn:str):
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
        valid_tablespace_list = get_tablespaces_in_block_and_cos(
            user, password, hostname, port, database,dsn)
        tablespace_name = " "
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False,dsn)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACE_BY_TABNAME.format(
            TABNAME=tablename, SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if item[0] in valid_tablespace_list:
                tablespace_name = item[0]
        return tablespace_name
    except Exception as e:
        print(e)

# pyodbc connection fucntions


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
    if dsn is not  None:
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


def db2wh_pyodbc_connection(user: str, password: str, hostname: str, port: str, database: str, test_con: bool,dsn:str) -> bool:
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
        connection_string = get_connection_string(
            user, password, hostname, port, database,dsn)
        cnxn = pyodbc.connect(connection_string)
        if test_con:
            try:
                conn = cnxn.cursor()
                conn.execute(LIST_TBSPACES)
                conn.fetchall()
                print(
                    "Connected to the Instance - {hostname}".format(hostname=hostname))
                print("Test Connection Successful")
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
        os.makedirs(migration_sub_directory)
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


def create_file_for_the_table_migration(directory_name: str, file_name: str):
    """_summary_

    Args:
        directory_name (str): _description_
        file_name (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        run_command('''
                     cd {LOG_DIRECTORY_NAME}
                     touch {FILE_NAME}
                    '''.format(LOG_DIRECTORY_NAME=directory_name, FILE_NAME=file_name))
        is_exist = os.path.exists(directory_name+"/"+file_name)
        if is_exist:
            return True
    except Exception as e:
        print(e)


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


def get_json_format_for_migration_run(schemaname: str, tablename: str, status: str, src_tbspace: str, dest_tbspace: str, migration_job_id: str):
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
    try:
        migration_meta_data = {
            "migration_job_id": migration_job_id,
            "source_tablespace": src_tbspace,
            "destination_tablespace": dest_tbspace,
            "status": "REQUESTED TO " + status,
            "table_name": tablename,
            "schema_name": schemaname,
            "phase_logs": [],
        }
        return migration_meta_data
    except Exception as e:
        print(e)


def find_adm_status_by_tablename(user: str, password: str, hostname: str, port: str, database: str, tablename: str,dsn:str):
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
            user, password, hostname, port, database,dsn)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        for item in rows:
            table_phase = item[0]
            return table_phase
    except Exception as e:
        print(e)


# status utilities

def print_table_row(tables) -> Table:
    """_summary_

    Args:
        tables (_type_): _description_

    Returns:
        Table: _description_
    """
    tb_table = Table()
    tb_table.add_column("Tablespace", justify="center", style="cyan")
    tb_table.add_column("Table count", justify="center", style="cyan")
    for tablespace in tables:
        tb_table.add_row(tablespace[0], str(tablespace[1]))
    return tb_table


def list_migration_runs(migration_batches, path):
    """_summary_

    Args:
        migration_batches (_type_): _description_
    """
    active_migration_job_details = []
    completed_migration_job_details = []
    for batch in migration_batches:
        migration_runs_path = path+"/"+batch
        migration_runs = os.listdir(migration_runs_path)
        if len(migration_runs) > 0:
            for migration_run in migration_runs:
                if ".json" in migration_run:
                    jfile = open(migration_runs_path+"/" +
                                 migration_run, "r", encoding='utf-8')
                    data = json.load(jfile)
                    data['batch_id'] = batch
                    if data['status'] != "COMPLETE":
                        active_migration_job_details.append(data)
                    else:
                        completed_migration_job_details.append(data)
    return active_migration_job_details, completed_migration_job_details


def parse_the_json_files_for_status(migration_job_details: list, user_id: str, password: str, hostname: str, port: str, database: str, table_header: list, active: bool,dsn:str) -> Table:
    """_summary_

    Args:
        migration_job_details (_type_): _description_

    Returns:
        Table: _description_
    """
    tb_table = Table()
    for table_column_name in table_header:
        tb_table.add_column(table_column_name, justify="center", style="cyan")
    for details in migration_job_details:
        init_time = " "
        end_time = " "
        init_bool = False
        end_bool = False
        phase_status = ""
        time_taken = "-"
        init_start = " "
        cleanup_end = " "
        original_rows = 0
        target_rows = 0
        progress = 0
        if len(details['phase_logs']) > 0:
            for phase in details['phase_logs']:
                if phase['STATUS'] != 'COMPLETE' and phase['STATUS'] != 'INPROGRESS':
                    phase_status = find_adm_status_by_tablename(
                        user_id, password, hostname, port, database, str(details['table_name']),dsn)
                else:
                    phase_status = details['status']
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
            phase_status = details['status'] 
        if active is True:
            if phase_status != "COMPLETE" and "REQUESTED TO" not in phase_status and  "ERROR" not in phase_status:
                target_table_name = get_the_original_tablename_from_admin_move_table(
                    details['table_name'], user_id, password, hostname, port, database,dsn)
                target_rows = get_the_rows_moved_in_admin_move_table(
                    details['schema_name'], target_table_name, user_id, password, hostname, port, database,dsn)
                original_rows = get_the_rows_moved_in_admin_move_table_using_count( details['schema_name'], details['table_name'], user_id, password, hostname, port, database,dsn)
                if original_rows == 0 or original_rows is None:
                    original_rows = get_the_rows_moved_in_admin_move_table(
                    details['schema_name'], details['table_name'], user_id, password, hostname, port, database,dsn)
                if target_rows is not None and original_rows is not None and int(original_rows) != 0:
                  if target_rows <=original_rows:
                    progress = str(math.ceil((100 - ((int(original_rows) - int(target_rows))/int(original_rows)) * 100))) + " %"
                  else:
                    progress = "TABLE_WRITE - Target "+ str(target_rows)
                tb_table.add_row(str(details['batch_id']), str(details['migration_job_id']), str(details['table_name']), details['schema_name'],
                                 phase_status, details['source_tablespace'], details['destination_tablespace'], str(progress))
        else:
            if phase_status == 'COMPLETE':
                tb_table.add_row(str(details['batch_id']), str(details['migration_job_id']), str(
                    details['table_name']), details['schema_name'], phase_status, details['source_tablespace'], details['destination_tablespace'], time_taken)
    return tb_table


# move utilities

def move_the_tables(schema, tablename, source_tablespace, dest_tbspace, log_directory_name, user_id, password, hostname, port, database,dsn,index_tbspace,copy_opts,runstats):
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
    migration_job_id = generate_uuid()
    migration_table_details = get_json_format_for_migration_run(
        schema, tablename, "INIT", source_tablespace, dest_tbspace, str(migration_job_id))
    report_file_name_for_the_table = migration_job_id + "-"+tablename+".json"
    std_output_name_for_the_file = migration_job_id + "-"+tablename+".log"
    file_creation_done = create_file_for_the_table_migration(
        log_directory_name, report_file_name_for_the_table)
    std_log_creation_done = create_file_for_the_table_migration(
        log_directory_name, std_output_name_for_the_file)
    if file_creation_done:
        with open(log_directory_name+"/"+report_file_name_for_the_table, 'w', encoding='utf-8') as f:
            json.dump(migration_table_details, f, indent=6)
    if std_log_creation_done:
        print("Table Name " + tablename)
        print("Migration ID " + migration_job_id)
        print("Reports in " + log_directory_name +
              "/"+report_file_name_for_the_table)
        print("Logs in " + log_directory_name+"/"+std_output_name_for_the_file)
        adm_move_table_ops_db2woc(user_id, password, hostname, port, database, schema, tablename, "INIT", source_tablespace,
                                  dest_tbspace, log_directory_name+"/"+report_file_name_for_the_table, log_directory_name+"/"+std_output_name_for_the_file,dsn,index_tbspace,copy_opts,runstats)


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


def validate_and_get_df_from_the_csv(item):
    """_summary_

    Args:
        item (_type_): _description_

    Returns:
        _type_: _description_
    """
    invalid_csv_column = []
    csv_file_exists = os.path.isfile(item)
    if csv_file_exists:
        with open(item, encoding='utf-8') as csv_file:
            column_reader = csv.reader(csv_file, delimiter=",")
            for row in column_reader:
                tables_column = row
                break
        for column in tables_column:
            if column not in TABLESPACE_CSV_COLUMNS:
                invalid_csv_column.append(column)
        if len(invalid_csv_column) == 0:
            with open(item, encoding='utf-8') as f:
                table_csv_reader = csv.DictReader(f)
                tables_in_df = [row for row in table_csv_reader]
                return tables_in_df
        else:
            print("Identified invalid column names in the CSV")
            print(invalid_csv_column)
            sys.exit(0)
    else:
        print("Kindly check the if the file path provided is correct")
        sys.exit(0)




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
        "TABLESPACES in Block", justify="center", style="cyan", no_wrap=True)
    tbs_cos_table.add_column(
        "TABLESPACES in COS", justify="center", style="cyan", no_wrap=True)
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


def export_the_data_as_csv(tables, filename_prefix):
    console.print("Exporting the data into CSV")
    columns = TABLESPACE_CSV_COLUMNS
    df = pd.DataFrame(tables, columns=columns)
    filename = filename_prefix + datetime.now().isoformat()+".csv"
    df.to_csv(filename, index=False)
    print(f"Data saved to CSV file: {filename}")
    return filename


def get_the_original_tablename_from_admin_move_table(tablename, user_id, password, hostname, port, database,dsn):
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
        user_id, password, hostname, port, database,dsn)
    cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
    conn = cnxn.cursor()
    conn.execute(ADM_MOVE_TABLE_FIND_TARGET_TABLE.format(
        TABLENAME=tablename))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]


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
        user_id, password, hostname, port, database, False,dsn)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))

    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def get_the_rows_moved_in_admin_move_table_using_count(schemaname, tablename, user_id, password, hostname, port, database,dsn):
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
        user_id, password, hostname, port, database, False,dsn)
    conn = cnxn.cursor()
    conn.execute(GET_THE_ROW_COUNT_FROM_TABLE_AFTER_COPY.format(
        TABLENAME=tablename, SCHEMANAME=schemaname))
    rows = conn.fetchall()
    cnxn.close()
    for item in rows:
        return item[0]

def get_list_of_objectspaces(user_id, password, hostname, port, database,dsn:str):
    """

    Returns:
        _type_: _description_
    """
    try:
        object_space_list = []
        cnxn = db2wh_pyodbc_connection(
                user_id, password, hostname, port, database, False,dsn)
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
    except Exception as e:
        print(e)


def check_for_user_created_indexes(user_id, password, hostname, port, database,tablename,schemaname,dsn:str):
    import pyodbc
    try:
        connection_string = get_connection_string(
            user_id, password, hostname, port, database,dsn)
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
