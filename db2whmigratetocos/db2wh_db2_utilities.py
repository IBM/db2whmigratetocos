"""

    Copyright IBM Corp. 2024-2025 All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import subprocess
import os
import uuid

from rich.console import Console


from db2whmigratetocos.queries import ADM_MOVE_TABLE_FIND_PHASE, LIST_SCHEMAS, LIST_TABLES_IN_SCHEMA, LIST_TABLES_IN_TSPACE, LIST_TBSPACE_BY_TABNAME, LIST_TBSPACES, TAB_SIZE


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
    """_summary_

    Args:
        command (str): _description_

    Returns:
        _type_: _description_
    """
    result = subprocess.check_output(command, shell=True, text=True)
    return result


# db2 utility functions

def get_tablespaces_in_block_and_cos(user: str, password: str, hostname: str, port: str, database: str):
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
        user_tablespaces_list = []
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACES)
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if "SYS" not in item[0] and "TS4CONSOLE" not in item[0] and "BIGSQLCATUTILITY" not in item[0] and "TEMP" not in item[0] and "TMP" not in item[0]:
                user_tablespaces_list.append(item[0])
        return user_tablespaces_list
    except Exception as e:
        print(e)


def get_schema_in_instance(user: str, password: str, hostname: str, port: str, database: str):
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
            user, password, hostname, port, database, False)
        conn = cnxn.cursor()
        conn.execute(LIST_SCHEMAS)
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if "SYS" not in item[0] and "NULL" not in item[0] and "SQL" not in item[0] and "IBMPDQ" not in item[0] and "DEFAULT" not in item[0]:
                user_schemas_list.append(item[0])
        return user_schemas_list
    except Exception as e:
        print(e)


def get_tables_under_schema_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, schemaname: str):
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
            user, password, hostname, port, database, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_SCHEMA.format(SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = len(rows)
        for item in rows:
            est_size = " "
            est_size = tab_size_by_table_name(
                user, password, hostname, port, database, schemaname, item[0])
            total_estimate_size += int(est_size)
            tables_in_schema.append([item[0], est_size])
        return table_cnt, total_estimate_size, tables_in_schema
    except Exception as e:
        print(e)


def tab_size_by_table_name(user: str, password: str, hostname: str, port: str, database: str, schemaname: str, tablename: str):
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
            user, password, hostname, port, database, False)
        conn = cnxn.cursor()
        conn.execute(TAB_SIZE.format(TABSCHEMA=schemaname, TABNAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            return int(item[0])+int(item[1])+int(item[2])+int(item[3])+int(item[4])+int(item[5])
    except Exception as e:
        print(e)


def get_tables_under_tablespace_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str):
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
            user, password, hostname, port, database, False)
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
                            user, password, hostname, port, database, item[1], item[0])
                        total_estimate_size += int(est_size)
                        table_names_in_tablespace.append(
                            [item[0], item[1], est_size])
        return total_estimate_size, table_names_in_tablespace, table_cnt
    except Exception as e:
        print(e)


def get_tables_cnt_under_tablespaces(user: str, password: str, hostname: str, port: str, database: str, tablespace: str):
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
            user, password, hostname, port, database, False)
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


def get_tabname_schemaname_under_tablespace_in_db2woc(user: str, password: str, hostname: str, port: str, database: str, tablespace: str):
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
            user, password, hostname, port, database, False)
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


def get_tbpsace_name_for_table(user: str, password: str, hostname: str, port: str, database: str, tablename: str):
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
            user, password, hostname, port, database)
        tablespace_name = " "
        cnxn = db2wh_pyodbc_connection(
            user, password, hostname, port, database, False)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACE_BY_TABNAME.format(TABNAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        print(rows)
        for item in rows:
            if item[0] in valid_tablespace_list:
                tablespace_name = item[0]
        return tablespace_name
    except Exception as e:
        print(e)

# pyodbc connection fucntions


def get_connection_string(user: str, password: str, hostname: str, port: str, database: str):
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


def db2wh_pyodbc_connection(user: str, password: str, hostname: str, port: str, database: str, test_con: bool) -> bool:
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
            user, password, hostname, port, database)
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


def check_if_logs_path_exist_else_create():
    """_summary_

    Returns:
        _type_: _description_
    """
    try:
        home = check_home_path()
        path = home.strip()+"/db2whmigratetocos-logs"
        is_exist = os.path.exists(path)
        print(is_exist)
        if is_exist:
            return path
        else:
            os.makedirs(path, exist_ok=True)
            return path
    except Exception as e:
        print(e)


def create_log_directory_for_migration_run(directory_name: str):
    """_summary_

    Args:
        directory_name (str): _description_

    Returns:
        _type_: _description_
    """
    try:
        directory_path = check_if_logs_path_exist_else_create()
        migration_sub_directory = str(
            directory_path)+"/"+directory_name.strip()
        os.makedirs(migration_sub_directory)
        return migration_sub_directory
    except Exception as e:
        print(e)


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
            "status": "REQUESTED TO" + status,
            "table_name": tablename,
            "schema_name": schemaname,
            "phase_logs": [],
        }
        return migration_meta_data
    except Exception as e:
        print(e)


def find_adm_status_by_tablename(user: str, password: str, hostname: str, port: str, database: str, tablename: str):
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
            user, password, hostname, port, database)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        for item in rows:
            table_phase = item[0]
            return table_phase
    except Exception as e:
        print(e)
