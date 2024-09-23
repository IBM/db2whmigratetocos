"""

    Copyright IBM Corp. 2024-2025 All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import traceback
from datetime import datetime

import os
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
import pandas as pd
import typer
from db2whmigratetocos.constants import STATUS_TABLE_HEADER
from db2whmigratetocos.db2wh_db2_utilities import check_home_path, check_if_logs_path_exist_else_create, create_a_log_directory_for_a_batch, db2wh_pyodbc_connection, export_the_data_as_csv, get_schema_in_instance, get_tables_cnt_under_tablespaces, get_tables_under_schema_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc, get_tbpsace_name_for_table, list_migration_runs, move_the_tables, parse_the_json_files_for_status, print_export_tables_in_block_and_cos, print_table_row, validate_and_get_df_from_the_csv, validate_the_input_db2_objects
from .db2whmigratetocos_install_prereq import db2whmigratetocos_init

app = typer.Typer()

console = Console()


@app.callback()
def callback():
    """
   Db2warehouse on cloud migrate to COS from Block
    """


@app.command()
def setup():
    """
    Setup the Db2 warehouse migrate tool.

    This helps in setting up the environment for the tool to run.
    This command takes care of the following.
     - Identifies the package manager, installs ODBC package.
     - Unpacks the Db2 ODBC driver and sets the PATH variables
     - Creates the directory to store logs and reports of migration runs
     - Does a final check on the setup to make sure the setup is complete

    Command :

     db2whmigratetocos setup
    """
    typer.echo("Installing the Db2 warehouse migrate tool")
    db2whmigratetocos_init()


@app.command()
def list(
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        scope: Annotated[str, typer.Option(
            help="List the tables by tablespace/schema")] = "tablespace",
        list: Annotated[str, typer.Option(
            help="all (or) list of tablespaces/schemas")] = "all",
        detail: Annotated[bool, typer.Option(
            help="List tables with its schema & size- true/false")] = False,
        export_csv: Annotated[bool, typer.Option(
            help="Export the table data into a CSV")] = False,
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001"):
    """
    List the tables in tablespaces/schemas with size
    \n
    This helps in listing the tables with schema and size in KB by Tablespace or Schema.\n
    It lists upto 75 tables for each tablespace or schema mentioned in the list variable\n
    The entire list can be exported to a csv\n
    \n
    -- scope -  tablespace/schema by which the tables needs to listed\n
    -- list  -  all/list of tablespaces/list of schema - the tables under the specified list will be listed\n
    -- detail / --no-detail - it prints the information regarding the table size, table schema \n
    -- export / --no-export - it exports the printed list to a CSV that can used for the MOVE command\n
    \n
    Command:
    \n
    db2whmigratetocos list  \n
      --scope  schema/tablespace  --list  all  \n
      --user-id user_id  --password password  --hostname  test.db2w.cloud.ibm.com \n
      --export-csv --detail \n

    """
    try:
        print()
        console.print("Test Connect to the Db2 warehouse instance")
        conn_status = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True)
        print()
        if conn_status:
            tablespace_list = []
            schema_list = []
            input_obj_list = list.split(",")
            all_objects = 'all' if 'all' in input_obj_list else None
            try:
                valid_tablespace_list = get_tablespaces_in_block_and_cos(
                    user_id, password, hostname, port, database)
            except Exception as e:
                print(
                    "unable to fetch the tablespaces, check if the instance is up and running")
                print(e)
            if scope == "tablespace":
                print()
                console.print("Listing the tablespaces")
                # validating the tablespace list
                if all_objects == 'all':
                    tablespace_list = valid_tablespace_list
                else:
                    tablespace_list = validate_the_input_db2_objects(
                        input_obj_list, valid_tablespace_list, "tablespaces")
                if tablespace_list is None:
                    print("Check if the Db2 warehouse instance is up and running")
                else:
                    if len(tablespace_list) != 0:
                        if detail is True:
                            print()
                            console.print(
                                "Gathering information about the tables in the Tablespace")
                            console.print(
                                "Displaying till 75 tables for each tablespace")
                            print()
                            tables_list_in_tablespaces = []
                            for tbspace in tablespace_list:
                                tbspace_store = " "
                                tbspace_store = "cos" if "OBJSTORE" in tbspace else "block-storage"
                                print()
                                console.rule(
                                    f"[bold orange4 italic]Tables in Tablespace - {tbspace}")
                                total_estimate, tables, table_cnt = get_tables_under_tablespace_in_db2woc(
                                    user_id, password, hostname, port, database, tbspace)
                                tb_table = Table()
                                tb_table.add_column(
                                    "Tablename", justify="center", style="cyan")
                                tb_table.add_column(
                                    "Schema", justify="center", style="cyan")
                                tb_table.add_column(
                                    "Table Size in KB", justify="center", style="cyan")
                                if len(tables) != 0:
                                    console.print(
                                        f"The total number of tables in tablespace is {table_cnt}")
                                    console.print(
                                        f"The total size of tables in tablespace is {total_estimate} KB")
                                    count = 0
                                    for table in tables:
                                        count = count+1
                                        if count <= 75:
                                            tb_table.add_row(
                                                table[0], table[1], str(table[2]))
                                        tables_list_in_tablespaces.append(
                                            [tbspace, table[0], table[1], str(table[2]), str(tbspace_store)])
                                    print()
                                    console.print(tb_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the tablespace")
                            if export_csv is True:
                                export_the_data_as_csv(
                                    tables_list_in_tablespaces, "db2whmigratetocos-tables-list-", "tablespace")
                        else:
                            print_export_tables_in_block_and_cos(
                                tablespace_list, export_csv)
                    else:
                        print()
                        print("No Tablespaces found")
            if scope == "schema":
                console.print(f"Listing the {scope}")
                try:
                    valid_schema_list = get_schema_in_instance(
                        user_id, password, hostname, port, database)
                except Exception as e:
                    print(e)
                    print(
                        "unable to fetch the schemas, check if the instance is up and running")
                if all_objects == 'all':
                    schema_list = valid_schema_list
                else:
                    schema_list = validate_the_input_db2_objects(
                        input_obj_list, valid_schema_list, "schemas")
                if schema_list is None:
                    print("Kindly check the schema list that is provided as input")
                else:
                    if len(schema_list) != 0:
                        if detail is True:
                            print()
                            console.print(
                                "Gathering the information about the tables in the schema")
                            console.print(
                                "Displaying till 75 tables for each schema")
                            print()
                            tables_in_schema = []
                            for schema in schema_list:
                                print()
                                console.rule(
                                    f"[bold red]Tables in Schema - {schema}")
                                table_cnt, total_estimate, tables = get_tables_under_schema_in_db2woc(
                                    user_id, password, hostname, port, database, schema)
                                sc_table = Table()
                                sc_table.add_column(
                                    "Tablename", justify="center", style="cyan")
                                sc_table.add_column(
                                    "Tables Size in KB", justify="center", style="cyan")
                                if len(tables) != 0:
                                    console.print(
                                        f"The total number of tables in schema is {table_cnt}")
                                    console.print(
                                        f"The total size of tables in schema is {total_estimate} KB")
                                    count = 0
                                    for table in tables:
                                        count = count+1
                                        if count <= 75:
                                            sc_table.add_row(
                                                table[0], str(table[1]))
                                        tables_in_schema.append(
                                            [schema, table[0], table[1]])
                                    console.print(sc_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the schema")
                                    print()
                            if export_csv is True:
                                export_the_data_as_csv(
                                    tables_in_schema, "db2whmigratetocos-schemas-tables-list-", "schema")
                        else:
                            schema_table = Table(show_footer=False)
                            for row in schema_list:
                                schema_table.add_row(str(row))
                            console.print(schema_table)
                            if export_csv is True:
                                console.print(
                                    "Exporting the schema list into CSV")
                                df = pd.DataFrame(
                                    schema_list, columns=["schema"])
                                filename = "db2whmigratetocos-schemas-list-"+datetime.now().isoformat()+".csv"
                                df.to_csv(filename, index=False)
                                console.print(
                                    "Exporting the list of tables in the schema")
                                print(f"Data saved to CSV file: {filename}")
            if scope != "schema" and scope != "tablespace":
                print(
                    "No suitable db2 object name is given. Kindly try giving tablespace/schema")
        else:
            print(
                "Cannot connect to the Instance. Kindly check if the status if up and running")
    except Exception as e:
        print(e)


@app.command()
def move(
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        scope: Annotated[str, typer.Option(
            help="Move tables by tablespace/schema")] = "tablespace",
        list: Annotated[str, typer.Option(
            help="Source tablespace/schema in block storage - all/comma seperated list of tablespace/schema")] = "USERSPACE1",
        dest_tbspace: Annotated[str, typer.Option(
            help="Destination tablespace in cos, where the data needs to be moved ")] = "OBJSTORESPACE1",
        user_id: Annotated[str, typer.Option(
            help="User Id to connect to Db2 warehouse Instance")] = "db2inst1",
        skip_schema: Annotated[str, typer.Option(
            help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] = "none",
        skip_tbspace: Annotated[str, typer.Option(
            help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")] = "none",
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001"):
    """
    Move the tablespaces to COS from Block
    \n
    This command helps to initiate the move of the list of tables to COS - OBJSTORESPACE.\n
    The move can be done by tablespace level or by schema level, with all/ provided list of tablespaces (or) schemas.\n
    A directory will be created for each run of the move command, to contain the logs and the report metrics.\n
    The movement status can be checked using the status comamnd - db2whmigratetocos status --help.\n
    \n
    --scope - tablespace/schema - move tables by tablespace/schema\n
    --list - all/list of tablespaces/list of schema - the tables under the specified list will be listed\n
    --dest_tablespace - OBJSTORESPACE1 - The destination tablespace in COS\n
    --skip_schema  - Skip a list of schema in the list - only used when the scope is schema\n
    --skip_tbspace - Skip a list of tablespaces in the list - only used when the scope is tablespace\n
    \n
    Command:
    \n
    db2whmigratetocos move \n
    --scope <schema> --list <list of objects>\n
    --skip-schema none --dest-tbspace OBJSTORESPACE1\n
    --user-id <user-id> --password <password> --hostname <host-name>\n

    """
    try:
        conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True)
        print()
        console.print("Test Connect to the Db2 warehouse instance")
        if conn_test:
            src_db2_obj_list = list.split(",")
            skip_tbspace_list = skip_tbspace.split(",")
            skip_schema_list = skip_schema.split(",")
            dest_tbspace_list = dest_tbspace.split(",")

            input_list_csv_check = [
                'csv' for i in src_db2_obj_list if 'csv' in i]
            if input_list_csv_check:
                input_list_csv = input_list_csv_check[0]
            else:
                input_list_csv = None
            if scope == "tablespace":
                valid_tbspace_list = get_tablespaces_in_block_and_cos(
                    user_id, password, hostname, port, database)
                all_tablespaces = 'all' if 'all' in src_db2_obj_list else None
                if input_list_csv == 'csv':
                    log_directory_name = create_a_log_directory_for_a_batch()
                    for item in src_db2_obj_list:
                        if '.csv' in item:
                            tables_in_df = validate_and_get_df_from_the_csv(
                                item)
                            if len(tables_in_df) > 0:
                                for idx, row in enumerate(tables_in_df):
                                    if row['tablespace'] in valid_tbspace_list:
                                        if row['tablespace'] not in skip_tbspace_list:
                                            if row['tablespace'] not in dest_tbspace_list:
                                                tables_in_tablespace = get_tabname_schemaname_under_tablespace_in_db2woc(
                                                    user_id, password, hostname, port, database, row['tablespace'])
                                                table_exists = False
                                                for item in tables_in_tablespace:
                                                    if row['tablename'] == item[0]:
                                                        table_exists = True
                                                if table_exists:
                                                    selected_dest_tbspace = idx % len(
                                                        dest_tbspace_list)
                                                    move_the_tables(row['schema'], row['Tablename'], row['Tablespace'], dest_tbspace_list[selected_dest_tbspace],
                                                                    log_directory_name, user_id, password, hostname, port, database)
                                                else:
                                                    print(
                                                        "Table not found in the tablespace")
                                            else:
                                                print(
                                                    "The source and the destination tablespace are same")
                                        else:
                                            print(
                                                "skipping the tablespace as per the input")
                                    else:
                                        print("the tablespace name is invalid")
                else:
                    if all_tablespaces == 'all':
                        tbspace_list = valid_tbspace_list
                    else:
                        tbspace_list = validate_the_input_db2_objects(
                            src_db2_obj_list, valid_tbspace_list, "tablespaces")
                    log_directory_name = create_a_log_directory_for_a_batch()
                    for tbspace in tbspace_list:
                        tables_in_userspace = []
                        if tbspace not in skip_tbspace_list:
                            if tbspace != dest_tbspace:
                                tables_in_userspace = get_tabname_schemaname_under_tablespace_in_db2woc(
                                    user_id, password, hostname, port, database, tbspace)
                                print(
                                    "Initiating the migration for each of the table, proceeding with next steps....")
                                tables_cnt = len(tables_in_userspace)
                                if len(tables_in_userspace) != 0:
                                    for idx, items in enumerate(tables_in_userspace):
                                        selected_dest_tbspace = idx % len(
                                            dest_tbspace_list)
                                        move_the_tables(items[1], items[0], tbspace, dest_tbspace_list[selected_dest_tbspace],
                                                        log_directory_name, user_id, password, hostname, port, database)
                                if len(tables_in_userspace) == 0:
                                    print("No tables found in the tablespace")
                            else:
                                print("The source and the destination are same")
                        else:
                            print("Skipping the tablespace - " + tbspace)
            if scope == "schema":
                valid_schema_list = get_schema_in_instance(
                    user_id, password, hostname, port, database)
                all_schemas = 'all' if 'all' in src_db2_obj_list else None
                if valid_schema_list is not None:
                    if input_list_csv == 'csv':
                        log_directory_name = create_a_log_directory_for_a_batch()
                        for item in src_db2_obj_list:
                            if '.csv' in item:
                                tables_in_df = validate_and_get_df_from_the_csv(
                                    item)
                                for idx, row in tables_in_df:
                                    if row['schema'] in valid_schema_list:
                                        if row['schema'] not in skip_schema_list:
                                            source_tablespace = get_tbpsace_name_for_table(
                                                user_id, password, hostname, port, database, row['Tablename'])
                                            tables_in_schema = get_tables_under_schema_in_db2woc(
                                                user_id, password, hostname, port, database, row['schema'])
                                            table_exists = False
                                            for item in tables_in_schema:
                                                if row['tablename'] == item[0]:
                                                    table_exists = True
                                            if table_exists:
                                                selected_dest_tbspace = idx % len(
                                                    dest_tbspace_list)
                                                if source_tablespace not in dest_tbspace_list:
                                                    move_the_tables(row['schema'], row['tablename'], source_tablespace, dest_tbspace_list[selected_dest_tbspace],
                                                                    log_directory_name, user_id, password, hostname, port, database)
                                                else:
                                                    print(
                                                        "The source and the destination are same")
                                            else:
                                                print(
                                                    "Skipping as table do not exist")
                                        else:
                                            print(
                                                "skipping the schema as per the input")
                                    else:
                                        print(
                                            "The specified schema is not valid")
                            else:
                                print("kindly check if the file exists in path")
                    else:
                        # validation of schema and setting the list for movment
                        print(valid_schema_list)
                        if all_schemas == 'all':
                            schema_list = valid_schema_list
                        else:
                            schema_list = validate_the_input_db2_objects(
                                src_db2_obj_list, valid_schema_list, "schemas")
                        for schema in schema_list:
                            tables_in_schema = []
                            if schema not in skip_schema_list:
                                tables_cnt, tota_size, tables_in_schema = get_tables_under_schema_in_db2woc(
                                    user_id, password, hostname, port, database, schema)
                                print(tables_cnt)
                                print(tota_size)
                                print(
                                    "Initiating the migration for each of the table, proceeding with next steps....")
                                if len(tables_in_schema) != 0:
                                    log_directory_name = create_a_log_directory_for_a_batch()
                                    for idx, item in tables_in_schema:
                                        selected_dest_tbspace = idx % len(
                                            dest_tbspace_list)
                                        source_tablespace = get_tbpsace_name_for_table(
                                            user_id, password, hostname, port, database, item[0])
                                        if source_tablespace not in dest_tbspace_list:
                                            move_the_tables(
                                                schema, item[0], source_tablespace, dest_tbspace_list[selected_dest_tbspace], log_directory_name, user_id, password, hostname, port, database)
                                if len(tables_in_schema) == 0:
                                    print("no tables found in the schema")
                            else:
                                print("Skipping the schema - " + schema)
                else:
                    print("Kindly check the schema list that is provided as input")
        else:
            print("Kindly check if the Db2 warehouse Instance is up and runnning")
    except Exception as e:
        print(e)
        print(traceback.format_exc())


@ app.command()
def status(
        scope: Annotated[str, typer.Option(help="tables - lists the no of tables in block & COS;migration-runs - migration runs that ran till now")],
        active_runs: Annotated[bool, typer.Option(help="active - lists the active migration runs;completed - lists the completed migration runs")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001"):
    '''
    Status and the metrics of the migration jobs

    The command is used to fetch the details about the tables in block and cos
    It can give the details and the status of a migration runs

    command:
     db2whmigratetocos status
     --scope migration-runs/tables
     --user-id <user-id> --password <password> --hostname <host-name>

    '''
    try:
        tables_in_block = []
        tables_in_cos = []
        total_tables_in_block = 0
        home = check_home_path()
        path = home.strip()+"/db2whmigratetocos-logs"
        tablespaces_in_instance = get_tablespaces_in_block_and_cos(
            user_id, password, hostname, port, database)
        if len(tablespaces_in_instance) != 0:
            for tablespace in tablespaces_in_instance:
                table_in_tbspace = get_tables_cnt_under_tablespaces(
                    user_id, password, hostname, port, database, tablespace)
                if "OBJ" not in tablespace:
                    total_tables_in_block = total_tables_in_block + table_in_tbspace
                    tables_in_block.append([tablespace, table_in_tbspace])
                else:
                    tables_in_cos.append([tablespace, table_in_tbspace])
        if scope == "tables":
            console.rule("[bold red]Tablespaces in Block")
            print_tables_in_block = print_table_row(tables_in_block)
            print(print_tables_in_block)
            console.rule("[bold red]Tablespaces in COS")
            print_in_tables_in_cos = print_table_row(tables_in_cos)
            print(print_in_tables_in_cos)
        if scope == "migration-runs":
            console.rule("[bold red]Migration Runs")
            print(
                "To check the complete logs and metrics,please find the log file in the respective location:")
            print(path+"/<batch-id>/<job-id>-<table-name>.json")
            print(path+"/<batch-id>/<job-id>-<table-name>.log")
            print()
            is_exist = os.path.exists(path)
            active_migration_job_details = []
            completed_migration_job_details = []
            if is_exist:
                migration_batches = os.listdir(path)
                if len(migration_batches) != 0:
                    active_migration_job_details, completed_migration_job_details = list_migration_runs(
                        migration_batches, path)
                    if active_runs is True:
                        if len(active_migration_job_details) != 0:
                            tb_table_migration_runs = parse_the_json_files_for_status(
                                active_migration_job_details, user_id, password, hostname, port, database, STATUS_TABLE_HEADER, active_runs)
                            console.print(tb_table_migration_runs)
                        else:
                            print("No active migration runs yet in the instance")
                    else:
                        if len(completed_migration_job_details) != 0:
                            tb_table_migration_runs = parse_the_json_files_for_status(
                                completed_migration_job_details, user_id, password, hostname, port, database, STATUS_TABLE_HEADER, active_runs)
                            console.print(tb_table_migration_runs)
                        else:
                            print("No migration runs yet in the instance")
                else:
                    print("No migration runs yet in the instance")
            else:
                print("The logs folder is not present")
                print("Creating the log folder")
                check_if_logs_path_exist_else_create()
    except Exception as e:
        print(e)
