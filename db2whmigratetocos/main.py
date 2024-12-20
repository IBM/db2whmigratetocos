"""

    Copyright IBM Corp. 2024 All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import sys
import traceback
from datetime import datetime

import os
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
import pandas as pd
import typer
from db2whmigratetocos.admin_move_table_func import cancel_terminate_admin_move_table
from db2whmigratetocos.constants import COPY_OPTIONS, STATUS_TABLE_HEADER, STATUS_TABLE_HEADER_ACTIVE_RUNS
from db2whmigratetocos.db2wh_db2_utilities import check_for_user_created_indexes, check_home_path, check_if_logs_path_exist_else_create, create_a_log_directory_for_a_batch, db2wh_pyodbc_connection, export_the_data_as_csv, get_list_of_objectspaces, get_schema_in_instance, get_tables_cnt_under_tablespaces, get_tables_under_schem_notabsize_in_db2woc, get_tables_under_schema_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tables_under_tablespace_no_tabsize_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc, get_tbpsace_name_for_table, list_migration_runs, move_the_tables, parse_the_json_files_for_status, print_export_tables_in_block_and_cos, print_table_row, validate_and_get_df_from_the_csv, validate_the_input_db2_objects
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
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
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
    -- detail / --no-detail - it prints the information regarding the table size, tablename and  schema \n
    -- export / --no-export - it exports the printed list to a CSV that can used for the MOVE command\n
    --dsn -  Pass the DSN name if it is already configured
    \n
    Command:
    \n
    db2whmigratetocos list  \n
      --scope  schema/tablespace  --list  all  \n
      --user-id user_id  --password password  --hostname  test.db2w.cloud.ibm.com \n
      --export-csv --detail --dsn \n

    """
    try:
        print()
        valid_dsn =  None
        console.print("Test Connect to the Db2 warehouse instance")
        if dsn is not None:
         valid_dsn = dsn
         conn_status = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
        else:
            valid_dsn = None
            conn_status = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
        print()
        if conn_status:
            tablespace_list = []
            schema_list = []
            input_obj_list = list.split(",")
            all_objects = 'all' if 'all' in input_obj_list else None
            try:
                valid_tablespace_list = get_tablespaces_in_block_and_cos(
                    user_id, password, hostname, port, database,valid_dsn)
            except Exception as e:
                print(
                    "unable to fetch the tablespaces, check if the instance is up and running")
                print(e)
            if scope == "tablespace":
                print()
                console.print("Listing the tablespaces")
                object_space_list = get_list_of_objectspaces(user_id,password,hostname,port,database,valid_dsn)
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
                                if len(object_space_list) != 0:
                                    tbspace_store = "cos" if tbspace in object_space_list else "block-storage"
                                print()
                                console.rule(
                                    f"[bold orange4 italic]Tables in Tablespace - {tbspace}")
                                total_estimate, tables, table_cnt = get_tables_under_tablespace_in_db2woc(
                                    user_id, password, hostname, port, database, tbspace,valid_dsn)
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
                               filename =  export_the_data_as_csv(
                                    tables_list_in_tablespaces, "db2whmigratetocos-tables-list-")
                               return filename
                        else:
                            tables_list_in_tablespaces = []
                            for tbspace in tablespace_list:
                                tbspace_store = " "
                                if len(object_space_list) != 0:
                                    tbspace_store = "cos" if tbspace in object_space_list else "block-storage"
                                print()
                                console.rule(
                                    f"[bold orange4 italic]Tables in Tablespace - {tbspace}")
                                tables, table_cnt = get_tables_under_tablespace_no_tabsize_in_db2woc(
                                    user_id, password, hostname, port, database, tbspace,valid_dsn)
                                tb_table = Table()
                                tb_table.add_column(
                                    "Tablename", justify="center", style="cyan")
                                tb_table.add_column(
                                    "Schema", justify="center", style="cyan")
                                if len(tables) != 0:
                                    console.print(
                                        f"The total number of tables in tablespace is {table_cnt}")
                                    count = 0
                                    for table in tables:
                                        count = count+1
                                        if count <= 75:
                                            tb_table.add_row(
                                                table[0], table[1])
                                        tables_list_in_tablespaces.append(
                                            [tbspace, table[0], table[1]," ",str(tbspace_store)])
                                    print()
                                    console.print(tb_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the tablespace")
                            if export_csv is True:
                               filename =  export_the_data_as_csv(
                                    tables_list_in_tablespaces, "db2whmigratetocos-tables-list-nodetail-")
                               return filename
                    else:
                        print()
                        print("No Tablespaces found")
            if scope == "schema":
                console.print(f"Listing the {scope}")
                try:
                    valid_schema_list = get_schema_in_instance(
                        user_id, password, hostname, port, database,valid_dsn)
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
                            object_space_list = get_list_of_objectspaces(user_id,password,hostname,port,database,valid_dsn)

                            for schema in schema_list:
                                print()
                                console.rule(
                                    f"[bold orange4 italic]Tables in Schema - {schema}")
                                table_cnt, total_estimate, tables = get_tables_under_schema_in_db2woc(
                                    user_id, password, hostname, port, database, schema,valid_dsn)
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
                                        tbspace_store = ""
                                        table_tablespace = get_tbpsace_name_for_table(
                                            user_id, password, hostname, port, database, table[0], schema,valid_dsn)
                                        if len(object_space_list) != 0:
                                                 tbspace_store = "cos" if table_tablespace in object_space_list else "block-storage"
                                        count = count+1
                                        if count <= 75:
                                            sc_table.add_row(
                                                table[0], str(table[1]))
                                        tables_in_schema.append(
                                            [table_tablespace, table[0], schema, table[1], str(tbspace_store)])
                                    console.print(sc_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the schema")
                                    print()
                            if export_csv is True:
                                filename = export_the_data_as_csv(
                                    tables_in_schema, "db2whmigratetocos-schemas-tables-list-")
                                return filename
                        else:
                            print()
                            console.print(
                                "Gathering the information about the tables in the schema")
                            console.print(
                                "Displaying till 75 tables for each schema")
                            print()
                            tables_in_schema = []
                            object_space_list = get_list_of_objectspaces(user_id,password,hostname,port,database,valid_dsn)
                            for schema in schema_list:
                                print()
                                console.rule(
                                    f"[bold orange4 italic]Tables in Schema - {schema}")
                                table_cnt,tables = get_tables_under_schem_notabsize_in_db2woc(
                                    user_id, password, hostname, port, database, schema,valid_dsn)
                                sc_table = Table()
                                sc_table.add_column(
                                    "Tablename", justify="center", style="cyan")
                                sc_table.add_column(
                                    "Schemaname", justify="center", style="cyan")
                                if len(tables) != 0:
                                    console.print(
                                        f"The total number of tables in schema is {table_cnt}")
                                    count = 0
                                    for table in tables:
                                        tbspace_store = ""
                                        table_tablespace = get_tbpsace_name_for_table(
                                            user_id, password, hostname, port, database, table[0], schema,valid_dsn)
                                        if len(object_space_list) != 0:
                                                 tbspace_store = "cos" if table_tablespace in object_space_list else "block-storage"
                                        count = count+1
                                        if count <= 75:
                                            sc_table.add_row(
                                                table[0],schema)
                                        tables_in_schema.append(
                                            [table_tablespace, table[0], schema," ",str(tbspace_store)])
                                    console.print(sc_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the schema")
                                    print()
                            if export_csv is True:
                                filename = export_the_data_as_csv(
                                    tables_in_schema, "db2whmigratetocos-schemas-tables-list-nodetail")
                                return filename
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
        list: Annotated[str, typer.Option(
            help="Source tablespace/schema in block storage - all/comma seperated list of tablespace/schema")] = None,
        csv_input: Annotated[str, typer.Option(
            help="CSV file as input to the move command as .csv file without the path")] = None,
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name configured in ODBC Driver Config File (odbcinst.ini)")] = None,
        log_directory_path: Annotated[str, typer.Option(
            help="Pass the log directory base path to store the log files")] = None,
        scope: Annotated[str, typer.Option(
            help="Move tables by tablespace/schema")] = "tablespace",
        schema_name: Annotated[str, typer.Option(
            help="Provide the schema name when moving a single table")] = None,
        runstats: Annotated[bool, typer.Option(
            help="Provide the schema name when moving a single table")] = False,
        table_name: Annotated[str, typer.Option(
            help="Move tables by tablespace/schema")] = None,
        dest_tbspace: Annotated[str, typer.Option(
            help="Destination tablespace in cos, where the data needs to be moved ")] = "OBJSTORESPACE1",
        index_tbspace : Annotated[str, typer.Option(
            help="Destination tablespace in cos, where the data needs to be moved ")] = None,
        copy_opts: Annotated[str, typer.Option(
            help="Destination tablespace in cos, where the data needs to be moved ")] = "COPY_USE_OTA,NO_STATS",
        user_id: Annotated[str, typer.Option(
            help="User Id to connect to Db2 warehouse Instance")] = "db2inst1",
        skip_schema: Annotated[str, typer.Option(
            help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] = None,
        skip_tbspace: Annotated[str, typer.Option(
            help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")] =None,
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
    --skip-schema  - Skip a list of schema in the list - only used when the scope is schema\n
    --skip-tbspace - Skip a list of tablespaces in the list - only used when the scope is tablespace\n
    --csv-input - Give the generated CSV as input for the move command
    --index-tbspace - The tablespace in block where the indexes are stored
    --dsn -  The DSN name if it is already configured
    --log-directory-path -  Pass the log directory base path to store the log files
    --copy-opts - To pass the copy options required for the tool 
    --runstats - To trigger external runstats after the table is moved
    \n
    Command:
    \n
    db2whmigratetocos move  --scope tablespace --list  DB_TS1\n 
    --dest-tbspace OBJSTORESPACE1 --index-tbspace USERSPACE1 \n
    --log-directory-path <path> --user-id  <user_id> --password <password>\n 
    --hostname <>hostnamE> \n
     
    """
    try:
        valid_dsn = " "

        if dsn is not None:
         valid_dsn = dsn
         conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,dsn)
        else:
         valid_dsn = None
         conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
        log_directory_base_path = " "
        if log_directory_path is not None:
            log_directory_base_path = log_directory_path+"/db2whmigratetocos-logs"
            is_exist = os.path.exists(log_directory_base_path)
            if is_exist is not True:
                os.makedirs(log_directory_base_path, exist_ok=True)
        else:
            print("Please provide the logs directory to be used to create logs\n")  
            sys.exit(0)          
        print()
        console.print("Test Connect to the Db2 warehouse instance")
        object_space_list = get_list_of_objectspaces(user_id,password,hostname,port,database,valid_dsn)
        if conn_test:
            valid_copy_opts = True
            if copy_opts is not None:
                user_copy_options = copy_opts.split(",")
                for user_copy_option in user_copy_options:
                    if user_copy_option not in COPY_OPTIONS:
                        valid_copy_opts = False
                        print("The provided copy option is not valid - " + user_copy_option)
                        sys.exit(0)
            if valid_copy_opts is True:
                print("The copy options are  valid - " + copy_opts)
            if list is None and csv_input is None and scope != "table":
                print("The list provided is empty. Kindly give all or a list of tablespaces/schemas")
                sys.exit(0)
            if list is not None:
             src_db2_obj_list = list.split(",")
            if skip_tbspace is not None:
               skip_tbspace_list = skip_tbspace.split(",")
            else:
                skip_tbspace_list = []
            if skip_schema is not None:
               skip_schema_list = skip_schema.split(",")
            else:
                skip_schema_list = []
            if dest_tbspace is not None:
               dest_tbspace_list = dest_tbspace.split(",")
            else:
                dest_tbspace_list = []
            if csv_input is not  None:
                if '.csv' in csv_input:
                    print("Validating the CSV file path")
                    if os.path.isfile(csv_input):
                        print(csv_input + " file is present in the path")
                    else:
                        print("The given file do not exist in the provided path")
                        sys.exit(0)
                else:
                    print("The given input is not a CSV file.. Aborting..")
                    sys.exit(0)
            if scope == "tablespace":
                valid_tbspace_list = get_tablespaces_in_block_and_cos(
                    user_id, password, hostname, port, database,valid_dsn)
                if csv_input is not None:
                    tables_in_df = validate_and_get_df_from_the_csv(
                        csv_input)
                    if len(tables_in_df) > 1:
                        log_directory_name = create_a_log_directory_for_a_batch(log_directory_base_path)
                        for idx, row in enumerate(tables_in_df):
                            if row['tablespace'] in valid_tbspace_list:
                                if row['tablespace'] not in skip_tbspace_list:
                                    if row['tablespace'] not in dest_tbspace_list:
                                        tables_in_tablespace = get_tabname_schemaname_under_tablespace_in_db2woc(
                                            user_id, password, hostname, port, database, row['tablespace'],valid_dsn)
                                        table_exists = False
                                        for item in tables_in_tablespace:
                                            if row['tablename'] == item[0]:
                                                table_exists = True
                                        if table_exists:
                                            selected_dest_tbspace = idx % len(
                                                dest_tbspace_list)
                                            if index_tbspace is None:
                                                index = check_for_user_created_indexes(user_id,password,hostname,port,database,row['tablename'],row['schema'],valid_dsn)
                                                if index is True:
                                                    index_tbspace_found = "USERSPACE1"
                                                else:
                                                    index_tbspace_found = dest_tbspace_list[selected_dest_tbspace]
                                            else:
                                                index_tbspace_found = index_tbspace
                                            move_the_tables(row['schema'], row['tablename'], row['tablespace'], dest_tbspace_list[selected_dest_tbspace],
                                                            log_directory_name, user_id, password, hostname, port, database,valid_dsn,index_tbspace_found,copy_opts,runstats)
                                        else:
                                            print(
                                                "Table not found in the tablespace")
                                    else:
                                        print(
                                            "The source and the destination tablespace are same")
                                else:
                                    print(
                                        "Skipping the tablespace as per the input")
                            else:
                                print("The tablespace name is invalid")
                    else:
                        print("The CSV is empty")
                else:
                    if list is not None:
                        all_tablespaces = 'all' if 'all' in src_db2_obj_list else None
                        if all_tablespaces == 'all':
                            tbspace_list = valid_tbspace_list
                        else:
                            tbspace_list = validate_the_input_db2_objects(
                                src_db2_obj_list, valid_tbspace_list, "tablespaces")
                        log_directory_name = create_a_log_directory_for_a_batch(log_directory_base_path)
                        for tbspace in tbspace_list:
                            tables_in_userspace = []
                            if tbspace not in skip_tbspace_list:
                                if tbspace not in dest_tbspace_list:
                                    tables_in_userspace = get_tabname_schemaname_under_tablespace_in_db2woc(
                                        user_id, password, hostname, port, database, tbspace,valid_dsn)
                                    print(
                                        "Initiating the migration for each of the table, proceeding with next steps....")
                                    tables_cnt = len(tables_in_userspace)
                                    if len(tables_in_userspace) != 0:
                                        for idx, items in enumerate(tables_in_userspace):
                                            selected_dest_tbspace = idx % len(
                                                dest_tbspace_list)
                                            if index_tbspace is  None:   
                                                index = check_for_user_created_indexes(user_id,password,hostname,port,database,items[0],items[1],valid_dsn)
                                                if index is True:
                                                    index_tbspace_found = "USERSPACE1"
                                                else:
                                                    index_tbspace_found = dest_tbspace_list[selected_dest_tbspace]
                                            else:
                                                index_tbspace_found = index_tbspace
                                            move_the_tables(items[1], items[0], tbspace, dest_tbspace_list[selected_dest_tbspace],
                                                            log_directory_name, user_id, password, hostname, port, database,valid_dsn,index_tbspace_found,copy_opts,runstats)
                                    if len(tables_in_userspace) == 0:
                                        print("No tables found in the tablespace")
                                else:
                                    print("The source and the destination are same")
                            else:
                                print("Skipping the tablespace - " + tbspace)
                    else:
                        print("The provided list is empty. Try giving all/ List of tablespaces/schemas in the list")
            if scope == "schema":
                valid_schema_list = get_schema_in_instance(
                    user_id, password, hostname, port, database,valid_dsn)
                if valid_schema_list is not None:
                    if csv_input != None:
                        tables_in_df = validate_and_get_df_from_the_csv(
                            csv_input)
                        if len(tables_in_df) > 1:
                            log_directory_name = create_a_log_directory_for_a_batch(log_directory_base_path)
                            for idx, row in enumerate(tables_in_df):
                                if row['schema'] in valid_schema_list:
                                    if row['schema'] not in skip_schema_list:
                                        source_tablespace = get_tbpsace_name_for_table(
                                            user_id, password, hostname, port, database, row['tablename'], row['schema'],valid_dsn)
                                        tables_cnt,size,tables_in_schema = get_tables_under_schema_in_db2woc(
                                            user_id, password, hostname, port, database, row['schema'])
                                        table_exists = False
                                        for item in tables_in_schema:
                                            if row['tablename'] == item[0]:
                                                table_exists = True
                                        if table_exists:
                                            selected_dest_tbspace = idx % len(dest_tbspace_list)
                                            if source_tablespace not in dest_tbspace_list:
                                                if index_tbspace is  None:  
                                                    index = check_for_user_created_indexes(user_id,password,hostname,port,database,row['tablename'],row['schema'],valid_dsn)
                                                    if index is True:
                                                        index_tbspace_found = "USERSPACE1"
                                                    else:
                                                        index_tbspace_found = dest_tbspace_list[selected_dest_tbspace]
                                                else:
                                                    index_tbspace_found = index_tbspace
                                                move_the_tables(row['schema'], row['tablename'], source_tablespace, dest_tbspace_list[selected_dest_tbspace],
                                                                log_directory_name, user_id, password, hostname, port, database,valid_dsn,index_tbspace_found,copy_opts,runstats)
                                            else:
                                                print(
                                                    "The source and the destination are same")
                                        else:
                                            print(
                                                "Skipping as table do not exist")
                                    else:
                                        print(
                                            "Skipping the schema as per the input")
                                else:
                                    print(
                                        "The specified schema is not valid")
                        else:
                            print("The CSV is empty")
                    else:
                        if list is not None:
                            all_schemas = 'all' if 'all' in src_db2_obj_list else None
                             # validation of schema and setting the list for movment
                            if all_schemas == 'all':
                                schema_list = valid_schema_list
                            else:
                                schema_list = validate_the_input_db2_objects(
                                    src_db2_obj_list, valid_schema_list, "schemas")
                            for schema in schema_list:
                                tables_in_schema = []
                                if schema not in skip_schema_list:
                                    tables_cnt, tota_size, tables_in_schema = get_tables_under_schema_in_db2woc(
                                        user_id, password, hostname, port, database, schema,valid_dsn)
                                    print(
                                        "Initiating the migration for each of the table, proceeding with next steps....")
                                    if len(tables_in_schema) != 0:
                                        log_directory_name = create_a_log_directory_for_a_batch(log_directory_base_path)
                                        for idx, item in enumerate(tables_in_schema):
                                            selected_dest_tbspace = idx % len(dest_tbspace_list)
                                            source_tablespace = get_tbpsace_name_for_table(
                                                user_id, password, hostname, port, database, item[0], schema,valid_dsn)
                                            if index_tbspace is  None:
                                                index = check_for_user_created_indexes(user_id,password,hostname,port,database,item[0],schema,valid_dsn)
                                                if index is True:
                                                    index_tbspace_found = "USERSPACE1" 
                                                else:
                                                    index_tbspace_found = dest_tbspace_list[selected_dest_tbspace]
                                            else:
                                                index_tbspace_found
                                            if source_tablespace not in dest_tbspace_list:
                                                move_the_tables(
                                                    schema, item[0], source_tablespace, dest_tbspace_list[selected_dest_tbspace], log_directory_name, user_id, password, hostname, port, database,valid_dsn,index_tbspace_found,copy_opts,runstats)
                                    if len(tables_in_schema) == 0:
                                        print("No tables found in the schema")
                                else:
                                    print("Skipping the schema - " + schema)
                        else:
                            print("The provided list is empty. Try giving all/ List of tablespaces/schemas in the list")
                else:
                    print("Kindly check the schema list that is provided as input")
            if scope == "table":
                if schema_name is not None:
                    if table_name is not None:
                        log_directory_name = create_a_log_directory_for_a_batch(log_directory_base_path)
                        source_tablespace = get_tbpsace_name_for_table(user_id, password, hostname, port, database,table_name, schema_name,valid_dsn)
                        if index_tbspace is None:
                            index = check_for_user_created_indexes(user_id,password,hostname,port,database,table_name,schema_name,valid_dsn)
                            if index is True:
                                index_tbspace_found = "USERSPACE1"
                            else:
                                index_tbspace_found = dest_tbspace_list[0]
                        else:
                           index_tbspace_found = index_tbspace
                        if source_tablespace not in dest_tbspace_list:
                             move_the_tables(schema_name,table_name, source_tablespace, dest_tbspace_list[0], log_directory_name, user_id, password, hostname, port, database,valid_dsn,index_tbspace_found,copy_opts,runstats)
                    else:
                        print("The table name is not provided") 
                else:
                        print("The schema name is not provided") 
        else:
            print("Kindly check if the Db2 warehouse Instance is up and runnning")
    except Exception as e:
        print(e)
        print(traceback.format_exc())


@ app.command()
def status(
        scope: Annotated[str, typer.Option(help="tables - lists the no of tables in block & COS;migration-runs - migration runs that ran till now")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        log_directory_path: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        active_runs: Annotated[bool, typer.Option(help="active - lists the active migration runs;completed - lists the completed migration runs")] = False,
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001"):
    '''
    Status and the metrics of the migration jobs

    The command is used to fetch the details about the tables in block and cos
    It can give the details and the status of a migration runs

    command:
     db2whmigratetocos status\n
     --scope migration-runs/tables\n
     --active-runs\n
     --log-directory-path <path>
     --user-id <user-id> --password <password> --hostname <host-name>

    '''
    valid_dsn = " "
    if dsn is not None:
        valid_dsn = dsn
        conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,dsn)
    else:
         valid_dsn = None
         conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
    if conn_test:
        tables_in_block = []
        tables_in_cos = []
        total_tables_in_block = 0
        if log_directory_path is not  None:
          path = log_directory_path+"/db2whmigratetocos-logs/"
        else:
                 print("Please provide the log path to know the status of the migration runs")
                 sys.exit(0)
        if scope == "tables":
            tablespaces_in_instance = get_tablespaces_in_block_and_cos(
                user_id, password, hostname, port, database,dsn)
            if len(tablespaces_in_instance) != 0:
                for tablespace in tablespaces_in_instance:
                    table_in_tbspace = get_tables_cnt_under_tablespaces(
                        user_id, password, hostname, port, database, tablespace,dsn)
                    if "OBJ" not in tablespace:
                        total_tables_in_block = total_tables_in_block + table_in_tbspace
                        tables_in_block.append([tablespace, table_in_tbspace])
                    else:
                        tables_in_cos.append([tablespace, table_in_tbspace])
                console.rule("[bold red]Tablespaces in Block")
                print_tables_in_block = print_table_row(tables_in_block)
                console.print(print_tables_in_block)
                console.rule("[bold red]Tablespaces in COS")
                print_in_tables_in_cos = print_table_row(tables_in_cos)
                console.print(print_in_tables_in_cos)
        if scope == "migration-runs":
            console.rule("[bold red]Migration Runs")
            print(
                "To check the complete logs and metrics,please find the log file in the respective location:")
            print(path+"<batch-id>/<job-id>-<table-name>.json")
            print(path+"<batch-id>/<job-id>-<table-name>.log")
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
                                active_migration_job_details, user_id, password, hostname, port, database, STATUS_TABLE_HEADER_ACTIVE_RUNS, active_runs,dsn)
                            console.print(tb_table_migration_runs)
                        else:
                            print("No active migration runs yet in the instance")
                    else:
                        if len(completed_migration_job_details) != 0:
                            tb_table_migration_runs = parse_the_json_files_for_status(
                                completed_migration_job_details, user_id, password, hostname, port, database, STATUS_TABLE_HEADER, active_runs,dsn)
                            console.print(tb_table_migration_runs)
                        else:
                            print("No migration runs yet in the instance")
                else:
                    print("No migration runs yet in the instance")
            else:
                print("The db2whmigratetocos-logs folder is not present in the given path")
                
   
@ app.command()
def cancel(
        schema_name: Annotated[str, typer.Option(help="Schema Name of the table")],
        table_name: Annotated[str, typer.Option(help="Table Name to cancel the run")],
        src_tablespace: Annotated[str, typer.Option(help="Source Tablespace Name to cancel the run")],
        dest_tablespace: Annotated[str, typer.Option(help="Destination tablespace Name to cancel the run")],
        index_tbspace: Annotated[str, typer.Option(help="Index tablespace tablespace Name to cancel the run")],
        use_adc: Annotated[bool, typer.Option(help=" Uses Sampling method to  create dictionary by default - give --use-adc to use ADC for dictionary creation")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        log_file_name: Annotated[str, typer.Option(
            help="The log file name to remove the log file ")] = None,
        report_file_name: Annotated[str, typer.Option(
            help="The report file name to remove the JSON file")] = None,
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001"):

    """
    To cancel a run for the table migration run 
    """
    try:
        valid_dsn = ""
        if dsn != None:
         valid_dsn = dsn
         conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
        else:
         valid_dsn = None
         conn_test = db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True,valid_dsn)
        print()
        console.print("Test Connect to the Db2 warehouse instance")
        if conn_test:
            if  os.path.exists(log_file_name):
                print("Removing the LOG File")
                os.remove(log_file_name)
            if  os.path.exists(report_file_name):
                print("Removing the JSON File")
                os.remove(report_file_name)
            if use_adc is False:
                copy_opts = "COPY_USE_OTA,NO_STATS"
            else:
                copy_opts = "COPY_USE_OTA,USE_ADC,NO_STATS"
            print("Cancelling the table migration")
            cancel_terminate_admin_move_table(user_id, password, hostname, port, database, schema_name, table_name, "TERM", src_tablespace, dest_tablespace,dsn,index_tbspace,copy_opts)
            cancel_terminate_admin_move_table(user_id, password, hostname, port, database, schema_name, table_name, "CANCEL", src_tablespace, dest_tablespace,dsn,index_tbspace,copy_opts)
    except Exception as e:
        print(e)
