"""

    Copyright IBM Corp. 2024-2025 All Rights Reserved.
    Licensed Materials - Property of IBM

"""
import traceback
import csv
from datetime import datetime
import json
import os
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
import pandas as pd
import typer
from db2whmigratetocos.db2wh_db2_utilities import check_home_path, check_if_logs_path_exist_else_create, create_file_for_the_table_migration, create_log_directory_for_migration_run, db2wh_pyodbc_connection, generate_uuid, get_json_format_for_migration_run, get_schema_in_instance, get_tables_cnt_under_tablespaces, get_tables_under_schema_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc, get_tbpsace_name_for_table, find_adm_status_by_tablename
from .db2whmigratetocos_install_prereq import db2whmigratetocos_init
from .admin_move_table_func import adm_move_table_ops_db2woc


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
        scope: Annotated[str, typer.Option(help="List the tables by tablespace/schema")],
        list: Annotated[str, typer.Option(help="all (or) list of tablespaces/schemas")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
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
            invalid_tbspace_list = []
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
                    if schema_list is None:
                        print("Check if the Db2 warehouse instance is up and running")
                else:
                    if detail is not True:
                        console.print(
                            "Kindly provide --detail to  get the tables for the list of tablespaces")
                    for tbspace in input_obj_list:
                        if tbspace not in valid_tablespace_list:
                            invalid_tbspace_list.append(tbspace)
                    if len(invalid_tbspace_list) > 0:
                        print("skipping invalid tablespaces")
                        print(invalid_tbspace_list)
                        for tbspace in valid_tablespace_list:
                            if tbspace in invalid_tbspace_list:
                                input_obj_list.remove(tbspace)
                        tablespace_list = input_obj_list
                    else:
                        tablespace_list = input_obj_list
                    if tablespace_list is None:
                        print(
                            "Kindly check the tablespace list that is provided as input")
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
                                if "OBJSTORE" in tbspace:
                                    tbspace_store = "COS"
                                else:
                                    tbspace_store = "Block"
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
                                        tb_table.add_row(
                                            table[0], table[1], str(table[2]))
                                        tables_list_in_tablespaces.append(
                                            [tbspace, table[0], table[1], str(table[2]), str(tbspace_store)])
                                        if count >= 75:
                                            break
                                    print()
                                    console.print(tb_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the tablespace")
                            if export_csv is True:
                                console.print("Exporting the data into CSV")
                                df = pd.DataFrame(tables_list_in_tablespaces, columns=[
                                                  "Tablespace", "Tablename", "Schema", "Size", "Storage"])
                                filename = "db2whmigratetocos-"+tbspace + \
                                    "-tables-list-"+datetime.now().isoformat()+".csv"
                                df.to_csv(filename, index=False)
                                print()
                                console.print(
                                    "Exporting the list of tables in the tablespace")
                                print(f"Data saved to CSV file: {filename}")
                        else:
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
                                    tbs_block, columns=["Tablespace"])
                                df_cos = pd.DataFrame(
                                    tbs_cos, columns=["Tablespace"])
                                blk_filename = "tbspaces-in-block-"+datetime.now().isoformat()+".csv"
                                cos_filename = "tbspaces-in-cos-"+datetime.now().isoformat()+".csv"
                                df_blk.to_csv(blk_filename, index=False)
                                df_cos.to_csv(cos_filename, index=False)
                                console.print(
                                    "The tablespaces in block can be found in " + blk_filename)
                                console.print(
                                    "The tablespaces in cos can be found in " + cos_filename)
                    else:
                        print()
                        print("No Tablespaces found")
            if scope == "schema":
                invalid_schema_list = []
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
                    if schema_list is None:
                        print("Check if the Db2 warehouse instance is up and running")
                else:
                    # validating the schema list
                    if detail is True:
                        console.print(
                            "Kindly provide --detail to  get the tables for the list of tablespaces")
                    for schema in input_obj_list:
                        if schema not in valid_schema_list:
                            invalid_schema_list.append(schema)
                    if len(invalid_schema_list) > 0:
                        print("skipping invalid schemas")
                        print(invalid_schema_list)
                        for schema in valid_schema_list:
                            if schema in invalid_schema_list:
                                input_obj_list.remove(schema)
                        schema_list = input_obj_list
                    else:
                        schema_list = input_obj_list
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
                                        sc_table.add_row(
                                            table[0], str(table[1]))
                                        tables_in_schema.append(
                                            [schema, table[0], table[1]])
                                        if count >= 75:
                                            break
                                    console.print(sc_table)
                                else:
                                    print()
                                    console.print(
                                        "No tables found in the schema")
                                    print()
                            if export_csv is True:
                                console.print(
                                    "Exporting the schema data into CSV")
                                df = pd.DataFrame(tables_in_schema, columns=[
                                                  "Schema", "Tablename", "Size"])
                                filename = "db2whmigratetocos-schemas-tables-list-" + \
                                    datetime.now().isoformat()+".csv"
                                df.to_csv(filename, index=False)
                                console.print(
                                    "Exporting the list of tables in the schema")
                                print(f"Data saved to CSV file: {filename}")
                        else:
                            schema_table = Table(show_footer=False)
                            for row in schema_list:
                                schema_table.add_row(str(row))
                            console.print(schema_table)
                            if export_csv is True:
                                console.print(
                                    "Exporting the schema list into CSV")
                                df = pd.DataFrame(
                                    schema_list, columns=["Schema"])
                                filename = "db2whmigratetocos-schemas-list-"+datetime.now().isoformat()+".csv"
                                df.to_csv(filename, index=False)
                                console.print(
                                    "Exporting the list of tables in the schema")
                                print(f"Data saved to CSV file: {filename}")
        else:
            print(
                "Cannot connect to the Instance. Kindly check if the status if up and running")
    except Exception as e:
        print(e)


@app.command()
def move(
        scope: Annotated[str, typer.Option(help="Move tables by tablespace/schema")],
        list: Annotated[str, typer.Option(help="Source tablespace/schema in block storage - all/comma seperated list of tablespace/schema")],
        dest_tbspace: Annotated[str, typer.Option(help="Destination tablespace in cos, where the data needs to be moved ")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
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
        db2wh_pyodbc_connection(
            user_id, password, hostname, port, database, True)
        print()
        src_db2_obj_list = list.split(",")
        skip_tbspace_list = skip_tbspace.split(",")
        skip_schema_list = skip_schema.split(",")
        input_list_csv_check = ['csv' for i in src_db2_obj_list if 'csv' in i]
        if input_list_csv_check:
            input_list_csv = input_list_csv_check[0]
        else:
            input_list_csv = None
        csv_columns = ['Tablespace', 'Tablename', 'Schema', 'Size', 'Storage']
        if scope == "tablespace":
            invalid_csv_column = []
            invalid_tbspace_list = []
            valid_tbspace_list = get_tablespaces_in_block_and_cos(
                user_id, password, hostname, port, database)
            all_tablespaces = 'all' if 'all' in src_db2_obj_list else None
            if input_list_csv == 'csv':
                c = datetime.now()
                current_time = c.strftime('%d%m%Y-%H%M%S')
                directory_name = "batch-"+str(current_time)
                log_directory_name = create_log_directory_for_migration_run(
                    directory_name)
                for item in src_db2_obj_list:
                    if '.csv' in item:
                        csv_file_exists = os.path.isfile(item)
                        if csv_file_exists:
                            # TODO Check the csv columns
                            # tables_column = pd.read_csv(item)
                            # for column in tables_column.columns():
                            #     if column not in csv_columns:
                            #         invalid_csv_column.append(column)
                            if len(invalid_csv_column) == 0:
                                with open(item, encoding='utf-8') as f:
                                    table_csv_reader = csv.DictReader(f)
                                    tables_in_df = [
                                        row for row in table_csv_reader]
                                    if len(tables_in_df) != 0:
                                        tables_in_tablespace = []
                                        for row in tables_in_df:
                                            if row['Tablespace'] in valid_tbspace_list:
                                                if row['Tablespace'] not in skip_tbspace_list:
                                                    if row['Tablespace'] != dest_tbspace:
                                                        tables_in_tablespace = get_tabname_schemaname_under_tablespace_in_db2woc(
                                                            user_id, password, hostname, port, database, row['Tablespace'])
                                                        table_exists = False
                                                        for item in tables_in_tablespace:
                                                            if row['Tablename'] == item[0]:
                                                                table_exists = True
                                                        if table_exists:
                                                            migration_job_id = generate_uuid()
                                                            migration_table_details = get_json_format_for_migration_run(
                                                                row['Schema'], row['Tablename'], "INIT", row['Tablespace'], dest_tbspace, str(migration_job_id))
                                                            report_file_name_for_the_table = migration_job_id + \
                                                                "-" + \
                                                                row['Tablename'] + \
                                                                ".json"
                                                            std_output_name_for_the_file = migration_job_id + \
                                                                "-" + \
                                                                row['Tablename'] + \
                                                                ".log"
                                                            std_log_creation_done = create_file_for_the_table_migration(
                                                                log_directory_name, std_output_name_for_the_file)
                                                            file_creation_done = create_file_for_the_table_migration(
                                                                log_directory_name, report_file_name_for_the_table)
                                                            if file_creation_done:
                                                                with open(log_directory_name+"/"+report_file_name_for_the_table, 'w', encoding='utf-8') as f:
                                                                    json.dump(
                                                                        migration_table_details, f, indent=6)
                                                            if std_log_creation_done:
                                                                print(
                                                                    "Table Name" + row['Tablename'])
                                                                print(
                                                                    "Migration ID " + migration_job_id)
                                                                print(
                                                                    "Reports in " + log_directory_name+"/"+report_file_name_for_the_table)
                                                                print(
                                                                    "Logs in " + log_directory_name+"/"+std_output_name_for_the_file)
                                                                adm_move_table_ops_db2woc(user_id, password, hostname, port, database, row['Schema'], row['Tablename'], "INIT", row[
                                                                                          'Tablespace'], dest_tbspace, log_directory_name+"/"+report_file_name_for_the_table, log_directory_name+"/"+std_output_name_for_the_file)
                                                                # adm_process = Process(target=adm_move_table_ops_db2woc, args=(user_id,password,hostname,port,database,row['Schema'],row['Tablename'],"INIT",row['Tablespace'],dest_tbspace,log_directory_name+"/"+report_file_name_for_the_table,log_directory_name+"/"+std_output_name_for_the_file))
                                                                # processes.append(adm_process)
                                                        else:
                                                            print(
                                                                "Table not found in the tablespace")
                                                    else:
                                                        print(
                                                            "Tha source and the destination tablespace are not same")
                                                else:
                                                    print(
                                                        "skipping the tablespace as per the input")
                                            else:
                                                print(
                                                    "the tablespace name is invalid")

                            else:
                                print("Identified invalid column names in the CSV")
                                print(invalid_csv_column)

                        else:
                            print(
                                "Kindly check the if the file path provided is correct")
            else:
                if all_tablespaces == 'all':
                    tbspace_list = valid_tbspace_list
                else:
                    for tbspace in src_db2_obj_list:
                        if tbspace not in valid_tbspace_list:
                            invalid_tbspace_list.append(tbspace)
                    if len(invalid_tbspace_list) > 0:
                        print("skipping invalid tablespaces")
                        print(invalid_tbspace_list)
                        for tbspace in valid_tbspace_list:
                            if tbspace in invalid_tbspace_list:
                                src_db2_obj_list.remove(tbspace)
                        tbspace_list = src_db2_obj_list
                    else:
                        tbspace_list = src_db2_obj_list
                c = datetime.now()
                current_time = c.strftime('%d%m%Y-%H%M%S')
                directory_name = "batch-"+str(current_time)
                log_directory_name = create_log_directory_for_migration_run(
                    directory_name)
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
                                for items in tables_in_userspace:
                                    print()
                                    migration_job_id = generate_uuid()
                                    migration_table_details = get_json_format_for_migration_run(
                                        items[1], items[0], "INIT", tbspace, dest_tbspace, str(migration_job_id))
                                    report_file_name_for_the_table = migration_job_id + \
                                        "-"+items[0]+".json"
                                    std_output_name_for_the_file = migration_job_id + \
                                        "-"+items[0]+".log"
                                    file_creation_done = create_file_for_the_table_migration(
                                        log_directory_name, report_file_name_for_the_table)
                                    std_log_creation_done = create_file_for_the_table_migration(
                                        log_directory_name, std_output_name_for_the_file)
                                    if file_creation_done:
                                        with open(log_directory_name+"/"+report_file_name_for_the_table, 'w', encoding='utf-8') as f:
                                            json.dump(
                                                migration_table_details, f, indent=6)
                                    if std_log_creation_done:
                                        # adm_process = Process(target=adm_move_table_ops_db2woc, args=(user_id,password,hostname,port,database,items[1],items[0],"INIT",tbspace,dest_tbspace,log_directory_name+"/"+report_file_name_for_the_table,log_directory_name+"/"+std_output_name_for_the_file))
                                        print("Table Name " + items[0])
                                        print("Migration ID " +
                                              migration_job_id)
                                        print("Reports in " + log_directory_name +
                                              "/"+report_file_name_for_the_table)
                                        print("Logs in " + log_directory_name +
                                              "/"+std_output_name_for_the_file)
                                        adm_move_table_ops_db2woc(user_id, password, hostname, port, database, items[1], items[
                                                                  0], "INIT", tbspace, dest_tbspace, log_directory_name+"/"+report_file_name_for_the_table, log_directory_name+"/"+std_output_name_for_the_file)
                            if len(tables_in_userspace) == 0:
                                print("no tables found in the tablespace")
                        else:
                            print("The source and the destination are same")
                    else:
                        print("Skipping the tablespace - " + tbspace)
        if scope == "schema":
            invalid_schema_list = []
            invalid_csv_column = []
            valid_schema_list = get_schema_in_instance(
                user_id, password, hostname, port, database)
            all_schemas = 'all' if 'all' in src_db2_obj_list else None
            if valid_schema_list is not None:
                if input_list_csv == 'csv':
                    c = datetime.now()
                    current_time = c.strftime('%d%m%Y-%H%M%S%f')
                    directory_name = "batch"+str(current_time)
                    log_directory_name = create_log_directory_for_migration_run(
                        directory_name)
                    for item in src_db2_obj_list:
                        if '.csv' in item:
                            csv_file_exists = os.path.isfile(item)
                            if csv_file_exists:
                                # schema_column = pd.read_csv(item)
                                # for column in list(schema_column):
                                #     if column not in csv_columns:
                                #         invalid_csv_column.append(column)
                                if len(invalid_csv_column) == 0:
                                    with open(item, encoding='utf-8') as f:
                                        table_csv_reader = csv.DictReader(f)
                                        tables_in_df = [
                                            row for row in table_csv_reader]
                                        if len(tables_in_df) != 0:
                                            for row in tables_in_df:
                                                if row['Schema'] in valid_schema_list:
                                                    if row['Schema'] not in skip_schema_list:
                                                        if row["Tablespace"] != dest_tbspace:
                                                            migration_job_id = generate_uuid()
                                                            migration_table_details = get_json_format_for_migration_run(
                                                                row['Schema'], row['Tablename'], "INIT", row['Tablespace'], dest_tbspace, str(migration_job_id))
                                                            report_file_name_for_the_table = migration_job_id + \
                                                                "-" + \
                                                                row['Tablename'] + \
                                                                ".json"
                                                            std_output_name_for_the_file = migration_job_id + \
                                                                "-" + \
                                                                row['Tablename'] + \
                                                                ".log"
                                                            file_creation_done = create_file_for_the_table_migration(
                                                                log_directory_name, report_file_name_for_the_table)
                                                            std_log_creation_done = create_file_for_the_table_migration(
                                                                log_directory_name, std_output_name_for_the_file)
                                                            if file_creation_done:
                                                                with open(log_directory_name+"/"+report_file_name_for_the_table, 'w', encoding='utf-8') as f:
                                                                    json.dump(
                                                                        migration_table_details, f, indent=6)
                                                            if std_log_creation_done:
                                                                print(
                                                                    "Table Name" + row['Tablename'])
                                                                print(
                                                                    "Migration ID " + migration_job_id)
                                                                print(
                                                                    "Reports in " + log_directory_name+"/"+report_file_name_for_the_table)
                                                                print(
                                                                    "Logs in " + log_directory_name+"/"+std_output_name_for_the_file)
                                                                adm_move_table_ops_db2woc(user_id, password, hostname, port, database, row['Schema'], row['Tablename'], "INIT", row[
                                                                                          'Tablespace'], dest_tbspace, log_directory_name+"/"+report_file_name_for_the_table, log_directory_name+"/"+std_output_name_for_the_file)
                                                    else:
                                                        print(
                                                            "skipping the schema as per the input")
                                                else:
                                                    print(
                                                        "The specified schema is not valid")
                                else:
                                    print(
                                        "Kindly check the column names in the csv provided")
                                    print("Required Format")
                                    print(csv_columns)
                                    print("Provided format")
                                    print(list())
                            else:
                                print("kindly check if the file exists in path")
                else:
                    # validation of schema and setting the list for movment
                    print(valid_schema_list)
                    if all_schemas == 'all':
                        schema_list = valid_schema_list
                    else:
                        for schema in src_db2_obj_list:
                            if schema not in valid_schema_list:
                                invalid_schema_list.append(schema)
                        if len(invalid_schema_list) > 0:
                            print("skipping invalid schemas")
                            print(invalid_schema_list)
                            for schema in valid_schema_list:
                                if schema in invalid_schema_list:
                                    src_db2_obj_list.remove(schema)
                            schema_list = src_db2_obj_list
                        else:
                            schema_list = src_db2_obj_list
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
                                c = datetime.now()
                                current_time = c.strftime('%d%m%Y-%H%M%S')
                                directory_name = "batch"+str(current_time)
                                log_directory_name = create_log_directory_for_migration_run(
                                    directory_name)
                                for item in tables_in_schema:
                                    source_tablespace = get_tbpsace_name_for_table(
                                        user_id, password, hostname, port, database, item[0])
                                    if source_tablespace not in dest_tbspace:
                                        migration_job_id = generate_uuid()
                                        migration_table_details = get_json_format_for_migration_run(
                                            schema, item[0], "INIT", source_tablespace, dest_tbspace, str(migration_job_id))
                                        report_file_name_for_the_table = migration_job_id + \
                                            "-"+item[0]+".json"
                                        std_output_name_for_the_file = migration_job_id + \
                                            "-"+item[0]+".log"
                                        file_creation_done = create_file_for_the_table_migration(
                                            log_directory_name, report_file_name_for_the_table)
                                        std_log_creation_done = create_file_for_the_table_migration(
                                            log_directory_name, std_output_name_for_the_file)
                                        if file_creation_done:
                                            with open(log_directory_name+"/"+report_file_name_for_the_table, 'w', encoding='utf-8') as f:
                                                json.dump(
                                                    migration_table_details, f, indent=6)
                                        if std_log_creation_done:
                                            # adm_process = Process(target=adm_move_table_ops_db2woc, args=(user_id,password,hostname,port,database,schema,item[0],"INIT",source_tablespace,dest_tbspace,log_directory_name+"/"+report_file_name_for_the_table,log_directory_name+"/"+std_output_name_for_the_file))
                                            print("Table Name " + item[0])
                                            print("Migration ID " +
                                                  migration_job_id)
                                            print(
                                                "Reports in " + log_directory_name+"/"+report_file_name_for_the_table)
                                            print(
                                                "Logs in " + log_directory_name+"/"+std_output_name_for_the_file)
                                            adm_move_table_ops_db2woc(user_id, password, hostname, port, database, schema, item[
                                                                      0], "INIT", source_tablespace, dest_tbspace, log_directory_name+"/"+report_file_name_for_the_table, log_directory_name+"/"+std_output_name_for_the_file)
                                            # processes.append(adm_process)
                            if len(tables_in_schema) == 0:
                                print("no tables found in the schema")
                        else:
                            print("Skipping the schema - " + schema)
            else:
                print("Kindly check the schema list that is provided as input")
    except Exception as e:
        print(e)
        print(traceback.format_exc())


@app.command()
def status(
        scope: Annotated[str, typer.Option(help="tables - lists the no of tables in block & COS;migration-runs - migration runs that ran till now")],
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
        tb_table_migration_runs = Table()
        tb_table_migration_runs.add_column(
            "Batch Id", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Job Id", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Table Name", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Schema Name", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Status", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Source Tbspace", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Dest Tbspace", justify="center", style="cyan")
        tb_table_migration_runs.add_column(
            "Time Taken (in secs)", justify="center", style="cyan")
        tablespaces_in_instance = get_tablespaces_in_block_and_cos(
            user_id, password, hostname, port, database)
        tables_in_block = []
        tables_in_cos = []
        total_tables_in_block = 0
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
            console.print(
                f"The tables yet to be moved to COS - {total_tables_in_block}")
            tb_table_block = Table()
            tb_table_block.add_column(
                "Tablespace", justify="center", style="cyan")
            tb_table_block.add_column(
                "Table count", justify="center", style="cyan")
            for tablespace in tables_in_block:
                tb_table_block.add_row(tablespace[0], str(tablespace[1]))
            console.print(tb_table_block)
            console.rule("[bold red]Tablespaces in COS")
            tb_table_cos = Table()
            tb_table_cos.add_column(
                "Tablespace", justify="center", style="cyan")
            tb_table_cos.add_column(
                "Table count", justify="center", style="cyan")
            for tablespace in tables_in_cos:
                tb_table_cos.add_row(tablespace[0], str(tablespace[1]))
            console.print(tb_table_cos)
        if scope == "migration-runs":
            console.rule("[bold red]Migration Runs")

            home = check_home_path()
            path = home.strip()+"/db2whmigratetocos-logs"
            is_exist = os.path.exists(path)
            migration_job_details = []
            print(
                "To check the complete logs and metrics,please find the log file in the respective location:")
            print(path+"/<batch-id>/<job-id>-<table-name>.json")
            print(path+"/<batch-id>/<job-id>-<table-name>.log")
            print()
            if is_exist:
                migration_batches = os.listdir(path)
                if len(migration_batches) > 0:
                    for batch in migration_batches:
                        migration_runs_path = path+"/"+batch
                        migration_runs = os.listdir(migration_runs_path)
                        if len(migration_runs) > 0:
                            for migration_run in migration_runs:
                                if ".json" in migration_run:
                                    jfile = open(
                                        migration_runs_path+"/"+migration_run, "r", encoding='utf-8')
                                    data = json.load(jfile)
                                    data['batch_id'] = batch
                                    migration_job_details.append(data)
                    for details in migration_job_details:
                        init_time = " "
                        end_time = " "
                        init_bool = False
                        end_bool = False
                        phase_status = ""
                        for phase in details['phase_logs']:
                            if phase['STATUS'] == "INIT":
                                init_time = phase['INIT_START']
                                init_bool = True
                            if phase['STATUS'] == "COMPLETE":
                                end_time = phase['CLEANUP_END']
                                end_bool = True
                            if phase['STATUS'] != 'COMPLETE':
                                phase_status = find_adm_status_by_tablename(
                                    user_id, password, hostname, port, database, str(details['table_name']))
                            else:
                                phase_status = details['status']
                        time_taken = "-"
                        init_start = " "
                        cleanup_end = " "
                        if init_bool and end_bool:
                            init_start = datetime.strptime(
                                init_time, "%Y-%m-%d-%H.%M.%S.%f")
                            cleanup_end = datetime.strptime(
                                end_time, "%Y-%m-%d-%H.%M.%S.%f")
                            time_taken = str(
                                int((cleanup_end - init_start).total_seconds()))
                        tb_table_migration_runs.add_row(str(details['batch_id']), str(details['migration_job_id']), str(
                            details['table_name']), details['schema_name'], phase_status, details['source_tablespace'], details['destination_tablespace'], time_taken)
                    console.print(tb_table_migration_runs)
                else:
                    print("There are no migration runs for the instance yet")

            else:
                print("The logs folder is not present")
                print("Creating the log folder")
                check_if_logs_path_exist_else_create()
    except Exception as e:
        print(e)
