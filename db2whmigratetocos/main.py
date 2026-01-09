"""
Copyright IBM Corp. 2024 All Rights Reserved.
Licensed Materials - Property of IBM
"""

from pathlib import Path
import sys
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

import os
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
import pandas as pd
import typer
from db2whmigratetocos.admin_move_table_func import cancel_terminate_admin_move_table
from db2whmigratetocos.constants import COPY_OPTIONS, STATUS_TABLE_HEADER, STATUS_TABLE_HEADER_ACTIVE_RUNS
from db2whmigratetocos.db2wh_db2_utilities import check_for_user_created_indexes, check_home_path, check_if_logs_path_exist_else_create, comma_string_to_list, create_a_log_directory_for_a_batch, create_tablespace, db2wh_pyodbc_connection, export_the_data_as_csv, filter_migration_tables, get_list_of_objectspaces, get_schema_in_instance, get_tables_cnt_under_tablespaces, get_tables_parent_tables, get_tables_under_schem_notabsize_in_db2woc, get_tables_under_schema_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tables_under_tablespace_no_tabsize_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc, get_tbpsace_name_for_table, list_migration_runs, move_table, parse_the_json_files_for_status, render_table, print_export_tables_in_block_and_cos, print_table_row, round_robin_counter, tab_size_by_table_name, validate_and_get_df_from_the_csv, validate_input_objects, validate_tables, validate_the_input_db2_objects
from db2whmigratetocos.fetch_tables_utilities import get_tables_by_schema, get_tables_by_tablespace
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
def fetch(
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
        scope: Annotated[str, typer.Option(
            help="List the tables by tablespace/schema", case_sensitive=False)] = "tablespace",
        objects: Annotated[str, typer.Option(
            help="all (or) list of tablespaces/schemas")] = "all",
        detail: Annotated[bool, typer.Option(
            help="List tables with its schema & size- true/false")] = False,
        export_csv: Annotated[bool, typer.Option(
            help="Export the table data into a CSV")] = False,
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001",
        enable_ssl: Annotated[bool, typer.Option(help="Enable SSL encryption for the database connection.")] = False,
        resolve_ri: Annotated[bool, typer.Option(help="Option to resolve referential integrity")] = False):
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
        console.print("Test Connect to the Db2 warehouse instance")

        connection_details = {
            "user_id": user_id, "password": password, "hostname": hostname, "port": port,
            "database": database, "dsn": dsn, "enable_ssl": enable_ssl
        }

        conn_status = db2wh_pyodbc_connection(connection_details, True)

        if not conn_status:
            console.print(
                "Cannot connect to the Instance. Kindly check if the database is up and running."
            )

            return

        input_obj_list = (
            "ALL"
            if "ALL" in objects.upper()
            else [obj.strip().upper() for obj in objects.split(",")]
        )

        object_tablespaces = get_list_of_objectspaces(connection_details)

        # To render table columns in the specific order
        columns_key_map = [
            ("Tablespace", "tablespace"), ("Storage", "storage"), ("Tablename", "tablename"),
            ("Schema", "schema"), ("Table Size in KB", "size")
        ]

        if scope == "tablespace":
            tables = get_tables_by_tablespace(
                connection_details, input_obj_list, detail, object_tablespaces
            )

        elif scope == "schema":
            tables = get_tables_by_schema(
                connection_details, input_obj_list, detail, object_tablespaces
            )

        else:
            console.print("Invalid scope.")
            return

        if tables:
            if resolve_ri:
                console.print("\nResolving referential integrity.")
                tables = get_tables_parent_tables(
                    tables, object_tablespaces, detail, connection_details
                )

                console.print("Tables after resolving referential integrity (first 75):")
                render_table(columns_key_map, tables)

            if export_csv:
                export_the_data_as_csv(tables)

    except Exception as e:
        print(e)


@app.command()
def move(
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        objects: Annotated[str, typer.Option(
            help="Source tablespace/schema in block storage - all/comma seperated list of tablespace/schema")] = "",
        resolve_ri: Annotated[bool, typer.Option(help="Option to resolve referential integrity")] = False,
        csv_input: Annotated[str, typer.Option(
            help="CSV file as input to the move command as .csv file without the path")] = None,
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name configured in ODBC Driver Config File (odbcinst.ini)")] = None,
        log_directory_path: Annotated[Path, typer.Option(
            help="Pass the log directory base path to store the log files")] = Path("logs"),
        scope: Annotated[str, typer.Option(
            help="Move tables by tablespace/schema")] = "tablespace",
        schema_name: Annotated[str, typer.Option(
            help="Provide the schema name when moving a single table")] = "",
        runstats: Annotated[bool, typer.Option(
            help="Execute RUNSTAT command")] = False,
        table_name: Annotated[str, typer.Option(
            help="Move tables by tablespace/schema")] = "",
        dest_tbspace: Annotated[str, typer.Option(
            help="Destination tablespace in cos, where the data needs to be moved ")] = "OBJSTORESPACE1",
        index_tbspace : Annotated[str, typer.Option(
            help="Destination index tablespace in cos, where the index needs to be moved ")] = "",
        copy_opts: Annotated[str, typer.Option(
            help="Copy options to be passed.")] = "COPY_USE_OTA,NO_STATS",
        user_id: Annotated[str, typer.Option(
            help="User Id to connect to Db2 warehouse Instance")] = "db2inst1",
        skip_schema: Annotated[str, typer.Option(
            help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] = "",
        skip_tbspace: Annotated[str, typer.Option(
            help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")] = "",
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001",
        enable_ssl: Annotated[bool, typer.Option(help="Enable SSL encryption for the database connection.")] = False,
        workers: Annotated[int, typer.Option(help="Number of worker threads to use", min=1)] = 1):
        
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
        if not objects and not csv_input and scope != "table":
            console.print("Insufficient data. Provide one of: --objects, --csv-input, or --table.")
            return

        console.print("Test Connect to the Db2 warehouse instance")

        connection_details = {
            "user_id": user_id, "password": password, "hostname": hostname, "port": port,
            "database": database, "dsn": dsn, "enable_ssl": enable_ssl
        }

        conn_status = db2wh_pyodbc_connection(connection_details, True)

        if not conn_status:
            console.print(
                "Cannot connect to the Instance. Kindly check if the status if up and running."
            )
            return

        if log_directory_path == Path("."):
            log_directory_path = Path("logs")

        console.print(f"Logs will be stored under '{log_directory_path.resolve()}'.")
        log_directory_path.mkdir(parents=True, exist_ok=True)

        if not copy_opts:
            copy_opts = ",".join(COPY_OPTIONS)

        else:
            invalid_copy_opts = [opt for opt in copy_opts.split(",") if opt not in COPY_OPTIONS]

            if invalid_copy_opts:
                console.print(f"Unsupported copy options: {','.join(invalid_copy_opts)}")
                console.print(f"Supported options: {', '.join(COPY_OPTIONS)}")
                return

        skip_tbspace = comma_string_to_list(skip_tbspace)
        skip_schema = comma_string_to_list(skip_schema)
        dest_tbspace = comma_string_to_list(dest_tbspace)
        index_tbspace = comma_string_to_list(index_tbspace)
        object_tablespaces = get_list_of_objectspaces(connection_details)
        migration_tables = []

        if csv_input:
            csv_path = Path(csv_input)

            if csv_path.suffix != ".csv":
                console.print("Input CSV file is not a CSV file.")
                return

            if not csv_path.is_file():
                console.print(f"Input CSV file does not exist: {csv_path}")
                return

            migration_tables = validate_and_get_df_from_the_csv(csv_path)
            if not migration_tables:
                console.print("Input CSV file is empty.")
                return

            migration_tables = validate_tables(connection_details, migration_tables)
            if not migration_tables:
                console.print(
                    f"Validation failed: None of the tables specified in '{csv_path}' exist in "
                    "the database."
                )
                return

        else:
            if scope not in ("tablespace", "schema", "table"):
                console.print(f"Invalid scope: {scope}")
                return

            if scope == "table":
                missing_options = [
                    opt_name
                    for opt_name, opt_val in [
                        ("--table-name", table_name), ("--schema-name", schema_name)
                    ]
                    if not opt_val
                ]

                if missing_options:
                    console.print(f"Missing options: {', '.join(missing_options)}")
                    return

                table_name = table_name.upper()
                schema_name = schema_name.upper()
                tablespace = get_tbpsace_name_for_table(connection_details, table_name, schema_name)

                if not tablespace:
                    console.print(
                        f"Tablespace not found for table '{schema_name}.{table_name}'"
                    )
                    return

                migration_tables.append({
                    "tablename": table_name, "tablespace": tablespace, "schema": schema_name,
                    "storage": "COS" if tablespace in object_tablespaces else "Block-Storage",
                    "size": str(tab_size_by_table_name(connection_details, schema_name, table_name))
                })

            else:
                input_obj_list = (
                    "ALL"
                    if "ALL" in objects.upper()
                    else [obj.strip().upper() for obj in objects.split(",")]
                )

                if scope == "tablespace":
                    migration_tables = get_tables_by_tablespace(
                        connection_details, input_obj_list, True, object_tablespaces
                    )

                else:
                    migration_tables = get_tables_by_schema(
                        connection_details, input_obj_list, True, object_tablespaces
                    )

        # Resolve Referential Integrity
        if resolve_ri:
            console.print("\nResolving referential integrity.")
            migration_tables = get_tables_parent_tables(
                migration_tables, object_tablespaces, True, connection_details
            )

        migration_tables = filter_migration_tables(
            connection_details, migration_tables, skip_tbspace, skip_schema, object_tablespaces
        )

        if not migration_tables:
            console.print("No valid tables found to migrate.")
            return

        # To render table columns in the specific order
        columns_key_map = [
                ("Tablespace", "tablespace"), ("Storage", "storage"),
                ("Tablename", "tablename"), ("Schema", "schema"),
                ("Table Size in KB", "size")
            ]
        console.print("Tables after resolving referential integrity and filtering:")
        render_table(columns_key_map, migration_tables, len(migration_tables))


        get_index = round_robin_counter(len(dest_tbspace))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    move_table, connection_details, table, dest_tbspace, index_tbspace, get_index,
                    runstats, copy_opts, log_directory_path
                ) for table in migration_tables
            ]

            wait(futures)

    except Exception as e:
        print(e)
        print(traceback.format_exc())


@app.command()
def status(
        scope: Annotated[str, typer.Option(help="tables - lists the no of tables in block & COS;migration-runs - migration runs that ran till now")],
        user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
        password: Annotated[str, typer.Option(help="Password of the User ID")],
        hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
        log_directory_path: Annotated[str, typer.Option(
            help="Pass the log directory base where log files are stored.")] = "",
        dsn: Annotated[str, typer.Option(
            help="Pass the DSN name if it is already configured")] = None,
        database: Annotated[str, typer.Option(
            help="Database to be connected")] = "BLUDB",
        active_runs: Annotated[bool, typer.Option(help="active - lists the active migration runs;completed - lists the completed migration runs")] = False,
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001",
        enable_ssl: Annotated[bool, typer.Option(help="Enable SSL encryption for the database connection.")] = False):
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
    if scope not in ("tables", "migration-runs"):
        console.print(f"Invalid scope: {scope}")
        sys.exit(1)

    connection_details = {
        "user_id": user_id, "password": password, "hostname": hostname, "port": port,
        "database": database, "dsn": dsn, "enable_ssl": enable_ssl
    }
    conn_status = db2wh_pyodbc_connection(connection_details, True)

    if not conn_status:
        console.print(
            "Cannot connect to the Instance. Kindly check if the status if up and running."
        )
        return

    if scope == "tables":
        tables_in_block = []
        tables_in_cos = []
        available_tablespaces = get_tablespaces_in_block_and_cos(connection_details)
        object_tablespaces = get_list_of_objectspaces(connection_details)

        if not available_tablespaces:
            console.print("No tablespaces are available.")
            return

        for tb_space in available_tablespaces:
            tables_count = get_tables_cnt_under_tablespaces(connection_details, tb_space)

            (tables_in_cos if tb_space in object_tablespaces else tables_in_block).append(
                {
                    "tablespace": tb_space,
                    "tables_count": str(tables_count)
                }
            )

        columns_key_map = [("Tablespace", "tablespace"), ("Number of Tables", "tables_count")]

        console.print("Tablespaces in Block")
        render_table(columns_key_map, tables_in_block, len(tables_in_block))

        console.print("Tablespaces in COS")
        render_table(columns_key_map, tables_in_cos, len(tables_in_cos))

    else:
        if not log_directory_path:
            console.print("Please provide the log path to know the status of the migration runs")
            sys.exit(1)

        log_directory_path: Path = Path(log_directory_path)

        if not log_directory_path.is_dir():
            console.print(f"Log directory path does not exist: {log_directory_path.resolve()}")
            sys.exit(1)

        batches = list(d for d in log_directory_path.iterdir() if d.is_dir())

        if not batches:
            console.print("No migrations done.")
            return

        console.rule("[bold red]Migration Runs")
        console.print(
            "To check the complete logs and metrics, please find the log file "
            "in the respective locations:"
        )
        console.print(
            f"Log file: {log_directory_path.resolve() / '<batch_id>/<job_id>-<schema>-<table>.log'}"
        )
        console.print(
            f"Report file: {log_directory_path.resolve() / '<batch_id>/<job_id>-<schema>-<table>.json'}"
        )

        migration_jobs = list_migration_runs(batches, active_runs)

        columns_key_map = [
            ("BathchId", "batch_id"), ("JobId", "migration_job_id"), ("Table", "tablename"),
            ("Schema", "schema"), ("Source", "source_tablespace"),
            ("Destination", "destination_tablespace"), ("Phase", "phase_name"), ("Error", "error")
        ]

        parsed_data = parse_the_json_files_for_status(connection_details, migration_jobs)
        columns_key_map = [
            ("BathchId", "batch_id"), ("JobId", "migration_job_id"), ("Table", "tablename"),
            ("Schema", "schema"), ("Source", "source_tablespace"),
            ("Destination", "destination_tablespace"), ("Phase", "phase_name"), ("Error", "error"),
            ("Progress", "progress")
        ]

        render_table(columns_key_map, parsed_data, len(parsed_data))

@app.command()
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
        port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001",
        enable_ssl: Annotated[bool, typer.Option(help="Enable SSL encryption for the database connection.")] = False):

    """
    To cancel a run for the table migration run 
    """
    try:
        console.print("Test Connect to the Db2 warehouse instance")
        connection_details = {
            "user_id": user_id, "password": password, "hostname": hostname, "port": port,
            "database": database, "dsn": dsn, "enable_ssl": enable_ssl
        }

        conn_status = db2wh_pyodbc_connection(connection_details, True)

        if not conn_status:
            console.print(
                "Cannot connect to the Instance. Kindly check if the database is up and running."
            )

            return

        if log_file_name and os.path.exists(log_file_name):
            print("Removing the LOG File")
            os.remove(log_file_name)

        if report_file_name and os.path.exists(report_file_name):
            print("Removing the JSON File")
            os.remove(report_file_name)

        if use_adc is False:
            copy_opts = "COPY_USE_OTA,NO_STATS"
        else:
            copy_opts = "COPY_USE_OTA,USE_ADC,NO_STATS"

        print("Cancelling the table migration")
        cancel_terminate_admin_move_table(
            connection_details, schema_name, table_name, "TERM", src_tablespace, dest_tablespace,
            index_tbspace, copy_opts
        )
        cancel_terminate_admin_move_table(
            connection_details, schema_name, table_name, "CANCEL", src_tablespace, dest_tablespace,
            index_tbspace, copy_opts
        )

    except Exception as e:
        print(e)
