import subprocess

from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import typer
from typing_extensions import Annotated
from rich.console import Console

from db2whmigratetocos.db2wh_db2_utilities import validate_and_get_df_from_the_csv

app = typer.Typer()
console = Console()


def round_robin_counter(n):
    index = -1

    def next_index():

        nonlocal index
        index = (index + 1) % n
        return index

    return next_index


def execute_move(table_details: dict, params: dict, rr_callback: Callable):

    if params["skip_schema"] and table_details["schema"].lower() in params["skip_schema"].lower():

        return (
            f"Skipping table '{table_details['tablename']}' as it is part of skip "
            f"scheme '{params['skip_schema']}'"
        )

    if params["skip_tbspace"] and table_details["tablespace"].lower() in params["skip_tbspace"]:

        return (
            f"Skipping table '{table_details['tablename']}' as it is part of skip "
            f"tablespace '{params['skip_tbspace']}'"
        )

    move_params = [
        item
        for k, v in params.items()
        if k not in ("dest_tbspace", "runstats", "enable_ssl") and v is not None
        for item in (f"--{k.replace('_', '-')}", v)
    ]

    move_params += [
        f"--{param.replace('_', '-')}"
        for param in ("runstats", "enable_ssl")
        if params[param]
    ]

    dest_tbspace = params["dest_tbspace"].strip(",").split(",")[rr_callback()]

    move_params.extend([
        "--dest-tbspace", dest_tbspace, "--scope", "table",
        "--table-name", table_details["tablename"],
        "--schema-name", table_details["schema"]
    ])

    cmd = ["db2whmigratetocos", "move"] + move_params

    resp = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return resp.stdout, resp.stderr


@app.command()
def cmove(
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    csv_input: Annotated[str, typer.Option(help="Input CSV file")],
    log_directory_path: Annotated[str, typer.Option(help="Pass the log directory base path to store the log files")] = None,
    database: Annotated[str, typer.Option(help="Database to be connected")] = "BLUDB",
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")] = "db2inst1",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")] = "50001",
    dsn: Annotated[str, typer.Option(help="Pass the DSN name configured in ODBC Driver Config File (odbcinst.ini)")] = None,
    runstats: Annotated[bool, typer.Option(help="Execute RUNSTAT command")] = False,
    dest_tbspace: Annotated[str, typer.Option(help="Destination tablespace in cos, where the data needs to be moved ")] = "OBJSTORESPACE1",
    index_tbspace : Annotated[str, typer.Option(help="Destination index tablespace in cos, where the index needs to be moved ")] = None,
    copy_opts: Annotated[str, typer.Option(help="Copy options to be passed.")] = "COPY_USE_OTA,NO_STATS",
    skip_schema: Annotated[str, typer.Option(help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] = None,
    skip_tbspace: Annotated[str, typer.Option(help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")] =None,
    enable_ssl: Annotated[bool, typer.Option(help="Enable SSL encryption for the database connection.")] = False
):
    """
    Move tables concurrently.
    """

    if Path(csv_input).suffix.lower() == ".csv":
        csv_content = validate_and_get_df_from_the_csv(csv_input)

        params = {
            "hostname": hostname,
            "password": password,
            "log_directory_path": log_directory_path,
            "database": database,
            "user_id": user_id,
            "port": port,
            "dsn": dsn,
            "runstats": runstats,
            "dest_tbspace": dest_tbspace,
            "index_tbspace": index_tbspace,
            "copy_opts": copy_opts,
            "skip_schema": skip_schema,
            "skip_tbspace": skip_tbspace,
            "enable_ssl": enable_ssl
        }

        get_index = round_robin_counter(len(dest_tbspace.strip(",").split(",")))

        with ThreadPoolExecutor(max_workers=10) as executor:

            fututres = {
                executor.submit(execute_move, tab_det, params, get_index) : tab_det
                for tab_det in csv_content
            }

            for fut in as_completed(fututres):
                console.print(fut.result())

    else:
        console.print(f"{csv_input} is not a csv file.")
