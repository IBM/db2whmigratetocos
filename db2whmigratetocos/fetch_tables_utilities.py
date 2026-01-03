"""
Copyright IBM Corp. 2024 All Rights Reserved.
Licensed Materials - Property of IBM
"""


from typing import Dict, List, Union
from rich.console import Console

from db2whmigratetocos.db2wh_db2_utilities import (
    get_schema_in_instance,
    get_tables_under_schema_in_db2woc,
    get_tables_under_tablespace_in_db2woc,
    get_tablespaces_in_block_and_cos,
    render_table,
    validate_input_objects,
)

console = Console()

def get_tables_by_tablespace(
        connection_details: dict,
        input_obj_list: Union[str, list],
        detail: bool,
        object_tablespaces: list
):
    console.print("Listing the tablespaces\n")
    console.print("Displaying till 75 tables for each tablespace")

    available_tablespaces = get_tablespaces_in_block_and_cos(connection_details)

    if not available_tablespaces:
        console.print("No tablespaces are available.")
        return []

    tablespaces = (
        available_tablespaces
        if input_obj_list == "all"
        else validate_input_objects(input_obj_list, available_tablespaces, "tablespaces")
    )

    if not tablespaces:
        console.print("No valid tablespaces found.")
        return []

    tables: List[Dict] = []

    # To render table columns in the specific order
    columns_key_map = [
        ("Tablename", "tablename"), ("Schema", "schema"), ("Table Size in KB", "size")
    ]

    for tb_space in tablespaces:
        console.rule(f"[bold orange4 italic]Tables in Tablespace - {tb_space}")
        tb_space_type = "COS" if tb_space in object_tablespaces else "Block-Storage"

        tb_space_size, tables_details, nos_tables = get_tables_under_tablespace_in_db2woc(
            connection_details, tb_space, detail
        )

        if not nos_tables:
            console.print("No tables found in the tablespace")
            continue

        if detail:
            console.print(
                f"The total size of tables in tablespace is {tb_space_size} KB"
            )

        tables.extend(
            {"tablespace": tb_space, "storage": tb_space_type, **table_details}
            for table_details in tables_details
        )

        console.print(f"The total number of tables in tablespace is {nos_tables}")
        render_table(columns_key_map, tables_details)

    return tables


def get_tables_by_schema(
        connection_details: dict,
        input_obj_list: Union[str, list],
        detail: bool,
        object_tablespaces: list
):
    available_schemas = get_schema_in_instance(connection_details)

    if not available_schemas:
        console.print("No schemas are available.")
        return []

    console.print("Listing the schemas\n")

    schemas = (
        available_schemas
        if input_obj_list == "all"
        else validate_input_objects(input_obj_list, available_schemas, "schemas")
    )

    if not schemas:
        console.print("No Valid schema found.")
        return []

    tables = []

    # To render table columns in the specific order
    columns_key_map = [
        ("Tablename", "tablename"), ("Tablespace", "tablespace"), ("Table Size in KB", "size")
    ]

    console.print("Displaying till 75 tables for each schema")

    for schema in schemas:
        console.rule(f"[bold orange4 italic]Tables in Schema - {schema}")

        schema_size, tables_details, nos_tables = get_tables_under_schema_in_db2woc(
            connection_details, schema, detail
        )

        if not nos_tables:
            console.print("No tables found in the schema")
            continue

        if detail:
            console.print(
                f"The total size of tables in schema is {schema_size} KB"
            )

        for table_details in tables_details:
            row = {
                "tablespace": table_details["tablespace"],
                "storage": (
                    "COS" if table_details["tablespace"] in object_tablespaces else "Block-Storage"
                ),
                "tablename": table_details["tablename"],
                "schema": schema
            }

            if detail:
                row["size"] = table_details["size"]

            tables.append(row)

        console.print(f"The total number of tables in schema is {nos_tables}")
        render_table(columns_key_map, tables_details)

    return tables
