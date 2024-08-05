
from datetime import datetime
from typing import List
import pandas as pd
import typer
from db2whmigratetocos.db2wh_db2_utilities import adm_move_table_ops_db2woc, db2wh_pyodbc_connection, get_schema_in_instance, get_table_move_time_estimate_in_db2woc, get_tables_under_schema_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc, get_tbpsace_name_for_table
from .db2whmigratetocos_install_prereq import db2whmigratetocos_init
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.align import Align

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
    setup the Db2 warehouse migrate tool 
    """
    typer.echo("Installing the Db2 warehouse migrate tool")
    db2whmigratetocos_init()

@app.command()
def list(
    scope:Annotated[str,typer.Option(help="List the db2 object - tablespace")],
    list:Annotated[str, typer.Option(help="list of tablespaces or all tablespaces and export to CSV")],
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    detail:Annotated[bool, typer.Option(help="List tables with its schema & size- true/false")]=False,
    export_csv:Annotated[bool, typer.Option(help="Export the table data into a CSV")]=False,
    database:Annotated[str,typer.Option(help="Database to be connected")] ="BLUDB",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")]="50001"):

    """
    LIST the tables by tablespaces in detail
    """
    console.print("Test Connect to the Db2 warehouse instance")
    conn_status =  db2wh_pyodbc_connection(user_id,password,hostname,port,database,True)
    print()
    if conn_status:
            tablespace_list = []
            invalid_tbspace_list = []
            schema_list = []
            input_obj_list = list.split(",")
            all_objects = 'all' if 'all' in input_obj_list else None
            try:
             valid_tablespace_list = get_tablespaces_in_block_and_cos(user_id,password,hostname,port,database)
            except Exception as e:
                print(e)
                print("unable to fetch the tablespaces, check if the instance is up and running")  
            if scope == "tablespace":
                console.print("Listing the tablespaces")
                #validating the tablespace list
                if all_objects == 'all':
                    tablespace_list = valid_tablespace_list
                    if schema_list is None:
                        print("Check if the Db2 warehouse instance is up and running")
                else:
                    if detail != True:
                        console.print("Kindly provide --detail to  get the tables for the list of tablespaces")
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
                        print("Kindly check the tablespace list that is provided as input")
                if tablespace_list is None:
                    print("Check if the Db2 warehouse instance is up and running")
                else:
                    if len(tablespace_list) != 0:
                        if detail==True:
                                console.print("Gathering information about the tables in the Tablespace")
                                console.print("Displaying till 75 tables for each tablespace")
                                tables_list_in_tablespaces = []
                                for tbspace in tablespace_list:
                                        tbspace_store = " "
                                        if "OBJSTORE" in tbspace:
                                            tbspace_store = "COS"
                                        else:
                                            tbspace_store = "Block"
                                        print(tbspace)
                                        console.rule("[bold red]Tables in Tablespace - {tablespace}".format(tablespace=tbspace))
                                        total_estimate,tables,table_cnt = get_tables_under_tablespace_in_db2woc(user_id,password,hostname,port,database,tbspace)
                                        tb_table = Table()
                                        tb_table.add_column("Tablename",justify="center", style="cyan")
                                        tb_table.add_column("Schema",justify="center", style="cyan" )
                                        tb_table.add_column("Table Size in KB",justify="center", style="cyan")
                                        if len(tables) != 0:
                                            console.print("The total number of tables in tablespace is {table_cnt}".format(table_cnt=table_cnt))
                                            console.print("The total size of tables in tablespace is {total_estimate} KB".format(total_estimate=total_estimate))
                                            count = 0
                                            for table in tables:
                                                count = count+1
                                                tb_table.add_row(table[0],table[1],str(table[2]))
                                                tables_list_in_tablespaces.append([tbspace,table[0],table[1],str(table[2]),str(tbspace_store)])
                                                if count >= 75:
                                                    break
                                            console.print(tb_table)
                                        else:
                                            console.print("No tables found in the tablespace")
                                if export_csv == True:
                                    console.print("Exporting the data into CSV")
                                    df = pd.DataFrame(tables_list_in_tablespaces, columns=["Tablespace","Tablename", "Schema", "Size","Storage"])
                                    filename = "db2whmigratetocos-"+tbspace +"-tables-list-"+datetime.now().isoformat()+".csv"
                                    df.to_csv(filename, index=False)
                                    console.print("Exporting the list of tables in the tablespace")
                                    print(f"Data saved to CSV file: {filename}")
                        else:
                                tbs_block =[]
                                tbs_cos = []
                                tbs_block_table = Table(show_footer=False)
                                tbs_cos_table = Table(show_footer=False)
                                tbs_block_table.add_column("TABLESPACES in Block",justify="center", style="cyan", no_wrap=True)
                                tbs_cos_table.add_column("TABLESPACES in COS",justify="center", style="cyan", no_wrap=True)
                                for row in tablespace_list:
                                   if "OBJ" in row:
                                      tbs_cos_table.add_row(str(row))
                                      tbs_cos.append(str(row))
                                   else:
                                      tbs_block_table.add_row(str(row))
                                      tbs_block.append(str(row))
                                console.print(tbs_block_table)
                                console.print(tbs_cos_table)
                                if export_csv == True:
                                    console.print("Exporting the tablespace list into CSV")
                                    df_blk = pd.DataFrame(tbs_block, columns=["Tablespace"])
                                    df_Cos = pd.DataFrame(tbs_cos, columns=["Tablespace"])
                                    blk_filename = "tbspaces-in-block-"+datetime.now().isoformat()+".csv"
                                    cos_filename = "tbspaces-in-cos-"+datetime.now().isoformat()+".csv"
                                    df_blk.to_csv(blk_filename, index=False)
                                    df_Cos.to_csv(cos_filename, index=False)
                                    console.print("The tablespaces in block can be found in " +blk_filename)
                                    console.print("The tablespaces in cos can be found in " +cos_filename)
                    else:
                        print("No Tablespaces found")
            if scope == "schema":
                invalid_schema_list = []
                console.print("Listing the {scope}".format(scope=scope))
                try:
                    valid_schema_list = get_schema_in_instance(user_id,password,hostname,port,database)
                except Exception as e:
                    print(e)
                    print("unable to fetch the schemas, check if the instance is up and running")   
                if all_objects == 'all':
                    schema_list = valid_schema_list
                    print(schema_list)
                    if schema_list is None:
                        print("Check if the Db2 warehouse instance is up and running")
                else:
                    #validating the schema list
                    if detail != True:
                        console.print("Kindly provide --detail to  get the tables for the list of tablespaces")
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
                        if detail == True:
                                console.print("Gathering the information about the tables in the schema")
                                console.print("Displaying till 75 tables for each schema")
                                tables_in_schema = []
                                for schema in schema_list:
                                    print(schema)
                                    console.rule("[bold red]Tables in Schema - {schema}".format(schema=schema))
                                    table_cnt,total_estimate,tables = get_tables_under_schema_in_db2woc(user_id,password,hostname,port,database,schema)
                                    sc_table = Table()
                                    sc_table.add_column("Tablename",justify="center", style="cyan")
                                    sc_table.add_column("Tables Size in KB",justify="center", style="cyan" )
                                    if len(tables) != 0:
                                            console.print("The total number of tables in schema is {table_cnt}".format(table_cnt=table_cnt))
                                            console.print("The total size of tables in schema is {total_estimate} KB".format(total_estimate=total_estimate))
                                            count = 0
                                            for table in tables:
                                                count = count+1
                                                tb_table.add_row(table[0],str(table[1]))
                                                tables_in_schema.append([schema,table[0],table[1]])
                                                if count >= 75:
                                                    break
                                            console.print(tb_table)
                                    else:
                                            console.print("No tables found in the schema")
                                if export_csv == True:
                                    console.print("Exporting the schema data into CSV")
                                    df = pd.DataFrame(tables_in_schema, columns=["Schema","Tablename","Size"])
                                    filename = "db2whmigratetocos-schemas-tables-list-"+datetime.now().isoformat()+".csv"
                                    df.to_csv(filename, index=False)
                                    console.print("Exporting the list of tables in the schema")
                                    print(f"Data saved to CSV file: {filename}")
                        else:
                            schema_table = Table(show_footer=False)
                            for row in schema_list:
                                schema_table.add_row(str(row))
                            console.print(schema_table)
                            if export_csv == True:
                                    console.print("Exporting the schema list into CSV")
                                    df = pd.DataFrame(schema_list, columns=["Schema"])
                                    filename = "db2whmigratetocos-schemas-list-"+datetime.now().isoformat()+".csv"
                                    df.to_csv(filename, index=False)
                                    console.print("Exporting the list of tables in the schema")
                                    print(f"Data saved to CSV file: {filename}")
    else:
        print("Cannot connect to the Instance. Kindly check if the status if up and running")
        
@app.command()
def move(
    scope: Annotated[str, typer.Option(help="Move tables by tablespace/schema")],
    list: Annotated[str, typer.Option(help="Source tablespace/schema in block storage - all/comma seperated list of tablespace/schema")],
    dest_tbspace:Annotated[str, typer.Option(help="Destination tablespace in cos, where the data needs to be moved ")],
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    skip_schema: Annotated[str, typer.Option(help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] ="none",
    skip_tbspace: Annotated[str, typer.Option(help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")]="none",
    database:Annotated[str,typer.Option(help="Database to be connected")] ="BLUDB",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")]="50001"):
    
    """
    Move the tablespaces to COS from Block
    """

    console.print("Test Connect to the Db2 warehouse instance")
    db2wh_pyodbc_connection(user_id,password,hostname,port,database,True)
    print()
    src_db2_obj_list = list.split(",")
    skip_tbspace_list = skip_tbspace.split(",")
    skip_schema_list = skip_schema.split(",")
    input_list_csv = 'csv' if '.csv' in src_db2_obj_list else None
    csv_columns =  ['Tablespace', 'Tablename', 'Schema', 'Size', 'Storage']
    if scope == "tablespace":               
            invalid_tbspace_list= []
            valid_tbspace_list = get_tablespaces_in_block_and_cos(user_id,password,hostname,port,database)
            all_tablespaces = 'all' if 'all' in src_db2_obj_list else None
            if input_list_csv == 'csv':
                for item in src_db2_obj_list:
                    if '.csv' in item:
                        tables_list_in_df = pd.read_csv(filepath=item)
                        if csv_columns == list(tables_list_in_df):
                            for index,row in tables_list_in_df:
                                if row['Tablespace'] in valid_tbspace_list:
                                    if row['Tablespace'] not in skip_tbspace_list:
                                        adm_move_table_ops_db2woc(user_id,password,hostname,port,database,row['Schema'],row['Tablename'],"INIT",row['Tablespace'],dest_tbspace)      
                                    else:
                                         print("skipping the tablespace as per the input")
                                else:
                                     print("the tablespace name is invalid")
                        else:
                            print("Kindly check the column names in the csv provided")
                            print("Required Format")
                            print(csv_columns)
                            print("Provided format")
                            print(list(tables_list_in_df))
            else:
                if all_tablespaces =='all':
                    tbspace_list = valid_tbspace_list
                else:
                    for tbspace in src_db2_obj_list:
                        if tbspace in valid_tbspace_list:
                            invalid_tbspace_list.append(tbspace)
                    
                    if len(invalid_tbspace_list)>0:
                        print("skipping invalid tablespaces")
                        print(invalid_tbspace_list)
                        for tbspace in valid_tbspace_list:
                            if tbspace in invalid_tbspace_list:
                                src_db2_obj_list.remove(tbspace)
                        tbspace_list =src_db2_obj_list
                    else:
                        tbspace_list = src_db2_obj_list
                for tbspace in tbspace_list:
                    tables_in_userspace =[]
                    if tbspace not in skip_tbspace_list:
                        tables_in_userspace=get_tabname_schemaname_under_tablespace_in_db2woc(user_id,password,hostname,port,database,tbspace)
                        print("Initiating the migration for each of the table, proceeding with next steps....")
                        tables_cnt = len(tables_in_userspace)
                        print(tables_cnt)
                        moved_cnt = 0
                        if len(tables_in_userspace) !=0 :
                            for items in tables_in_userspace:
                                print()
                                print(items)
                                adm_move_table_ops_db2woc(user_id,password,hostname,port,database,items[1],items[0],"INIT",tbspace,dest_tbspace)
                                print()
                                moved_cnt = moved_cnt+1
                                print(str(moved_cnt) + "/" +str(tables_cnt) +" is done ")
                                print("------------------------------------------------------------------")
                                if len(tables_in_userspace) == 0:
                                    print("no tables found in the tablespace")
                                else:
                                    print("Skipping the tablespace - " +  tbspace)
    if scope=="schema":
        invalid_schema_list=[]
        valid_schema_list = get_schema_in_instance(user_id,password,hostname,port,database)
        all_schemas = 'all' if 'all' in src_db2_obj_list else None
        if valid_schema_list!= None:
            if input_list_csv == 'csv':
                    for item in src_db2_obj_list:
                        if '.csv' in item:
                            tables_list_in_df = pd.read_csv(filepath=item)
                            if csv_columns == list(tables_list_in_df):
                                for index,row in tables_list_in_df:
                                    if row['Schema'] in schema_list:
                                        if row['Schema'] not in skip_schema_list:
                                            adm_move_table_ops_db2woc(user_id,password,hostname,port,database,row['Schema'],row['Tablename'],"INIT",row['Tablespace'],dest_tbspace)   
                                        else:
                                            print("skipping the schema as per the input")
                                    else:
                                        print("The specified schema is not valid")
                            else:
                                print("Kindly check the column names in the csv provided")
                                print("Required Format")
                                print(csv_columns)
                                print("Provided format")
                                print(list(tables_list_in_df))
            else:
                #validation of schema and setting the list for movment
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
                    tables_in_schema =[]
                    if schema not in skip_schema_list:
                        tables_cnt,tota_size,tables_in_schema = get_tables_under_schema_in_db2woc(user_id,password,hostname,port,database,schema)
                        print(tables_cnt)
                        print(tota_size)
                        print("Initiating the migration for each of the table, proceeding with next steps....")
                        if len(tables_in_schema) !=0 :
                            for item in tables_in_schema:
                                source_tablespace = get_tbpsace_name_for_table(user_id,password,hostname,port,database,item[0])
                                if source_tablespace not in dest_tbspace:
                                    adm_move_table_ops_db2woc(user_id,password,hostname,port,database,schema,items[0],"INIT",source_tablespace,dest_tbspace)
                                    print()
                                    moved_cnt = moved_cnt+1
                                    print(str(moved_cnt) + "/" +str(tables_cnt) +" is done ")
                                    print("------------------------------------------------------------------")
                        if len(tables_in_schema) == 0:
                                print("no tables found in the schema")
                    else:
                        print("Skipping the schema - " +  schema)
        else:
            print("Kindly check the schema list that is provided as input")
            
            
                        
 
    


   








   
    
    
