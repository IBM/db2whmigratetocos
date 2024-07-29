
from datetime import datetime
from typing import List
import pandas as pd
import typer
from db2whmigratetocos.db2wh_db2_utilities import adm_move_table_ops_db2woc, db2wh_pyodbc_connection, get_schema_in_instance, get_table_move_time_estimate_in_db2woc, get_tables_under_tablespace_in_db2woc, get_tablespaces_in_block_and_cos, get_tabname_schemaname_under_tablespace_in_db2woc
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
def install():
    """
    Install the Db2 warehouse migrate tool 
    """
    typer.echo("Installing the Db2 warehouse migrate tool")
    db2whmigratetocos_init()

@app.command()
def list(
    db2_object:Annotated[str,typer.Option(help="List the db2 object - tablespace")],
    db2_obj_list:Annotated[str, typer.Option(help="list of tablespaces or all tablespaces and export to CSV")],
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    list_tables:Annotated[bool, typer.Option(help="List tables with its schema & size- true/false")]=False,
    export_csv:Annotated[bool, typer.Option(help="Export the table data into a CSV")]=False,
    database:Annotated[str,typer.Option(help="Database to be connected")] ="BLUDB",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")]="50001"):

    """
    LIST the tables by tablespaces in detail
    """
    console.print("Test Connect to the Db2 warehouse instance")
    db2wh_pyodbc_connection(user_id,password,hostname,port,database,True)
    print()
    tablespace_list = []
    schema_list = []
    tbspace_input_obj = db2_obj_list.split(",")
    all_tablespaces = 'all' if 'all' in tbspace_input_obj else None
    if db2_object == "tablespace":
        console.print("Listing the tablespaces")
        if all_tablespaces == 'all':
            tablespace_list = get_tablespaces_in_block_and_cos(user_id,password,hostname,port,database)
        else:
            if list_tables != True:
                 console.print("Kindly provide --show-detail as True to get the tables for the list of tablespaces")
            tablespace_list = tbspace_input_obj
        if tablespace_list is None:
            print("Check if the Db2 warehouse instance is up and running")
        else:
            if len(tablespace_list) != 0:
                if list_tables==True:
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
                                tb_table.add_column("Table Size",justify="center", style="cyan")
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
    if db2_object == "schema":
        console.print("Listing the {db2_object}".format(db2_object=db2_object))
        schema_list = get_schema_in_instance(user_id,password,hostname,port,database)
        print(schema_list)
        
@app.command()
def move(
    src_tbspace: Annotated[str, typer.Option(help="Source tablespace in block storage - all/comma seperated list of tablespaces")],
    dest_tbspace:Annotated[str, typer.Option(help="Destination tablespace in cos, where the data needs to be moved ")],
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    skip_schema_name: Annotated[str, typer.Option(help="Skips an individual schema or a set of schmeas in the list of source tablespaces")] ="none",
    skip_tbspace: Annotated[str, typer.Option(help="Source tablespaces in block that needs to be skipped - none/comma seperated list of tablespaces")]="none",
    database:Annotated[str,typer.Option(help="Database to be connected")] ="BLUDB",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")]="50001"):
    
    """
    Move the tablespaces to COS from Block
    """

    console.print("Test Connect to the Db2 warehouse instance")
    db2wh_pyodbc_connection(user_id,password,hostname,port,database,True)
    print()
    src_tbspace_list = src_tbspace.split(",")
    skip_tbspace_list = skip_tbspace.split(",")
    skip_schema_name = skip_schema_name.split(",")
    all_tablespaces = 'all' if 'all' in src_tbspace_list else None
    skip_schema_name_flag = 'none' if 'none' in skip_schema_name else None
    if len(src_tbspace) > 1:
         if skip_schema_name_flag == 'none':
            print("Skip Schema option is available only for a single source tablespace")      
    tbspace_list = get_tablespaces_in_block_and_cos(user_id,password,hostname,port,database)
    if all_tablespaces != 'all':
        for tbspace in src_tbspace_list:
            if tbspace in tbspace_list:
                if tbspace not in skip_tbspace_list:
                    tables_in_userspace=get_tabname_schemaname_under_tablespace_in_db2woc(user_id,password,hostname,port,database,tbspace)
                    print("Initiating the migration for each of the table, proceeding with next steps....")
                    tables_cnt = len(tables_in_userspace)
                    moved_cnt = 0
                    if len(tables_in_userspace) !=0 :
                        for items in tables_in_userspace:
                            if items[1] not in skip_schema_name:
                                print()
                                adm_move_table_ops_db2woc(user_id,password,hostname,port,database,items[1],items[0],"INIT",src_tbspace,dest_tbspace)
                                print()
                                moved_cnt = moved_cnt+1
                                print(str(moved_cnt) + "/" +str(tables_cnt) +" is done ")
                                print("------------------------------------------------------------------")
                    if len(tables_in_userspace) == 0:
                       print("no tables found in the tablespace")
                else:
                    print("Skipping the tablespace - " +  tbspace)
            else:
                print("The tablespace name is not valid -  " + tbspace)
    else:
         for tbspace in tbspace_list:
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
                            adm_move_table_ops_db2woc(user_id,password,hostname,port,database,items[1],items[0],"INIT",src_tbspace,dest_tbspace)
                            print()
                            moved_cnt = moved_cnt+1
                            print(str(moved_cnt) + "/" +str(tables_cnt) +" is done ")
                            print("------------------------------------------------------------------")
                    if len(tables_in_userspace) == 0:
                       print("no tables found in the tablespace")
                       print()
                else:
                    print("Skipping the tablespace - " +  tbspace)

       
        
 
    


   








   
    
    
