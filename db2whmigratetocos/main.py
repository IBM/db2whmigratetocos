
import typer
from db2whmigratetocos.db2wh_db2_utilities import adm_move_table_ops_db2woc, db2wh_pyodbc_connection, get_table_move_time_estimate_in_db2woc, get_tables_under_tablespace_in_db2woc
from .db2whmigratetocos_install_prereq import db2whmigratetocos_init
from typing_extensions import Annotated
from rich.console import Console

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
def move(
    db2wh:Annotated[str,typer.Argument(help="Db2 warehouse setup- db2woc/db2wop")],
    user_id: Annotated[str, typer.Option(help="User Id to connect to Db2 warehouse Instance")],
    password: Annotated[str, typer.Option(help="Password of the User ID")],
    hostname: Annotated[str, typer.Option(help="Hostname of the Db2 warehouse Instance")],
    database:Annotated[str,typer.Option(help="Database to be connected")] ="BLUDB",
    port: Annotated[str, typer.Option(help="Port to be used for Db2 warehouse Instance")]="50001"):
    
    """
    Move the tablespaces to COS from Block
    """
    typer.echo("Initiating the process of moving to NCOS")

    print("Test Connect to the Db2 warehouse instance")
    db2wh_pyodbc_connection(user_id,password,hostname,port,database,True)
    if db2wh == "db2woc":
        print()
        print()
        console.print("Initiating the Data Movement in Db2woc instance")
        console.print("DEFAULT SOURCE - USERSPACE1 : DEAFULT DESTINATION - OBJSTORESPACE1")
        print()
        estimate_size,tables_in_userspace= get_tables_under_tablespace_in_db2woc(user_id,password,hostname,port,database,"USERSPACE1")
        time_taken = get_table_move_time_estimate_in_db2woc(user_id,password,hostname,port,database)
        print(estimate_size)
        print(time_taken)
        print(estimate_size * time_taken)
        print("Do you want to proceed?")
        accept = input("Enter if you want to proceed, one of the following options:\n 1.Accept\n 2.Decline\n")
        if int(accept) == 1:
            print("Initiating the migration for each of the table, proceeding with next steps....")
            for items in tables_in_userspace:
                 adm_move_table_ops_db2woc(user_id,password,hostname,port,database,items[1],items[0])
        else:
            print("Aboritng the migration process")
        
        

       
        
 
    


   








   
    
    
