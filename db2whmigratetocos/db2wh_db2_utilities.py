
import subprocess
import time
import pyodbc
from rich.console import Console
from rich.table import Table



from db2whmigratetocos.queries import LIST_TABLES_IN_TSPACE, LIST_TBSPACES, TAB_SIZE
console = Console()


def get_connection_string(user:str,password:str,hostname:str,port:str,database:str):
    home_path = check_home_path()
    Driver = "Driver={"+home_path.strip()+"/db2_cli_odbc_driver/odbc_cli/clidriver/lib/libdb2o.so};"
    Database="Database="+database+";"
    Hostname="Hostname="+hostname+";"
    Port = "Port="+port+";"
    Uid = "Uid="+user+";"
    Pass = "Pwd="+password+";"
    Security ="Security=ssl;"
    Protocol="Protocol=TCPIP;"
    con_str = Driver+Database+Hostname+Port+Uid+Pass+Security+Protocol
    return con_str
     
def check_home_path():
   try:
     HOME = run_command("echo $HOME")
     return HOME
   except Exception as e:
      print(e)

def run_command(command):
    result = subprocess.check_output(command, shell=True,text=True)
    return result


def get_tables_under_tablespace_in_db2woc(user:str,password:str,hostname:str,port:str,database:str,tablespace:str):
    try:
        tb_table = Table(title="Tables in {tablespace} in Db2WoC".format(tablespace=tablespace))
        tb_table.add_column("Table Name", justify="center", style="cyan", no_wrap=True)
        tb_table.add_column("Schema Name ", style="magenta")
        tb_table.add_column("Table Size ", style="magenta")
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        table_cnt = 0
        total_estimate =0
       
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()  
        cnxn.close()
        print("Listing all the tables") 
        with console.status("[magenta]Covid detector booting up") as status:
            for item in rows:
                    if  "SYS"  not in item[1]:
                            status.update(
                                    status="[bold magenta]Estimating each table size",
                                    spinner="material",
                                    spinner_style="green",
                            )
                            table_cnt = table_cnt + 1
                            est_size = tab_size_by_table_name(user,password,hostname,port,database,item[1],item[0])
                            total_estimate += int(est_size)
                            tb_table.add_row(item[0],item[1],str(est_size))
        console.print("The number of Tables:", table_cnt)
        console.print(tb_table)
        console.print("Total estimated size to move is")
        console.print(total_estimate)
    except Exception as e:
         print(e)
         

def tab_size_by_table_name(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str):
        try:
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(TAB_SIZE.format(TABSCHEMA = schemaname,TABNAME=tablename))
            rows = conn.fetchall()
            cnxn.close()
            for item in rows:
                return int(item[0])+int(item[1])+int(item[2])+int(item[3])+int(item[4])
        except Exception as e:
            print(e) 
              
def db2wh_pyodbc_connection(user:str,password:str,hostname:str,port:str,database:str,test_con:bool):
        try:
            connection_string = get_connection_string(user,password,hostname,port,database)
            cnxn = pyodbc.connect(connection_string)
            if test_con:
                 try:
                    conn = cnxn.cursor()
                    conn.execute(LIST_TBSPACES)
                    rows = conn.fetchall()
                    print("Connected to the Instance - {hostname}".format(hostname=hostname))
                    print("Test Connection Successful")
                 except Exception as e:
                    print(e)
            else:
                  return cnxn
        except Exception as e:
            print(e) 

