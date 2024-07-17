
import datetime
import subprocess
import time
import pyodbc
from rich.console import Console
from rich.table import Table

from db2whmigratetocos.constants import SAMPLE_DATA
from db2whmigratetocos.queries import  ADM_MOVE_TABLE_CLEANUP_ERROR_STATE, ADM_MOVE_TABLE_CMD_DB2WOC, ADM_MOVE_TABLE_CMD_DB2WOC_MOVE, ADM_MOVE_TABLE_FIND_PHASE, ADM_MOVE_TABLE_PHASE_ERROR_STATE, DRP_SAM_TABLE, LIST_TABLES_IN_TSPACE, LIST_TBSPACES, SAMPLE_TABLE, TAB_SIZE
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


def drop_sample_table(user:str,password:str,hostname:str,port:str,database:str):
    try:
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(DRP_SAM_TABLE)
        conn.commit()
        cnxn.close()
    except Exception as e:
         print(e)



def get_tables_under_tablespace_in_db2woc(user:str,password:str,hostname:str,port:str,database:str,tablespace:str):
    try:
        tb_table = Table(title="Tables in {tablespace} in Db2WoC".format(tablespace=tablespace))
        tb_table.add_column("Table Name", justify="center", style="cyan", no_wrap=True)
        tb_table.add_column("Schema Name ", style="magenta")
        tb_table.add_column("Table Size ", style="magenta")
        table_names_in_tablespace = []
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        table_cnt = 0
        total_estimate =0
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()  
        cnxn.close()
        print("Listing all the tables") 
        with console.status("") as status:
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
                            table_names_in_tablespace.append([item[0],item[1]])
        console.print("The number of Tables:", table_cnt)
        console.print(tb_table)
        console.print("Total estimated size to move is")
        console.print(total_estimate)
        return total_estimate,table_names_in_tablespace
    except Exception as e:
         print(e)

def get_table_move_time_estimate_in_db2woc(user:str,password:str,hostname:str,port:str,database:str):
     print("creating a sample table and schema for movement")
     create_table_for_sample_table(user,password,hostname,port,database)
     print("loading the data in sample table and schema for movment")
     load_data_to_the_sample_table(user,password,hostname,port,database)
     print("Testing the movement of the sample table")
     time_taken_per_kb_in_secs =  admin_move_table_with_move(user,password,hostname,port,database,'DB2INST1','DB2WHTESTTABLE','MOVE')
     print("cleaning the sample table")
     drop_sample_table(user,password,hostname,port,database)
     return time_taken_per_kb_in_secs

            

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


     
def load_data_to_the_sample_table(user:str,password:str,hostname:str,port:str,database:str):
     try:
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        insert_query = f"INSERT INTO DB2INST1.DB2WHTESTTABLE VALUES (?, ?)"
        conn.executemany(insert_query, SAMPLE_DATA)
        conn.commit()
        cnxn.close()
     except Exception as e:
         print(e)

      
def create_table_for_sample_table(user:str,password:str,hostname:str,port:str,database:str):
    try:
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(SAMPLE_TABLE)
        conn.commit()
        cnxn.close()
    except Exception as e:
         print(e)



def admin_move_table_with_move(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,option:str):
    try:
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC_MOVE.format(SCHEMANAME = schemaname,TABLENAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if item[0] == 'INIT_START':
                init_start = item[1]
            if item[0] == 'CLEANUP_END':
                cleanup_end = item[1]
            if item[0] == 'ORIGINAL_TBLSIZE':
                table_size = item[1]
        init_time = datetime.datetime.strptime(init_start, "%Y-%m-%d-%H.%M.%S.%f") 
        cleanup_end = datetime.datetime.strptime(cleanup_end, "%Y-%m-%d-%H.%M.%S.%f")
        time_taken_per_kb_in_secs = int((cleanup_end - init_time).total_seconds())/int(table_size)
        return time_taken_per_kb_in_secs
             
    except Exception as e:
         print(e)

def find_adm_status_for_a_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str):
    try:
        table_phase = " "
        connection_string = get_connection_string(user,password,hostname,port,database)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        print(rows)
        for item in rows:
             if item[0] == 'STATUS':
                  print(table_phase)
                  table_phase = item[1]
                  return table_phase
    except Exception as e:
         print(e)
   

def adm_move_table_ops_db2woc(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,status:str):
    try:
        init_start = ""
        cleanup_end = ""
        while status !="COMPLETE":
            print("INIT Phase for {TABLENAME}".format(TABLENAME=tablename))
            status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"INIT")
            if status == "INIT":
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"INIT")
            if status == "COPY":
                print("COPY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"COPY")
            if status == "REPLAY":
                print("REPLAY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"REPLAY")
                print("SWAP Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"SWAP")
                if status == "COMPLETE":
                    init_time = datetime.datetime.strptime(init_start, "%Y-%m-%d-%H.%M.%S.%f") 
                    cleanup_end = datetime.datetime.strptime(cleanup_end, "%Y-%m-%d-%H.%M.%S.%f")
                    time_taken= int((cleanup_end - init_time).total_seconds())
                    print("Movement COMPLETE for {TABLENAME} in".format(TABLENAME=tablename) + str(time_taken))
    except Exception as e:
        print(e)
                  
def adm_move_table_phase(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,phase:str):
        init_start = ""
        cleanup_end = ""
        try:
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME = schemaname,TABLENAME=tablename,OPTION=phase))
            rows = conn.fetchall()
            for item in rows:
                if item[0] == 'INIT_START':
                    init_start = item[1]
                if item[0] == 'STATUS':
                    status = item[1]
                if item[0] == 'CLEANUP_END':
                    cleanup_end = item[1]
            return status,init_start,cleanup_end
        except Exception as e:
            print(e.args)
            x,y = e.args
            print(x)
            print(y)
            if ADM_MOVE_TABLE_PHASE_ERROR_STATE in y:
                     status = find_adm_status_for_a_table(user,password,hostname,port,database,tablename)
                     print("Error: " + ADM_MOVE_TABLE_PHASE_ERROR_STATE)
                     print("Failed in {status} and retrying the movement from the status".format(status=status))
                     return status,init_start,cleanup_end
            if ADM_MOVE_TABLE_CLEANUP_ERROR_STATE in y:
                     status = find_adm_status_for_a_table(tablename)
                     print("Error: " + ADM_MOVE_TABLE_CLEANUP_ERROR_STATE)
                     print("Cleaning up the failed movement and retrying the movement from INIT")
                     if status != "COMPLETE" or "CLEANUP":
                        status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"CANCEL")
                        status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"TERM")
                        adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT")
                     
                          
     
     
