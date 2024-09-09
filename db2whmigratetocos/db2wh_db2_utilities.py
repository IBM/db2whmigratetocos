
import datetime
import json
import subprocess
import os
import uuid 

from rich.console import Console
from rich.table import Table

from db2whmigratetocos.queries import  ADM_MOVE_TABLE_CLEANUP_ERROR_STATE, ADM_MOVE_TABLE_CMD_DB2WOC, ADM_MOVE_TABLE_CMD_DB2WOC_MOVE, ADM_MOVE_TABLE_FIND_PHASE, ADM_MOVE_TABLE_PHASE_ERROR_STATE, ADM_MOVE_TABLE_STRUCK_PHASE, DRP_SAM_TABLE, LIST_SCHEMAS, LIST_TABLES_IN_SCHEMA, LIST_TABLES_IN_TSPACE, LIST_TBSPACE_BY_TABNAME, LIST_TBSPACES, SAMPLE_TABLE, TAB_SIZE
import multiprocessing as mp

console = Console()


# os_ functions
def check_home_path():
   try:
     HOME = run_command("echo $HOME")
     return HOME
   except Exception as e:
      print(e)

def run_command(command):
    result = subprocess.check_output(command, shell=True,text=True)
    return result



#db2 utility functions

def get_tablespaces_in_block_and_cos(user:str,password:str,hostname:str,port:str,database:str):
    try:
            user_tablespaces_list = []
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(LIST_TBSPACES)
            rows = conn.fetchall()
            cnxn.close()
            for item in rows:
                if "SYS" not in item[0] and "TS4CONSOLE" not in item[0] and "BIGSQLCATUTILITY" not in item[0] and "TEMP" not in item[0] and "TMP" not in item[0] :
                    user_tablespaces_list.append(item[0])
            return user_tablespaces_list
    except Exception as e:
            print(e) 
    
def get_schema_in_instance(user:str,password:str,hostname:str,port:str,database:str):
    try:
        user_schemas_list = []
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(LIST_SCHEMAS)
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
            if "SYS" not in item[0] and "NULL" not in item[0] and "SQL" not in item[0] and "IBMPDQ" not in item[0]  and "DEFAULT" not in item[0]:
                    user_schemas_list.append(item[0])
        return user_schemas_list
    except Exception as e:
            print(e) 

def get_tables_under_schema_in_db2woc(user:str,password:str,hostname:str,port:str,database:str,schemaname:str):
    try:
        tables_in_schema = []
        total_estimate_size = 0
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_SCHEMA.format(SCHEMANAME=schemaname))
        rows = conn.fetchall()
        cnxn.close()
        table_cnt = len(rows)
        for item in rows:
            est_size = " "
            est_size = tab_size_by_table_name(user,password,hostname,port,database,schemaname,item[0])
            total_estimate_size += int(est_size)
            tables_in_schema.append([item[0],est_size])
        return table_cnt,total_estimate_size,tables_in_schema
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
                return int(item[0])+int(item[1])+int(item[2])+int(item[3])+int(item[4])+int(item[5])
        except Exception as e:
            print(e) 

def get_tables_under_tablespace_in_db2woc(user:str,password:str,hostname:str,port:str,database:str,tablespace:str):
    try:
        table_names_in_tablespace=[]
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        table_cnt = 0
        total_estimate_size =0
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()  
        cnxn.close()
        with console.status("") as status:
            for item in rows:
                    if  "SYS"  not in item[1]:
                      if  str(item[0]).endswith('t') == False:
                        table_cnt = table_cnt + 1
                        est_size = tab_size_by_table_name(user,password,hostname,port,database,item[1],item[0])
                        total_estimate_size += int(est_size)
                        table_names_in_tablespace.append([item[0],item[1],est_size])
        return total_estimate_size,table_names_in_tablespace,table_cnt
    except Exception as e:
         print(e)   

def get_tables_cnt_under_tablespaces(user:str,password:str,hostname:str,port:str,database:str,tablespace:str):
    try:
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()  
        cnxn.close()
        table_cnt = 0
        with console.status("") as status:
            for item in rows:
                    if  "SYS"  not in item[1]:
                      if  str(item[0]).endswith('t') == False:
                        table_cnt = table_cnt + 1
        return table_cnt
    except Exception as e:
         print(e)


def get_tabname_schemaname_under_tablespace_in_db2woc(user:str,password:str,hostname:str,port:str,database:str,tablespace:str):
    try:
        table_names_in_tablespace=[]
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        table_cnt = 0
        total_estimate_size =0
        conn.execute(LIST_TABLES_IN_TSPACE.format(TABLESPACE=tablespace))
        rows = conn.fetchall()  
        cnxn.close()
        with console.status("") as status:
            for item in rows:
                    if  "SYS"  not in item[1]:
                            table_names_in_tablespace.append([item[0],item[1]])
        return table_names_in_tablespace
    except Exception as e:
         print(e)

def get_tbpsace_name_for_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str):
    try:
        valid_tablespace_list = get_tablespaces_in_block_and_cos(user,password,hostname,port,database)
        tablespace_name = " "
        cnxn= db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(LIST_TBSPACE_BY_TABNAME.format(TABNAME=tablename))
        rows = conn.fetchall()  
        cnxn.close()
        print(rows)
        for item in rows:
             if item[0] in valid_tablespace_list:
                tablespace_name = item[0]
        return tablespace_name
    except Exception as e:
         print(e)
     

# #estimate fucntions

# def create_table_for_sample_table(user:str,password:str,hostname:str,port:str,database:str):
#     try:
#         cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
#         conn = cnxn.cursor()
#         conn.execute(SAMPLE_TABLE)
#         conn.commit()
#         cnxn.close()
#     except Exception as e:
#          print(e)   

# def load_data_to_the_sample_table(user:str,password:str,hostname:str,port:str,database:str):
#      try:
#         cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
#         conn = cnxn.cursor()
#         insert_query = f"INSERT INTO DB2INST1.DB2WHTESTTABLE VALUES (?, ?)"
#         conn.executemany(insert_query, SAMPLE_DATA)
#         conn.commit()
#         cnxn.close()
#      except Exception as e:
#          print(e)
# def drop_sample_table(user:str,password:str,hostname:str,port:str,database:str):
#     try:
#         cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
#         conn = cnxn.cursor()
#         conn.execute(DRP_SAM_TABLE)
#         conn.commit()
#         cnxn.close()
#     except Exception as e:
#          print(e)
# def get_table_move_time_estimate_in_db2woc(user:str,password:str,hostname:str,port:str,database:str):
#      print("creating a sample table and schema for movement")
#      create_table_for_sample_table(user,password,hostname,port,database)
#      print("loading the data in sample table and schema for movment")
#      load_data_to_the_sample_table(user,password,hostname,port,database)
#      print("Testing the movement of the sample table")
#      time_taken_per_kb_in_secs =  admin_move_table_with_move(user,password,hostname,port,database,'DB2INST1','DB2WHTESTTABLE','MOVE')
#      print("cleaning the sample table")
#      drop_sample_table(user,password,hostname,port,database)
#      return time_taken_per_kb_in_secs

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
            
#pyodbc connection fucntions

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
         
def db2wh_pyodbc_connection(user:str,password:str,hostname:str,port:str,database:str,test_con:bool) -> bool:
        import pyodbc
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
                    return True
                 except Exception as e:
                    print(e)
            else:
                  return cnxn
        except Exception as e:
            print(e) 

#logs functions

def generate_uuid():
     id = uuid.uuid4() 
     return str(id).split("-")[0]

def check_if_logs_path_exist_else_create():
    try:
      HOME = check_home_path()
      path = HOME.strip()+"/db2whmigratetocos-logs"
      isExist = os.path.exists(path)
      print(isExist)
      if isExist:
        return path
      else:
        os.makedirs(path,exist_ok=True)
        return path   
    except Exception as e:
            print (e)
           
def create_log_directory_for_migration_run(directory_name:str):
    try:
        directory_path = check_if_logs_path_exist_else_create()
        migration_sub_directory = str(directory_path)+"/"+directory_name.strip()
        os.makedirs(migration_sub_directory)
        return migration_sub_directory
    except Exception as e:
        print(e)
    
def create_file_for_the_table_migration(directory_name:str,file_name:str):
    try:
        run_command('''
                     cd {LOG_DIRECTORY_NAME}
                     touch {FILE_NAME}
                    '''.format(LOG_DIRECTORY_NAME = directory_name,FILE_NAME= file_name))
        isExist= os.path.exists(directory_name+"/"+file_name)
        if isExist:
            return True
    except Exception as e:
         print(e)
    
def unzip_the_adm_script():
   print()
   print("unziping the driver package")  
   try:
      find_whl = run_command("find db2whmigratetocos-0.2-py3-none-any.whl")
      if find_whl.strip() == "db2whmigratetocos-0.2-py3-none-any.whl":
        unzip_out = run_command("unzip ./db2whmigratetocos-0.2-py3-none-any.whl 'db2whmigratetocos/admin_move_table_func.py' -d .")
        print(unzip_out)
      else:
         print(".whl file not found..aborting")
   except Exception as e:
      print(e)  
   
def get_json_format_for_migration_run(schemaname:str,tablename:str,status:str,src_tbspace:str,dest_tbspace:str,migration_job_id:str):
    try:
        migration_meta_data = {
            "migration_job_id":migration_job_id,
            "source_tablespace":src_tbspace,
            "destination_tablespace":dest_tbspace,
            "status":"REQUEST SUBMITTED",
            "table_name":tablename,
            "schema_name":schemaname,
            "phase_logs":[],
        }
        return migration_meta_data
    except Exception as e:
        print(e)

     

#admin_move_table_functions

def find_adm_status_by_tablename(user:str,password:str,hostname:str,port:str,database:str,tablename:str):
    import pyodbc
    try:
        table_phase = " "
        connection_string = get_connection_string(user,password,hostname,port,database)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        for item in rows:
                table_phase = item[0]
                return table_phase
    except Exception as e:
         print(e)

def admin_move_table_with_move(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,option:str):
    try:
        cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC_MOVE.format(SCHEMANAME = schemaname,TABLENAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        print("adm_table")
        print(rows)
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

def find_adm_status_for_struck_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str):
    import pyodbc
    try:
        table_phase = " "
        connection_string = get_connection_string(user,password,hostname,port,database)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_STRUCK_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        cnxn.close()
        for item in rows:
             return item[0] 
    except Exception as e:
         print(e)
          
def find_adm_status_for_a_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str,schemaname:str,src_tbspace:str,dest_tbspace:str,log_file_name:str):
    import pyodbc
    try:
        table_phase = " "
        connection_string = get_connection_string(user,password,hostname,port,database)
        cnxn = pyodbc.connect(connection_string+"LONGDATACOMPAT=1;")
        conn = cnxn.cursor()
        conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=tablename))
        rows = conn.fetchall()
        if len(rows) == 0:
             actual_table_name = find_adm_status_for_struck_table(user,password,hostname,port,database,tablename)
             conn.execute(ADM_MOVE_TABLE_FIND_PHASE.format(TABLENAME=actual_table_name))
             rows = conn.fetchall()
             for item in rows:
                table_phase = item[0]
                print("after finding original name")
                adm_move_table_phase(user,password,hostname,port,database,schemaname,actual_table_name,"CANCEL",src_tbspace,dest_tbspace,log_file_name)
                adm_move_table_phase(user,password,hostname,port,database,schemaname,actual_table_name,"TERM",src_tbspace,dest_tbspace,log_file_name)
                return "INIT"
        else:
            print(rows)
            for item in rows:
                table_phase = item[0]
                return table_phase
    except Exception as e:
         print(e)

def cancel_terminate_admin_move_table(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,phase:str,src_tbspace:str,dest_tbspace:str):
        try:
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME = schemaname,TABLENAME=tablename,OPTION=phase,SOURCE_TBSPACE=src_tbspace,DEST_TBSPACE=dest_tbspace))
            rows = conn.fetchall()
            print(rows)
            print(rows)
        except Exception as e:
             print(e)

def adm_move_table_phase(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,phase:str,src_tbspace:str,dest_tbspace:str,log_file_name):
        try:
            init_start = ""
            cleanup_end = ""
            status = " "
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME = schemaname,TABLENAME=tablename,OPTION=phase,SOURCE_TBSPACE=src_tbspace,DEST_TBSPACE=dest_tbspace))
            rows = conn.fetchall()
            print(rows)
            log_for_the_phase = parse_adm_move_table_by_phase(rows,phase)
            with open(log_file_name,'r+') as file:
                file_data = json.load(file)
                file_data["phase_logs"].append(log_for_the_phase)
                file_data["status"] = log_for_the_phase["STATUS"]
                file.seek(0)
                json.dump(file_data, file, indent = 6) 
            for item in rows:
                if item[0] == 'INIT_START':
                    init_start = item[1]
                if item[0] == 'STATUS':
                    status = item[1]
                if item[0] == 'CLEANUP_END':
                    cleanup_end = item[1]
            return status,init_start,cleanup_end
        except Exception as e:
            x,y = e.args
            if ADM_MOVE_TABLE_PHASE_ERROR_STATE in y:
                    status = find_adm_status_for_a_table(user,password,hostname,port,database,tablename,schemaname,src_tbspace,dest_tbspace,log_file_name)
                    print("Error: " + ADM_MOVE_TABLE_PHASE_ERROR_STATE)
                    log_for_the_phase = {
                          "STATUS":"TERMINATED",
                          "ERROR_CODE":ADM_MOVE_TABLE_PHASE_ERROR_STATE,
                          "MESSAGE":"Retrying the migration of the table"
                    }
                    with open(log_file_name,'r+') as file:
                             file_data = json.load(file)
                             file_data["phase_logs"].append(log_for_the_phase)
                             file_data["status"] = log_for_the_phase["STATUS"]
                             file.seek(0)
                             json.dump(file_data, file, indent = 6)
                    cancel_terminate_admin_move_table(user,password,hostname,port,database,schemaname,tablename,"TERM",src_tbspace,dest_tbspace)
                    cancel_terminate_admin_move_table(user,password,hostname,port,database,schemaname,tablename,"CANCEL",src_tbspace,dest_tbspace)
                    return "TERMINATED",None,None
            # if ADM_MOVE_TABLE_CLEANUP_ERROR_STATE in y:
            #          status = find_adm_status_for_a_table(user,password,hostname,port,database,tablename,schemaname,src_tbspace,dest_tbspace)
            #          print("Error: " + ADM_MOVE_TABLE_CLEANUP_ERROR_STATE)
            #          print("Cleaning up the failed movement and retrying the movement from INIT")
            #          if status != "COMPLETE" or "CLEANUP":
            #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"TERM",src_tbspace,dest_tbspace,log_file_name)
            #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"CANCEL",src_tbspace,dest_tbspace,log_file_name)
            #             adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace)

def adm_move_table_ops_db2woc(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,status:str,src_tbspace:str,dest_tbspace:str,log_directory_name:str):
        init_start = ""
        cleanup_end = ""
        while status !="COMPLETE":
            print("INIT Phase for {TABLENAME}".format(TABLENAME=tablename))
            if status == "INIT":
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace,log_directory_name)
            if status == "COPY":
                print("COPY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"COPY",src_tbspace,dest_tbspace,log_directory_name)
            if status == "TERMINATED":
                status,init_start,cleanup_end = adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace,log_directory_name)
            if status == "REPLAY":
                print("REPLAY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"REPLAY",src_tbspace,dest_tbspace,log_directory_name)
                print("SWAP Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"SWAP",src_tbspace,dest_tbspace,log_directory_name)
                if status == "COMPLETE":
                    init_time = datetime.datetime.strptime(init_start, "%Y-%m-%d-%H.%M.%S.%f") 
                    cleanup_end = datetime.datetime.strptime(cleanup_end, "%Y-%m-%d-%H.%M.%S.%f")
                    time_taken= int((cleanup_end - init_time).total_seconds())
                    print("Movement COMPLETE for {TABLENAME} in ".format(TABLENAME=tablename) + str(time_taken) + " seconds")
            if status == None:
                print("Kindly check if the instance is up and running")
    

def parse_adm_move_table_by_phase(rows:any, phase:str):
    init_start = " "
    init_end = " "
    init_opts = " "
    copy_start = " "
    copy_end = " "
    copy_total_rows = " "
    copy_opts = " "
    swap_start = " "
    swap_end = " "
    cleanup_start = " "
    cleanup_end = " "
    if phase == "INIT":
        for item in rows:
               if item[0] == 'INIT_START':
                    init_start = item[1]
               if item[0] == 'INIT_END':
                    init_end = item[1]
               if item[0] == 'INIT_OPTS':
                    init_opts = item[1]
        init_phase_details = {
                "STATUS":phase,
                "INIT_START": init_start,
                "INIT_END":init_end,
                "INIT_OPTS":init_opts
                }
        return init_phase_details
    if phase == "COPY":
        for item in rows:
               if item[0] == 'COPY_START':
                    copy_start = item[1]
               if item[0] == 'COPY_END':
                    copy_end = item[1]
               if item[0] == 'COPY_TOTAL_ROWS':
                    copy_total_rows = item[1]
               if item[0] == 'COPY_OPTS':
                    copy_opts = item[1]
        copy_phase_details = {
                "STATUS":phase,
                "COPY_START": copy_start,
                "COPY_END":copy_end,
                "COPY_TOTAL_ROWS":copy_total_rows,
                "COPY_OPTS":copy_opts
                }
        return copy_phase_details
    if phase == "REPLAY":
        for item in rows:
                replay_phase_details = {
                     "STATUS":"REPLAY"
                }
        return replay_phase_details
    if phase == "SWAP":
        for item in rows:
               print(item)
               if item[0] == 'SWAP_START':
                    swap_start = item[1]
               if item[0] == 'SWAP_END':
                    swap_end = item[1]
               if item[0] == 'CLEANUP_START':
                    cleanup_start = item[1]
               if item[0] == 'CLEANUP_END':
                    cleanup_end = item[1]
        swap_phase_details = {
                "STATUS":"COMPLETE",
                "SWAP_START": swap_start,
                "SWAP_END":swap_end,
                "CLEANUP_START":cleanup_start,
                "CLEANUP_END":cleanup_end
                }
        return swap_phase_details
                 
             
                         

                     
                          
     
     
