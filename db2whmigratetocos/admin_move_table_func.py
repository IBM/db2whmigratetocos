#!/usr/bin/env python3

import datetime
import json
import pyodbc
import subprocess
import argparse
import logging
from multiprocessing import parent_process

logger = logging.getLogger(__name__)
ADM_MOVE_TABLE_CMD_DB2WOC = "CALL SYSPROC.ADMIN_MOVE_TABLE('{SCHEMANAME}','{TABLENAME}','{DEST_TBSPACE}','{SOURCE_TBSPACE}','{DEST_TBSPACE}','','','','','USE_ADC,COPY_USE_OTA,COPY_USE_RID=0,NO_STATS,ALLOW_READ_ACCESS','{OPTION}')"
ADM_MOVE_TABLE_PHASE_ERROR_STATE = "SQL2104N"
ADM_MOVE_TABLE_CLEANUP_ERROR_STATE = "SQL2105N"
ADM_MOVE_TABLE_FIND_PHASE= "SELECT VALUE FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='STATUS' AND TABNAME='{TABLENAME}'"
ADM_MOVE_TABLE_STRUCK_PHASE="SELECT TABNAME FROM SYSTOOLS.ADMIN_MOVE_TABLE WHERE KEY='TARGET' AND VALUE='{TABLENAME}'"

def check_home_path():
   try:
     HOME = run_command("echo $HOME")
     return HOME
   except Exception as e:
      print(e)

def run_command(command):
    result = subprocess.check_output(command, shell=True,text=True)
    return result

def define_logger_file(log_file_name):
    
     log_file_handler = logging.FileHandler(log_file_name, mode="a", encoding="utf-8")
     logger.setLevel(logging.DEBUG)
     logger.addHandler(log_file_handler)

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
        try:
            connection_string = get_connection_string(user,password,hostname,port,database)
            cnxn = pyodbc.connect(connection_string)
            return cnxn
        except Exception as e:
            print(e) 

#admin_move_table_functions

def find_adm_status_for_struck_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str):
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
          
def find_adm_status_for_a_table(user:str,password:str,hostname:str,port:str,database:str,tablename:str,schemaname:str,src_tbspace:str,dest_tbspace:str,report_file_name:str):
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
                adm_move_table_phase(user,password,hostname,port,database,schemaname,actual_table_name,"CANCEL",src_tbspace,dest_tbspace,report_file_name)
                adm_move_table_phase(user,password,hostname,port,database,schemaname,actual_table_name,"TERM",src_tbspace,dest_tbspace,report_file_name)
                return "INIT"
        else:
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
            logger.info(phase)
            logger.info(rows)
        except Exception as e:
             print(e)

def adm_move_table_phase(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,phase:str,src_tbspace:str,dest_tbspace:str, report_file_name):
        try:
            init_start = ""
            cleanup_end = ""
            status = " "
            cnxn = db2wh_pyodbc_connection(user,password,hostname,port,database,False)
            conn = cnxn.cursor()
            conn.execute(ADM_MOVE_TABLE_CMD_DB2WOC.format(SCHEMANAME = schemaname,TABLENAME=tablename,OPTION=phase,SOURCE_TBSPACE=src_tbspace,DEST_TBSPACE=dest_tbspace))
            rows = conn.fetchall()
            logger.info(phase)
            logger.info(rows)
            log_for_the_phase = parse_adm_move_table_by_phase(rows,phase)
            with open( report_file_name,'r+') as file:
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
                    status = find_adm_status_for_a_table(user,password,hostname,port,database,tablename,schemaname,src_tbspace,dest_tbspace, report_file_name)
                    logger.error("Error: " + ADM_MOVE_TABLE_PHASE_ERROR_STATE)
                    log_for_the_phase = {
                          "STATUS":"TERMINATED",
                          "ERROR_CODE":ADM_MOVE_TABLE_PHASE_ERROR_STATE,
                          "MESSAGE":"Retrying the migration of the table"
                    }
                    with open( report_file_name,'r+') as file:
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
            #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"TERM",src_tbspace,dest_tbspace, report_file_name)
            #             status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"CANCEL",src_tbspace,dest_tbspace, report_file_name)
            #             adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace)


    

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
               if item[0] == 'SWAP_START':
                    swap_start = item[1]
               if item[0] == 'SWAP_END':
                    swap_end = item[1]
               if item[0] == 'CLEANUP_START':
                    cleanup_start = item[1]
               if item[0] == 'CLEANUP_END':
                    cleanup_end = item[1]
               if item[0] == 'INIT_START':
                    init_start = item[1]
        swap_phase_details = {
                "STATUS":"COMPLETE",
                "SWAP_START": swap_start,
                "SWAP_END":swap_end,
                "CLEANUP_START":cleanup_start,
                "CLEANUP_END":cleanup_end,
                }
        return swap_phase_details
    

def adm_move_table_ops_db2woc(user:str,password:str,hostname:str,port:str,database:str,schemaname:str,tablename:str,status:str,src_tbspace:str,dest_tbspace:str, report_file_name:str,log_file_name:str):
        parent  = parent_process()
        print(parent.is_alive)
        print(parent.pid)
        define_logger_file(log_file_name)
        init_start = ""
        cleanup_end = ""
        logger.info
        while status !="COMPLETE":
            logger.info("INIT Phase for {TABLENAME}".format(TABLENAME=tablename))
            if status == "INIT":
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace, report_file_name)
            if status == "COPY":
                logger.info("COPY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"COPY",src_tbspace,dest_tbspace, report_file_name)
            if status == "TERMINATED":
                status,init_start,cleanup_end = adm_move_table_ops_db2woc(user,password,hostname,port,database,schemaname,tablename,"INIT",src_tbspace,dest_tbspace, report_file_name,log_file_name)
            if status == "REPLAY":
                logger.info("REPLAY Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"REPLAY",src_tbspace,dest_tbspace, report_file_name)
                logger.info("SWAP Phase for {TABLENAME}".format(TABLENAME=tablename))
                status,init_start,cleanup_end = adm_move_table_phase(user,password,hostname,port,database,schemaname,tablename,"SWAP",src_tbspace,dest_tbspace, report_file_name)
                if status == "COMPLETE":
                    init_time = datetime.datetime.strptime(init_start, "%Y-%m-%d-%H.%M.%S.%f") 
                    cleanup_end = datetime.datetime.strptime(cleanup_end, "%Y-%m-%d-%H.%M.%S.%f")
                    time_taken= int((cleanup_end - init_time).total_seconds())
                    logger.info("Movement COMPLETE for {TABLENAME} in ".format(TABLENAME=tablename) + str(time_taken) + " seconds")
            if status == None:
               logger.info("Kindly check if the instance is up and running")              
