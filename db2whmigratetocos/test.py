import subprocess
import pyodbc
# Create a new table called test_table in tablespace T1


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
    con_str = Driver+Database+Hostname+Port+Uid+Pass+Security+Protocol+"LONGDATACOMPAT=1;LOBMAXCOLUMNSIZE=10485875;"
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
  

def db2wh_pyodbc_connection(user:str,password:str,hostname:str,port:str,database:str):
        try:
            connection_string = get_connection_string(user,password,hostname,port,database)
            cnxn = pyodbc.connect(connection_string)
            return cnxn
        except Exception as e:
            print(e) 

try:
    cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
    conn = cnxn.cursor()
    print("INIT")
    ADM_MOVE_REPORT = "Select distinct(name), ColType, Length from Sysibm.syscolumns where tbname = 'ADMIN_MOVE_TABLE'"
    conn.execute(ADM_MOVE_REPORT)
    print(conn.fetchall())
except Exception as e:
    print(e)

