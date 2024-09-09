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
    cnxn = db2wh_pyodbc_connection()
    conn = cnxn.cursor()
    print("INIT")
    conn.execute("SELECT SUM(COL_OBJECT_P_SIZE),SUM(DATA_OBJECT_P_SIZE), SUM(INDEX_OBJECT_P_SIZE), SUM(LONG_OBJECT_P_SIZE), SUM(LOB_OBJECT_P_SIZE), SUM(XML_OBJECT_P_SIZE) FROM SYSIBMADM.ADMINTABINFO WHERE TABSCHEMA='BDDATA3' AND TABNAME='\"CATALOG_RETURNSAOyNsTt\"'  GROUP BY TABSCHEMA, TABNAME")
    rows = conn.fetchall()
    cnxn.close()
    print(rows)
    for item in rows:
        print(item)
except Exception as e:
    print(e)

