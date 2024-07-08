import subprocess
import pandas as pd
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
  

def db2wh_pyodbc_connection(user:str,password:str,hostname:str,port:str,database:str):
        try:
            connection_string = get_connection_string(user,password,hostname,port,database)
            cnxn = pyodbc.connect(connection_string)
            return cnxn
        except Exception as e:
            print(e) 

print("Create a new schema called TEST")
cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
conn = cnxn.cursor()
sql = "CREATE SCHEMA DB2WHTEST"
conn.execute(sql)
conn.commit()
conn.close()

cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
conn = cnxn.cursor()
print("creating the table")
sql = "CREATE TABLE DB2WHTEST.DB2WHTESTTABLE (id INT PRIMARY KEY NOT NULL, name VARCHAR(255)) IN USERSPACE1"
conn.execute(sql)
conn.commit()
conn.close()

print("Load the data into the table")
print("INSERTING")
for i in range(1, 1001):
    cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
    conn = cnxn.cursor()
    sql = f"INSERT INTO DB2WHTEST.DB2WHTESTTABLE VALUES ({i}, 'Record {i}')"
    try:
        print(i)
        stmt = conn.execute(sql)
        conn.commit()
    except Exception as e:
        print("Error:", e)
    conn.close()


cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
conn = cnxn.cursor()
print("List the data from the table")
sql = """SELECT * FROM DB2WHTEST.DB2WHTESTTABLE """
conn.execute(sql)
print(conn.fetchall())

# Generate sample data
cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
conn = cnxn.cursor()
print("AMT table")
params = "CALL SYSPROC.ADMIN_MOVE_TABLE('DB2WHTEST','DB2WHTESTTABLE','OBJSTORESPACE1','OBJSTORESPACE1','OBJSTORESPACE1','','','','','ALLOW_READ_ACCESS','MOVE')"
output = conn.execute(params)
print(conn.fetchall())

cnxn = db2wh_pyodbc_connection("db2inst1","bf5aa381b75a1a09","blueark-test-large-db2woc.us-south.dev.db2w.cloud.ibm.com","50001","BLUDB")
conn = cnxn.cursor()
print("REPORT")
ADM_MOVE_REPORT ="CALL SYSPROC.ADMIN_MOVE_TABLE('DB2WHTEST','DB2WHTESTTABLE','OBJSTORESPACE1','OBJSTORESPACE1','OBJSTORESPACE1','','','','','ALLOW_READ_ACCESS','REPORT')"
conn.execute(ADM_MOVE_REPORT)
print(conn.fetchall())

