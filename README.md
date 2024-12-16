# IBM Db2whmigratetocos

IBM® Db2whmigratetocos is a tool used to migrate data between the block storage in the db2 warehouse instances to COS buckets. Once configured, you can trigger migrations through a command line interface installed in a virtual machine. You will also be able to track and monitor the active and completed migrations 

**Note:**
-   The IBM Db2whmigratetocos works with Db2 warehouse V3.0 and above, that has support to the Native COS feature
-   IBM Db2whmigratetocos can be run in the Virtual machine wiht Red Hat Enterprise Linux 9.x.

**Supported migration scenarios include:**

-   Move the tables in all tablespace
-   Move selected tablespace tables
-   Move all tablespace with skipping one or more tablespaces
-   Move the tables in all schemas
-   Move selected schema tables
-   Move all schemas with skipping one or more schemas
-   Move by the generated CSV by skipping schema/tablespace
-   List the tables in tablespaces
-   List the tables in tablespaces and export to CSV
-   List the tables in schema
-   List the tables in and export to CSV


**Operations that are restricted on the source table**

The Db2whmigratetocos Tool, which uses Admin_move_table, records any changes done to the source table. Some operations in the source table may affect the move, resulting in inconsistencies between the source and target tables that stored procedures cannot easily detect.

These operations include:

-   TRUNCATE TABLE (without restrict when delete triggers)

-   IMPORT ... REPLACE INTO ...

-   LOAD TABLE

-   ALTER TABLE

-   REORG (both online and offline)

**Operations that will affect the table move operation**

There are operations that can cause the tool to fail while a move is in progress. These operations include:

-   Dropping of **SYSTOOLSPACE** table space

-   Dropping/Renaming the source table

-   Dropping/Renaming any of the temporary objects created by OTM during the INIT phase (target table, staging table, triggers on source table, protocol table)

-   Altering non-user-configurable values in the protocol table

## Installing  Db2whmigratetocos

The following information describes the installation pre-requisites and contains instructions for installation and setup

**Prerequisite for setting up Db2whmigratetocos**

To use the Db2whmigratetocos CLI tool, you require an RHEL 9.X Virtual machine with mentioned prerequisites in which the wheel file can be installed

Required before installing the tool

-   Requires Python 3.9+ and PIP to install the wheel file
-   Working yum repository connections, Db2whmigratetocos installs dependencies using yum - Unix-ODBC and Python-devel
-   Hostname for the corresponding Db2 warehouse instance
-   Root user privileges for the Virtual machine
-   User_id with SYSADM  and the password for the corresponding Db2 warehouse instance

**Setting up the Db2whmigratetocos:**

The db2whmigratetocos tool is in the form of a wheel package that can be installed using the PIP command.

**Procedure:**

1.Create a python virtual environment and activate it.

```python3 -m venv db2whmigrate-venv```
```source db2whmigrate-venv/bin/activate```

2.Run Setup the db2whmigratetocos using the following command

```db2whmigratetocos setup```

when all of the options in the setup command is OK, then the tool is ready to use.

## Using the Db2whmigratetocos

Db2whmigratetocos tool allow users to list the tables in the warehouse instance, by schema level or tablespace level and move them based on needs of the user. The user has the flexibility to skip the tables by schema level or tablespace level and initiate migration runs. The status of the migration run can be tracked using the status command.

### Commands

**List the tables in tablespaces/schemas with size**

This helps in listing the tables with schema and size in KB by Tablespace or Schema.

It lists upto 75 tables for each tablespace or schema mentioned in the list variable

The entire list can be exported to a csv. The options for the above command is as follows:

- -- scope - tablespace/schema by which the tables needs to listed 
- -- list - all/list of tablespaces/list of schema - the tables under the specified list will be listed 
- -- detail / --no-detail - it prints the information regarding the table size, table schema 
- -- export / --no-export - it exports the printed list to a CSV that can used for the MOVE command
```
db2whmigratetocos list
--scope schema/tablespace --list all/list of schemas or tablespaces
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--dsn\<DSN name\> --export-csv/--no-export-csv –detail/--no-detail
```
**Examples:** 

**List all the tables in  tablespaces** 

```
db2whmigratetocos list
--scope tablespace --list all --no-detail
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
```

**List the tables in the tablespaces in detail** 

```
db2whmigratetocos list
--scope tablespace --list all
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail
```

**List the tables in tablespaces list in detail** 

```
db2whmigratetocos list
--scope tablespace --list TBSPACE1,TBSPACE2,TBSPACE3
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail
```

**List the tables in all tablespaces in detail and export to CSV** 

```
db2whmigratetocos list
--scope tablespace --list all
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail --export-csv
```

**List the tables in tablespace list in detail and export to CSV** 

```
db2whmigratetocos list
--scope tablespace --list TBSPACE1,TBSPACE2
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail --export-csv
```

**List all the schemas** 

```
db2whmigratetocos list
--scope schema --list all
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
```

**List the tables in the schema in detail**

```
db2whmigratetocos list
--scope schema --list all
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail
```

**List the tables in schema list in detail**

```
db2whmigratetocos list
--scope tablespace --list SCHEMA1,SCHEMA2
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail
```

**List the tables in all schemas in detail and export to CSV**
```
db2whmigratetocos list
--scope schema --list all
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail --export-csv
```

**List the tables in schema list in detail and export to CSV**
```
db2whmigratetocos list
--scope schema --list SCHEMA1,SCHEMA2
--user-id user_id --password password --hostname test.db2w.cloud.ibm.com
--detail --export-csv
```

**Move the tablespaces to COS from Block**

This command initiates the move of the list of tables to COS - OBJSTORESPACE.The move can be done at the tablespace or schema level, using ALL or the specified list of tablespaces (or) schemas.
Each run of the move command will generate a directory containing the logs and report metrics. Check the movement status with the status command - db2whmigratetocos status -help.

- --scope - tablespace/schema - move tables by tablespace/schema
- --list - all/list of tablespaces/list of schema - the tables under the specified list will be listed
- --dest_tablespace - OBJSTORESPACE1 - The destination tablespace in COS
- --skip-schema - Skip a list of schema in the list - only used when the scope is schema
- --skip-tbspace - Skip a list of tablespaces in the list - only used when the scope is tablespace
- --csv-input - Give the generated CSV as input for the move command
- --index-tbspace - The tablespace in block where indexes will be stored
- --dsn - The DSN name if it is already configured
- --copy-opts - To pass the copy options required for the tool
    -  COPY_USE_OTA - Required  parameter should be provided
    -  NO_STATS - If you don't want runstats to be run on the Moved table, If this option is not provided, runstats is collected as part of the Admin Move Table operation.- Optional
    -  USE_ADC - Uses Automatic dictinary creation to create the dictionary/skipping uses sampling method to create dictionary - Optional
    -  ALLOW_READ_ACCESS - For Offline Admin Move Table, not providing this option will enable online data migration - Optional.
- --runstats - To trigger external runstats after the table is moved
- --log-directory-path - Pass the log directory base path to store the log files

Note: The move command needs to be run in nohup mode to make sure the process does not stop if the client connection in the VM gets disconnected

Command:
```
nohup db2whmigratetocos move --scope tablespace --list DB_TS1
\--dest-tbspace OBJSTORESPACE1 --index-tbspace USERSPACE1 --copy-opts
\--log-directory-path \<path\> --user-id \<user_id\> --password \<password\>
\--hostname \<\>hostnamE\> --use-adc
\> migration_run.out 2\>&1 &
```
**To Move a single table,**
```
db2whmigratetocos move   
 --scope table --schema-name \<schema-name\>  
 --table-name \<table-name\>  
 --dest-tbspace \<destination_tbspace\>  
 --copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
 --dsn \<DSN-NAME\> --logs-path \<logs-path\>  
 --user-id \<user_id\> --password \<password\>  
 --hostname \<hostname\>
```
Examples:

**Move tables in all tablespaces**
```
db2whmigratetocos move
--scope tablespace --list all
--dest-tbspace OBJSTORESPACE1 
--log-directory-path \<path\>
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
\> migration_run.out 2\>&1 &
```

**Move tables in tablespace list**
```
db2whmigratetocos move
--scope tablespace --list TBSPACE1,TBSPACE2
--dest-tbspace OBJSTORESPACE1 
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>

\> migration_run.out 2\>&1 &
```

**Move tables in all other tablespaces except skip list**
```
db2whmigratetocos move
--scope tablespace --list all
--dest-tbspace OBJSTORESPACE1
--skip-tbspace TBSPACE1
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```

**Move tables in all schemas**
```
db2whmigratetocos move
--scope schema --list all
--dest-tbspace OBJSTORESPACE1
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```
**Move tables in schema list**
```
db2whmigratetocos move
--scope schema --list SCHEMA1,SCHEMA2
--dest-tbspace OBJSTORESPACE1
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```
**Move tables in all schemas except skip list**
```
db2whmigratetocos move
--scope schema --list all
--dest-tbspace OBJSTORESPACE1
--skip-schema SCHEMA1,SCHEMA2
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```
**Move tables by schema-skip schema with CSV as input**
```
db2whmigratetocos move
--scope schema --csv-input \<csv filename\>
--dest-tbspace OBJSTORESPACE1
--skip-schema SCHEMA1,SCHEMA2
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```
**Move tables by Tablespace-skip tablespace with CSV as input**
```
db2whmigratetocos move
--scope tablespace --csv-input \<csv filename\>
--dest-tbspace OBJSTORESPACE1
--skip-tbspace tablespace1,tablespace2
--copy-opts COPY_USE_OTA,NO_STATS,USE_ADC,ALLOW_READ_ACCESS
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
--dsn <dsn-name>
\> migration_run.out 2\>&1 &
```

**Status of the migration jobs**

The command is used to fetch the details about the tables in block and cos.It can give the details and the status of a migration runs as well. The scope paramter value -  migration runs will list the details about the table, while the tables - will list us the number of tables in block storage and COS respectively.

**Command**
````
db2whmigratetocos status
--scope migration-runs/tables
--active-runs
--log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
````
**Examples:**

**To know about the status of the previous migration-runs**

db2whmigratetocos status
--scope migration-runs –no-active-runs --log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

**To know about the status of the active migration-runs**
```
db2whmigratetocos status
--scope migration-runs –-active-runs --log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
```
**To know about the status of the tables in block and COS**
```
db2whmigratetocos status
--scope tables. –-active-runs --log-directory-path \<path\>
--user-id \<user-id\> --password \<password\> --hostname \<host-name\>
```

