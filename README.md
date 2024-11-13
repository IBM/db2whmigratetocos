# IBM Db2whmigratetocos

IBM® Db2whmigratetocos is a tool used to migrate data between the block storage in the db2 warehouse instances to COS buckets. Once configured, you can trigger migrations through a command line interface

**Note:**

-   The IBM Db2whmigratetocos works with Db2 warehouse V3.0 and above, that has support to the Native COS feature

-   IBM Db2whmigratetocos is available on the Red Hat Enterprise Linux 9.x.

**Supported migration scenarios include:**

-   Move the tables in all tablespace

-   Move all tablespace with skipping one or more tablespaces

-   Move the tables in all schemas

-   Move all schemas with skipping one or more schemas

-   List the tables in tablespaces

-   list the tables in tablespaces and export to CSV

-   List the tables in schema

-   List the tables in and export to CSV

-   Move by the generated CSV by skipping schema/tablespace

## Prerequisites and restrictions for Db2whmigratetocos

This provides a list of the prerequisites and restrictions for Db2whmigratetocos

**Prerequisites:**

Db2whmigratetocos have few prerequisites, i.e:

-   Requires Python 3.9+ and PIP to install the wheel file

-   Working yum repository connections, Db2whmigratetocos installs dependencies using yum

**Restrictions and limitations**

The Db2whmigratetocos has some restrictions and limitations.

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

Installing and uninstalling Db2whmigratetocos

The following information describes the installation pre-requisites and contains instructions for installation and setup

**Prerequisite for setting up Db2whmigratetocos**

To use the Db2whmigratetocos CLI tool, you require an RHEL 9.X Virtual machine with mentioned prerequisites in which the wheel file can be installed

Required before installing the tool

-   Python3.9+ and PIP being available in the Virtual Machine

-   Hostname for the corresponding Db2 warehouse instance

-   Root user privileges for the Virtual machine

-   db2inst1 and the instance password for the corresponding Db2 warehouse instance

**Setting up the Db2whmigratetocos:**

The db2whmigratetocos tool is in the form of a wheel package that can be installed using the PIP command.

**Procedure:**

1.Create a python virtual environment and activate it.

python3 -m venv db2whmigrate-venv

source db2whmigrate-venv/bin/activate

2\. Run pre_install.sh to install the dependencies

./pre_install.sh

3\. If the DSN is setup, the script will indentify, that can used for the connection to the warehouse instance. The user needs to be changed as a instance user, and the tool can be used directly

Else,

Run Setup the db2whmigratetocos using the following command

db2whmigratetocos setup

when all of the options in the setup command is OK, then the tool is ready to use.

Using the Db2whmigratetocos

Db2whmigratetocos tool allow users to list the tables in the warehouse instance, by schema level or tablespace level and move them based on needs of the user. The user has the flexibility to skip the tables by schema level or tablespace level and initiate migration runs. The status of the migration run can be tracked using the status command.

# Procedure:

**List the tables in tablespaces/schemas with size**

This helps in listing the tables with schema and size in KB by Tablespace or Schema.

It lists upto 75 tables for each tablespace or schema mentioned in the list variable

The entire list can be exported to a csv. The options for the above command is as follows:

\-- scope - tablespace/schema by which the tables needs to listed\\n

\-- list - all/list of tablespaces/list of schema - the tables under the specified list will be listed\\n

\-- detail / --no-detail - it prints the information regarding the table size, table schema \\n

\-- export / --no-export - it exports the printed list to a CSV that can used for the MOVE command

db2whmigratetocos list

\--scope schema/tablespace --list all/list of schemas or tablespaces

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--dsn\<DSN name\> --export-csv/--no-export-csv –detail/--no-detail

Examples:

List all the tablespaces

db2whmigratetocos list

\--scope tablespace --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

List the tables in the tablespaces in detail

db2whmigratetocos list

\--scope tablespace --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail

List the tables in a list of tablespaces in detail

db2whmigratetocos list

\--scope tablespace --list TBSPACE1,TBSPACE2,TBSPACE3

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail

List the tables in all tablespaces in detail and export to CSV

db2whmigratetocos list

\--scope tablespace --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail --export-csv

List the tables in list tablespaces in detail and export to CSV

db2whmigratetocos list

\--scope tablespace --list TBSPACE1,TBSPACE2

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail --export-csv

List all the schemas

db2whmigratetocos list

\--scope schema --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

List the tables in the schema in detail

db2whmigratetocos list

\--scope schema --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail

List the tables in a list of schema in detail

db2whmigratetocos list

\--scope tablespace --list SCHEMA1,SCHEMA2

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail

List the tables in all schemas in detail and export to CSV

db2whmigratetocos list

\--scope schema --list all

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail --export-csv

List the tables in list schema in detail and export to CSV

db2whmigratetocos list

\--scope schema --list SCHEMA1,SCHEMA2

\--user-id user_id --password password --hostname test.db2w.cloud.ibm.com

\--detail --export-csv

**Move the tablespaces to COS from Block**

This command initiates the move of the list of tables to COS - OBJSTORESPACE.The move can be done at the tablespace or schema level, using ALL or the specified list of tablespaces (or) schemas.

Each run of the move command will generate a directory containing the logs and report metrics. Check the movement status with the status command - db2whmigratetocos status -help.

\--scope - tablespace/schema - move tables by tablespace/schema\\n

\--list - all/list of tablespaces/list of schema - the tables under the specified list will be listed\\n

\--dest_tablespace - OBJSTORESPACE1 - The destination tablespace in COS\\n

\--skip-schema - Skip a list of schema in the list - only used when the scope is schema\\n

\--skip-tbspace - Skip a list of tablespaces in the list - only used when the scope is tablespace\\n

\--csv-input - Give the generated CSV as input for the move command

\--index-tbspace - The tablespace in block where the indexes are stored

\--dsn - The DSN name if it is already configured

\--use-adc - Uses Sampling method to create dictionary by default - give --use-adc to use ADC for dictionary creation

\--log-directory-path - Pass the log directory base path to store the log files

Note: The move command needs to be run in nohup mode to make sure the process does not stop if the client gets disconnected

Command:

nohup db2whmigratetocos move --scope tablespace --list DB_TS1

\--dest-tbspace OBJSTORESPACE1 --index-tbspace USERSPACE1

\--log-directory-path \<path\> --user-id \<user_id\> --password \<password\>

\--hostname \<\>hostnamE\> --use-adc

\> migration_run.out 2\>&1 &

**To Move a single table,**

db2whmigratetocos move   
 --scope table --schema-name \<schema-name\>  
 --table-name \<table-name\>  
 --dest-tbspace \<destination_tbspace\>  
 --index-tbspace \<tablespace in block\>  
 --dsn \<DSN-NAME\> --logs-path \<logs-path\>  
 --user-id \<user_id\> --password \<password\>  
 --hostname \<hostname\>

Examples:

Move by tables in all tablespaces

db2whmigratetocos move

\--scope tablespace --list all

\--dest-tbspace OBJSTORESPACE1 --index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move by tables in list of tablespaces

db2whmigratetocos move

\--scope tablespace --list TBSPACE1,TBSPACE2

\--dest-tbspace OBJSTORESPACE1 --index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move by tables in list of tablespaces with skip tablespaces

db2whmigratetocos move

\--scope tablespace --list all

\--dest-tbspace OBJSTORESPACE1

\--skip-tbspace TBSPACE1

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move by tables in all schemas

db2whmigratetocos move

\--scope schema --list all

\--dest-tbspace OBJSTORESPACE1

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move by tables in list of schema

db2whmigratetocos move

\--scope schema --list SCHEMA1,SCHEMA2

\--dest-tbspace OBJSTORESPACE1

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move by tables in list of schema with skip schema

db2whmigratetocos move

\--scope schema --list all

\--dest-tbspace OBJSTORESPACE1

\--skip-schema SCHEMA1,SCHEMA2

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move tables by schema-skip schema with CSV as input

db2whmigratetocos move

\--scope schema --csv-input \<csv filename\>

\--dest-tbspace OBJSTORESPACE1

\--skip-schema SCHEMA1,SCHEMA2

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

Move tables by Tablespace-skip tablespace with CSV as input

db2whmigratetocos move

\--scope tablespace --csv-input \<csv filename\>

\--dest-tbspace OBJSTORESPACE1

\--skip-tbspace tablespace1,tablespace2

\--index-tbspace USERSPACE1

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

\> migration_run.out 2\>&1 &

**Status and the metrics of the migration jobs**

The command is used to fetch the details about the tables in block and cos.It can give the details and the status of a migration runs as well.

Command

db2whmigratetocos status\\n

\--scope migration-runs/tables\\n

\--active-runs\\n

\--log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

Examples:

To know about the status of the previous migration-runs

db2whmigratetocos status

\--scope migration-runs –no-active-runs --log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

To know about the status of the active migration-runs

db2whmigratetocos status

\--scope migration-runs –-active-runs --log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>

To know about the status of the tables in block and COS

db2whmigratetocos status

\--scope tables. –-active-runs --log-directory-path \<path\>

\--user-id \<user-id\> --password \<password\> --hostname \<host-name\>


