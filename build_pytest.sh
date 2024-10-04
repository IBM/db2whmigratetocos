!/bin/sh
python3 -m build
rm -rf db2whmigratetocos.egg-info
cd dist
scp -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev db2whmigratetocos-0.0.1.tar.gz root@9.30.214.235:/root/db2migrate/ 
ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "rm -rf /root/db2migrate/db2whmigratetocos-0.0.1 && rm -rf /root/db2migrate/db2whmigratetocos-0.0.1.tar && echo "removed the old build""
ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "tar xvfz /root/db2migrate/db2whmigratetocos-0.0.1.tar.gz -C /root/db2migrate/ && echo "unzip done""
pwd
cd ../tests
ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1 && pwd"
scp -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev  content.json  root@9.30.214.235:/root/db2migrate/db2whmigratetocos-0.0.1/tests
#scp -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev  content_copy.json  root@9.30.214.235:/root/db2migrate/db2whmigratetocos-0.0.1/tests
#ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_connection_string.py"
#ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_schema_in_instance.py"
#ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_tablespaces_in_block_and_cos.py"
#ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_tables_under_tablespace_in_db2woc.py"
#ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_tablespace_name_for_table.py"
ssh -i /Users/ruhisehgal/.ssh/db2whmigratetocos-dev root@9.30.214.235 "cd /root/db2migrate/db2whmigratetocos-0.0.1  && python -m unittest tests/test_tables_under_schema_in_db2woc.py"