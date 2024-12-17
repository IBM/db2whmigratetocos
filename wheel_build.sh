
#Copyright IBM Corp. 2024  All Rights Reserved.
#Licensed Materials - Property of IBM
#shell scipt to build the wheel file
!/bin/sh
python3 -m build
rm -rf db2whmigratetocos.egg-info
cd dist 
scp  db2whmigratetocos-0.0.1-py3-none-any.whl  root@9.46.250.214:/root
# #scp -i /Users/pv_ln/.ssh/db2whmigratetocos-dev db2whmigratetocos-0.0.1-py3-none-any.whl root@9.30.214.235:/root/db2migrate/ 



