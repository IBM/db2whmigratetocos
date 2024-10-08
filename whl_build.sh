#!/bin/sh
python3 -m build
rm -rf db2whmigratetocos.egg-info
cd dist
scp -i /Users/pv_ln/.ssh/db2whmigratetocos-dev db2whmigratetocos-0.0.1.tar.gz root@9.30.214.235:/root/db2migrate/ 
#scp -i /Users/pv_ln/.ssh/db2whmigratetocos-dev db2whmigratetocos-0.0.1-py3-none-any.whl root@9.30.87.55:/root/
#scp -i /Users/pv_ln/.ssh/db2whmigratetocos-dev db2whmigratetocos-0.0.1-py3-none-any.whl root@9.30.214.235:/root/db2migrate/ 



