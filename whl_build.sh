#!/bin/sh
python3 -m build
cd dist
scp db2whmigratetocos-0.2-py3-none-any.whl pv_ln@9.46.245.16:/home/pv_ln/db2migrate/ 


