#!/usr/bin/env python3

import setuptools 
 
setuptools.setup(
    name="db2whmigratetocos",
    description="A Db2 warehouse CLI utility to move data to NCOS",
    version="0.1",
    packages=["db2whmigratetocos","db2whmigratetocos.db2_cli_odbc_driver"],
    package_dir={'db2whmigratetocos': 'db2whmigratetocos'},
    package_data={'db2whmigratetocos':['db2_cli_odbc_driver/*']},
    include_package_data=True,
    install_requires=[
        "typer",     
        "setuptools==70.0.0",
        "wheel==0.43.0",
        "pyodbc",
        "pandas"
    ],
    entry_points={
        "console_scripts": [
            "db2whmigratetocos = db2whmigratetocos.main:app"
        ]
    },
)