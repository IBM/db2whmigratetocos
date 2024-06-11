#!/usr/bin/env python3

import setuptools 
 
setuptools.setup(
    name="db2migratetocos",
    description="A Db2 CLI utility to move to COS from Block",
    version="0.1",
    packages=setuptools.find_packages(),
    install_requires=[
        "click",     
        "setuptools==70.0.0",
        "wheel==0.43.0",
        "pyodbc"
    ],
    entry_points={
        "console_scripts": [
            "db2migratetocos = db2migratetocos.db2migratetocos_install_prereq:db2migratetocos_init"
        ]
    },
)