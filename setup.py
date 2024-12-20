#!/usr/bin/env python3

"""
Copyright IBM Corp. 2024  All Rights Reserved.
Licensed Materials - Property of IBM
This is the setup file for the python wheel file build

"""

import setuptools


VERSION_NAME = "0.0.1"
PACKAGE_NAME = "db2whmigratetocos"+VERSION_NAME+"-py3-none-any.whl"

setuptools.setup(
    name="db2whmigratetocos",
    description="A Db2 warehouse CLI utility to move data to NCOS",
    version=VERSION_NAME,
    author="Lakshmi Narayanan PV Ruhi Sehgal",
    packages=["db2whmigratetocos", "db2whmigratetocos.db2_cli_odbc_driver"],
    package_dir={'db2whmigratetocos': 'db2whmigratetocos'},
    package_data={'db2whmigratetocos': ['db2_cli_odbc_driver/*']},
    include_package_data=True,
    install_requires=[
        "typer",
        "setuptools",
        "wheel",
        "pyodbc",
        "pandas",
    ],
    entry_points={
        "console_scripts": [
            "db2whmigratetocos = db2whmigratetocos.main:app"
        ]
    },
)
