
import setuptools 
 
setuptools.setup(
    name="db2migratetocos",
    description="A Db2 CLI utility to move to COS from Block",
    version="0.1",
    packages=setuptools.find_packages(),
    install_requires=[
        "click",
    ],
    entry_points={
        "console_scripts": [
            "db2migratetocos = db2migratetocos.cli:cli"
        ]
    },
)