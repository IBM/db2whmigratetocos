import click
from .db2migratetocos_install_prereq import db2migratetocos_init

@click.group()
def cli():
    pass

@cli.command("setuptool")
def setuptool():
    print(f'Db2 Migrate to COS Utility')
    db2migratetocos_init()
    
