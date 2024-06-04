import click
from .lib import connect_to_db

@click.group()
def cli():
    pass

@click.command()
def info():

    print(f'Db2 Migrate to COS Utility')
    connect_to_db("BLUDB")


cli.add_command(info)