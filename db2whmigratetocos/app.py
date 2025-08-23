from db2whmigratetocos.main import app
from db2whmigratetocos.parallel_executor import app as parallel_app

app.add_typer(parallel_app)
