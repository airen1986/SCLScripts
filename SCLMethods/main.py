from .sql_connector import SqlConnect
from .simulation.main import main as run_simulation


def main(db_name, method_name):
    with SqlConnect(db_name) as conn:
        if method_name == "simulation":
            run_simulation(conn)
