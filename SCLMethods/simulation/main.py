from .queries import create_output_tables
from .sc_class import SupplyChain


def main(conn):
    conn.run_block(create_output_tables)
    scc_obj = SupplyChain(conn)
    n_periods = 2800
    generate_demand(scc_obj, n_periods, conn)


def generate_demand(scc_obj, periods, conn):
    for t in range(periods):
        scc_obj.daily_process(t, conn)