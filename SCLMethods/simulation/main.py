import simpy
from .sc_class import SupplyChain


def main(conn):
    scc_obj = SupplyChain(conn)
    n_periods = 10000
    generate_demand(scc_obj, n_periods, conn)


def generate_demand(scc_obj, periods, conn):
    for t in range(periods):
        scc_obj.daily_process(t, conn)