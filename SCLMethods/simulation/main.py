import simpy
from .sc_class import SupplyChain


def main(conn):
    scc_obj = SupplyChain(conn)
    n_periods = 1000
    generate_demand(scc_obj, n_periods)


def generate_demand(scc_obj, periods):
    for t in range(periods):
        scc_obj.update_demand()
        scc_obj.receive_in_transit(t)
        scc_obj.ship_local_demand()
        scc_obj.ship_dependent_demand(t)
        scc_obj.inventory_control()
        j = scc_obj.process_orders(t)
        while j > 0:
            scc_obj.ship_dependent_demand(t)
            scc_obj.receive_in_transit(t)
            scc_obj.ship_local_demand()
            scc_obj.inventory_control()
            j = scc_obj.process_orders(t)