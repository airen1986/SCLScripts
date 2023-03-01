from .queries import get_inventory_sql, get_transportation_sql


def get_inventory_data(conn):
    inventory = {}
    for item, location, moq, lt_mean, lt_std_dev, demand_mean, demand_std_dev, r_val \
            in conn.execute(get_inventory_sql):
        demand = (float(demand_mean), float(demand_std_dev))
        lead_time = (float(lt_mean), float(lt_std_dev))
        row_dict = {'demand': demand, 'lead_time': lead_time,
                    'moq': float(moq), 'r_val': float(r_val)}
        if item not in inventory:
            inventory[item] = {location: row_dict}
        else:
            inventory[item][location] = row_dict
    return inventory


def get_transportation(conn):
    transportation = {}
    for item, location, source in conn.execute(get_transportation_sql):
        if item not in transportation:
            transportation[item] = {location: source}
        else:
            transportation[item][location] = source
    return transportation


def initialize_inventory(inv_data):
    on_hand = {}
    open_order = {}
    in_transit = {}
    wip = {}
    dependent_demand = {}
    for item in inv_data:
        on_hand[item] = {}
        open_order[item] = {}
        in_transit[item] = {}
        wip[item] = {}
        dependent_demand[item] = {}
    for location in inv_data[item]:
            on_hand[item][location] = inv_data[item][location]['moq'] / 2
            open_order[item][location] = 0
            in_transit[item][location] = {}
            wip[item][location] = {}
            dependent_demand[item][location] = []
    return on_hand, open_order, in_transit, wip, dependent_demand
