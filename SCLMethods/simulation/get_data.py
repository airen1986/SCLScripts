from .queries import *


def populate_active_combinations(conn):
    conn.execute(delete_active_combinations)
    conn.execute(insert_forecast_query)
    ct = 1
    while ct > 0:
        conn.execute(tp_insert_query)
        ct = conn.execute("select changes()").fetchone()[0]
        if ct == 0:
            conn.execute(bom_insert_query)
            ct = conn.execute("select changes()").fetchone()[0]


def get_combinations(conn):
    populate_active_combinations(conn)
    item_locations = {}
    for row in conn.execute(get_combinations_sql):
        item = row[0]
        location = row[1]
        data_dict = {
            'source': row[2], 'demand': (float(row[3]), float(row[4])),
            'moq': float(row[5]), 'on_hand_qty': float(row[5])/2, 'r_val': float(row[6]),
            'lead_time': (float(row[7]), float(row[8])), 'opening_inv': 0,
            'backorder_qty': 0, 'forecast_qty': 0, 'shipped_qty': 0, 'ordered_qty': 0,
            'transit_qty': {}, 'wip_qty': {}, 'dependent_demand': [], 'receipt_qty': 0,
            'open_orders': 0
        }
        if item not in item_locations:
            item_locations[item] = {location: data_dict}
        else:
            item_locations[item][location] = data_dict
    return item_locations






