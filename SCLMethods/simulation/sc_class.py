from random import normalvariate
from copy import deepcopy
from .queries import planning_insert_query
from .get_data import get_inventory_data, get_transportation, initialize_inventory


class SupplyChain:
    def __init__(self, conn):
        self.inv_data, self.zero_dict = get_inventory_data(conn)
        self.tp_data = get_transportation(conn)
        self.on_hand_inv, self.open_order, self.in_transit,\
            self.wip, self.dependent_demand = initialize_inventory(self.inv_data)
        self.backorders = deepcopy(self.zero_dict)
        self.local_demand = {}
        self.scheduled_receipts = {}
        self.opening_inventory = {}
        self.shipped_qty = {}
        self.received_qty = {}
        self.forecast_qty = {}
        self.ordered_qty = {}
        conn.execute("DELETE FROM planning_data")
        conn.intermediate_commit()

    def update_demand(self):
        for item in self.inv_data:
            self.local_demand[item] = {}
            for location in self.inv_data[item]:
                demand_mean, demand_std_dev = self.inv_data[item][location]['demand']
                if demand_mean > 0:
                    demand_qty = normalvariate(demand_mean, demand_std_dev)/7
                    self.local_demand[item][location] = demand_qty
                else:
                    self.local_demand[item][location] = 0

    def ship_backorders(self):
        for item in self.backorders:
            for location in self.backorders[item]:
                if self.backorders[item][location] == 0:
                    continue
                ship_qty = min(self.on_hand_inv[item][location], self.backorders[item][location])
                self.on_hand_inv[item][location] -= ship_qty
                self.backorders[item][location] -= ship_qty
                self.shipped_qty[item][location] += ship_qty

    def ship_local_demand(self):
        for item in self.local_demand:
            for location in self.local_demand[item]:
                demand_qty = self.local_demand[item][location]
                ship_qty = min(self.on_hand_inv[item][location], demand_qty)
                if ship_qty <= 0:
                    self.backorders[item][location] += demand_qty
                    continue
                self.on_hand_inv[item][location] -= ship_qty
                self.shipped_qty[item][location] += ship_qty
                if ship_qty < demand_qty:
                    remaining_qty = demand_qty - ship_qty
                    self.backorders[item][location] += remaining_qty

    def inventory_control(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                receipt_qty = sum(self.in_transit[item][location][t]
                                  for t in self.in_transit.get(item, {}).get(location, {}))
                source_location = self.tp_data.get(item, {}).get(location, None)
                if source_location:
                    receipt_qty += sum(row[1] if row[0] == location else 0 for row in
                                       self.dependent_demand.get(item, {}).get(source_location, {}))
                dependent_demand = sum(row[1] for row in
                                       self.dependent_demand.get(item, {}).get(location, {}))
                projected_inv = self.on_hand_inv[item][location] + receipt_qty - \
                                self.backorders[item][location] - dependent_demand
                if projected_inv <= self.inv_data[item][location]['r_val']:
                    revised_r_val = 2 * self.inv_data[item][location]['r_val'] - projected_inv
                    order_qty = max(revised_r_val, self.inv_data[item][location]['moq'])
                    self.open_order[item][location] = order_qty
                    self.ordered_qty[item][location] += order_qty

    def process_orders(self, t):
        i = 0
        for item in self.open_order:
            for location in self.open_order[item]:
                qty = self.open_order[item][location]
                if qty == 0:
                    continue
                i += 1
                if location not in self.tp_data.get(item, {}):
                    self.start_production(item, location, qty, t)
                else:
                    source = self.tp_data[item][location]
                    self.dependent_demand[item][source].append((location, qty))
                self.open_order[item][location] = 0
        return i

    def ship_dependent_demand(self, t):
        for item in self.dependent_demand:
            for location in self.dependent_demand[item]:
                on_hand_qty = self.on_hand_inv[item][location]
                if on_hand_qty == 0 or len(self.dependent_demand[item][location]) == 0:
                    continue
                for idx, row in enumerate(self.dependent_demand[item][location]):
                    destination_location = row[0]
                    order_qty = row[1]
                    if order_qty == 0:
                        continue
                    ship_qty = min(order_qty, on_hand_qty)
                    self.start_transit(item, location, destination_location, ship_qty, t)
                    on_hand_qty = self.on_hand_inv[item][location]
                    self.dependent_demand[item][location][idx] = (destination_location, order_qty - ship_qty)
                    if on_hand_qty == 0:
                        break
                while len(self.dependent_demand[item][location]) > 0 \
                        and self.dependent_demand[item][location][0][1] == 0:
                    self.dependent_demand[item][location].pop(0)

    def start_transit(self, item, location, destination, qty, t):
        if qty == 0:
            return
        mu, sigma = self.inv_data[item][destination]['lead_time']
        lt = round(normalvariate(mu, sigma), 0)
        if lt < 0:
            lt = 0
        self.on_hand_inv[item][location] -= qty
        if t+lt not in self.in_transit[item][destination]:
            self.in_transit[item][destination][t + lt] = 0
        self.in_transit[item][destination][t + lt] += qty

    def start_production(self, item, location, qty, t):
        if qty == 0:
            return
        mu, sigma = self.inv_data[item][location]['lead_time']
        lt = round(normalvariate(mu, sigma), 0)
        if lt < 0:
            lt = 0
        if t+lt not in self.in_transit[item][location]:
            self.in_transit[item][location][t+lt] = 0
        self.in_transit[item][location][t+lt] += qty

    def receive_in_transit(self, t):
        for item in self.in_transit:
            for location in self.in_transit[item]:
                if t in self.in_transit[item][location]:
                    qty = self.in_transit[item][location][t]
                    self.received_qty[item][location] += qty
                    self.on_hand_inv[item][location] += qty
                    self.in_transit[item][location][t] = 0

    def initialize_opening_inv(self):
        self.opening_inventory = deepcopy(self.on_hand_inv)
        self.shipped_qty = deepcopy(self.zero_dict)
        self.received_qty = deepcopy(self.zero_dict)
        self.ordered_qty = deepcopy(self.zero_dict)

    def daily_process(self, t, conn):
        self.initialize_opening_inv()
        self.update_demand()
        self.receive_in_transit(t)
        self.ship_backorders()
        self.ship_local_demand()
        self.ship_dependent_demand(t)
        self.inventory_control()
        j = self.process_orders(t)
        while j > 0:
            self.ship_dependent_demand(t)
            self.receive_in_transit(t)
            self.ship_backorders()
            self.inventory_control()
            j = self.process_orders(t)
        self.write_data(t, conn)

    def write_data(self, t, conn):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                opening_qty = self.opening_inventory.get(item, {}).get(location, 0)
                source_location = self.tp_data.get(item, {}).get(location, None)
                forecast_qty = self.local_demand.get(item, {}).get(location, 0)
                shipped_qty = self.shipped_qty.get(item, {}).get(location, 0)
                received_qty = self.received_qty.get(item, {}).get(location, 0)
                order_qty = self.ordered_qty.get(item, {}).get(location, 0)
                backorder_qty = self.backorders.get(item, {}).get(location, 0)
                in_transit_dict = self.in_transit.get(item, {}).get(location, {})
                in_transit_qty = sum(in_transit_dict[t] for t in in_transit_dict)
                dependent_list = self.dependent_demand.get(item, {}).get(location, [])
                backorder_qty += sum(i[1] for i in dependent_list)
                closing_qty = self.on_hand_inv.get(item, {}).get(location, 0)
                data_row = (item, location, source_location, t, forecast_qty, order_qty,
                            in_transit_qty, received_qty, opening_qty, closing_qty,
                            shipped_qty, backorder_qty)
                conn.execute(planning_insert_query, data_row)






