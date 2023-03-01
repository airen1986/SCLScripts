from random import normalvariate
from .get_data import get_inventory_data, get_transportation, initialize_inventory


class SupplyChain:
    def __init__(self, conn):
        self.inv_data = get_inventory_data(conn)
        self.tp_data = get_transportation(conn)
        self.on_hand_inv, self.open_order, self.in_transit,\
            self.wip, self.dependent_demand = initialize_inventory(self.inv_data)
        self.backorders = {}
        self.local_demand = {}
        self.scheduled_receipts = {}

    def update_demand(self):
        for item in self.inv_data:
            self.local_demand[item] = {}
            for location in self.inv_data[item]:
                demand_mean, demand_std_dev = self.inv_data[item][location]['demand']
                if demand_mean > 0:
                    demand_qty = normalvariate(demand_mean, demand_std_dev)/7
                    self.local_demand[item][location] = demand_qty

    def ship_local_demand(self):
        # ship backorders first
        for item in self.backorders:
            for location in self.backorders[item]:
                ship_qty = min(self.on_hand_inv[item][location], self.backorders[item][location])
                self.on_hand_inv[item][location] -= ship_qty
                self.backorders[item][location] -= ship_qty
        for item in self.local_demand:
            for location in self.local_demand[item]:
                if self.local_demand[item][location] <= 0:
                    continue
                remaining_qty = self.on_hand_inv[item][location] - self.local_demand[item][location]
                if remaining_qty >= 0:
                    # ship local demand
                    self.on_hand_inv[item][location] = remaining_qty
                else:
                    # backorder local demand
                    self.on_hand_inv[item][location] = 0
                    remaining_qty = abs(remaining_qty)
                    if item not in self.backorders:
                        self.backorders[item] = {location: remaining_qty}
                    elif location not in self.backorders[item]:
                        self.backorders[item][location] = remaining_qty
                    else:
                        self.backorders[item][location] += remaining_qty
                self.local_demand[item][location] = 0

    def inventory_control(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                print(item, location, self.on_hand_inv[item][location] )
                receipt_qty = sum(self.in_transit[item][location][t]
                                  for t in self.in_transit.get(item, {}).get(location, {}))
                dependent_demand = sum(row[1] for row in
                                       self.dependent_demand.get(item, {}).get(location, {}))
                projected_inv = self.on_hand_inv[item][location] + receipt_qty - \
                                self.backorders.get(item, {}).get(location, 0) - dependent_demand
                if projected_inv <= self.inv_data[item][location]['r_val']:
                    revised_r_val = 2 * self.inv_data[item][location]['r_val'] - projected_inv
                    order_qty = max(revised_r_val, self.inv_data[item][location]['moq'])
                    self.open_order[item][location] = order_qty

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
                if on_hand_qty == 0:
                    continue
                for destination_location, order_qty in self.dependent_demand[item][location]:
                    ship_qty = min(order_qty, on_hand_qty)
                    self.start_transit(item, location, destination_location, ship_qty, t)
                    on_hand_qty = on_hand_qty - ship_qty
                    if on_hand_qty == 0:
                        break

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
                    self.on_hand_inv[item][location] += qty
                    self.in_transit[item][location][t] = 0



