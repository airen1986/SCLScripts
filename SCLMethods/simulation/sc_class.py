import random
from random import normalvariate
from math import sqrt
from .queries import planning_insert_query, delete_planning_data
from .get_data import get_combinations, get_bom


class SupplyChain:
    def __init__(self, conn):
        self.inv_data = get_combinations(conn)
        self.bom_relation = get_bom(conn)
        self.open_orders = {}
        #random.seed(143)
        conn.execute(delete_planning_data)
        conn.intermediate_commit()

    def ship_backorders(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                backorder_qty = data['backorder_qty']
                on_hand_qty = data['on_hand_qty']
                ship_qty = min(on_hand_qty, backorder_qty)
                data['on_hand_qty'] -= ship_qty
                data['backorder_qty'] -= ship_qty
                data['shipped_qty'] += ship_qty

    def ship_local_demand(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                on_hand_qty = data['on_hand_qty']
                demand_qty = data['forecast_qty']
                ship_qty = min(on_hand_qty, demand_qty)
                remaining_qty = demand_qty - ship_qty
                data['on_hand_qty'] -= ship_qty
                data['backorder_qty'] += remaining_qty
                data['shipped_qty'] += ship_qty

    def inventory_control(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                receipt_qty = sum(data['transit_qty'][t] for t in data['transit_qty'])
                receipt_qty += sum(data['wip_qty'][t] for t in data['wip_qty'])
                source_location = data['source']
                if source_location:
                    receipt_qty += sum(row[1] if row[0] == location else 0 for row in
                                       self.inv_data[item][source_location]['dependent_demand'])
                else:
                    receipt_qty += data['production_backorder']
                receipt_qty -= sum(row[1] for row in data['dependent_demand'])
                receipt_qty -= data['consumption_backorder']
                projected_inv = data['on_hand_qty'] + receipt_qty - data['backorder_qty']

                if projected_inv <= data['r_val']:
                    revised_r_val = 2 * data['r_val'] - projected_inv
                    order_qty = max(revised_r_val, data['moq'])
                    data['ordered_qty'] += order_qty
                    data['open_orders'] = order_qty

    def process_orders(self, t):
        i = 0
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                qty = data['open_orders']
                if qty == 0:
                    continue
                i += 1
                source = data['source']
                if source is None:
                    data['production_backorder'] += qty
                    for component, usage_qty in self.bom_relation.get(item, {}).get(location, []):
                        required_qty = usage_qty * qty
                        self.inv_data[component][location]['consumption_backorder'] += required_qty
                        self.inv_data[component][location]['order_received'] += required_qty
                else:
                    self.inv_data[item][source]['dependent_demand'].append((location, qty))
                    self.inv_data[item][source]['order_received'] += qty
                data['open_orders'] = 0
        return i

    def ship_dependent_demand(self, t):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                on_hand_qty = data['on_hand_qty']
                dependent_demand = data['dependent_demand']
                if on_hand_qty == 0 or len(dependent_demand) == 0:
                    continue
                for idx, row in enumerate(dependent_demand):
                    destination_location = row[0]
                    order_qty = row[1]
                    if order_qty == 0:
                        continue
                    ship_qty = min(order_qty, on_hand_qty)
                    data['shipped_qty'] += ship_qty
                    self.start_transit(item, location, destination_location, ship_qty, t)
                    on_hand_qty = data['on_hand_qty']
                    data['dependent_demand'][idx] = (destination_location, order_qty - ship_qty)
                    if on_hand_qty == 0:
                        break
                while len(data['dependent_demand']) > 0 and data['dependent_demand'][0][1] == 0:
                    data['dependent_demand'].pop(0)

    def start_transit(self, item, location, destination, qty, t):
        if qty == 0:
            return
        data  = self.inv_data[item][destination]
        mu, sigma = data['lead_time']
        lt = round(normalvariate(mu, sigma), 0)
        if lt < 0:
            lt = 0
        self.inv_data[item][location]['on_hand_qty'] -= qty
        if t+lt not in data['transit_qty']:
            data['transit_qty'][t + lt] = 0
        data['transit_qty'][t + lt] += qty

    def process_production(self, t):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                production_qty = data['production_backorder']
                if production_qty == 0:
                    continue
                init_production = 1
                for component, usage_qty in self.bom_relation.get(item, {}).get(location, []):
                    required_qty = usage_qty * production_qty
                    if self.inv_data[component][location]['on_hand_qty'] < required_qty:
                        init_production = 0
                if init_production == 1:
                    self.start_production(item, location, production_qty, t)
                    data['production_backorder'] -= production_qty

    def start_production(self, item, location, qty, t):
        if qty == 0:
            return
        data = self.inv_data[item][location]
        mu, sigma = data['lead_time']
        lt = round(normalvariate(mu, sigma), 0)
        if lt < 0:
            lt = 0
        if t+lt not in data['wip_qty']:
            data['wip_qty'][t+lt] = 0
        data['wip_qty'][t+lt] += qty
        for component, usage_qty in self.bom_relation.get(item, {}).get(location, []):
            required_qty = usage_qty * qty
            self.inv_data[component][location]['on_hand_qty'] -= required_qty
            self.inv_data[component][location]['consumption_backorder'] -= required_qty
            self.inv_data[component][location]['consumed_qty'] += required_qty

    def receive_in_transit(self, t):
        received = False
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                qty = data['transit_qty'].get(t, 0) + data['wip_qty'].get(t, 0)
                if qty > 0:
                    data['receipt_qty'] += qty
                    data['on_hand_qty'] += qty
                    data['transit_qty'][t] = 0
                    data['wip_qty'][t] = 0
                    received = True
        return received

    def initialize_opening_inv(self):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                data['opening_inv'] = data['on_hand_qty']
                data['receipt_qty'] = 0
                data['ordered_qty'] = 0
                data['order_received'] = 0
                data['shipped_qty'] = 0
                data['open_orders'] = 0
                data['consumed_qty'] = 0
                demand_mean, demand_std_dev = data['demand']
                data['forecast_qty'] = 0
                if demand_mean > 0:
                    demand_qty = normalvariate(demand_mean/7, demand_std_dev/sqrt(7)) - data['negative_forecast']
                    if demand_qty > 0:
                        data['forecast_qty'] = demand_qty
                        data['negative_forecast'] = 0
                    else:
                        data['negative_forecast'] = -demand_qty

    def daily_process(self, t, conn):
        self.initialize_opening_inv()
        self.receive_in_transit(t)
        self.process_production(t)
        self.ship_backorders()
        self.ship_local_demand()
        self.ship_dependent_demand(t)
        self.inventory_control()
        j = self.process_orders(t)
        while j > 0:
            self.process_production(t)
            self.ship_dependent_demand(t)
            self.inventory_control()
            j = self.process_orders(t)
        received = self.receive_in_transit(t)
        while received:
            self.process_production(t)
            self.ship_backorders()
            self.ship_dependent_demand(t)
            received = self.receive_in_transit(t)

        self.write_data(t, conn)

    def write_data(self, t, conn):
        for item in self.inv_data:
            for location in self.inv_data[item]:
                data = self.inv_data[item][location]
                opening_qty = data['opening_inv']
                source_location = data['source']
                forecast_qty = data['forecast_qty']
                shipped_qty = data['shipped_qty']
                received_qty = data['receipt_qty']
                order_qty = data['ordered_qty']
                backorder_qty = data['backorder_qty']
                in_transit_dict = data['transit_qty']
                in_transit_qty = sum(in_transit_dict[t] for t in in_transit_dict)
                wip_dict = data['wip_qty']
                wip_qty = sum(wip_dict[t] for t in wip_dict)
                dependent_list = data['dependent_demand']
                backorder_qty += sum(i[1] for i in dependent_list)
                closing_qty = data['on_hand_qty']
                production_backorder =  data['production_backorder']
                consumption_backorder = data['consumption_backorder']
                order_received_qty = data['order_received']
                consumed_qty = data['consumed_qty']
                data_row = (item, location, source_location, t, forecast_qty, order_qty,
                            in_transit_qty, received_qty, opening_qty, closing_qty,
                            shipped_qty, backorder_qty, wip_qty, production_backorder,
                            consumption_backorder, consumed_qty, order_received_qty)
                conn.execute(planning_insert_query, data_row)






