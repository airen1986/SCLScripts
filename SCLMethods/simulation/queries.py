get_inventory_sql = """select Inventory.ITEMCODE,
                       Inventory.LOCATIONCODE,
                       ifnull(ifnull(Transportation.MinimumShipmentQuantity, 
                            Inventory.MINIMUMORDERQUANTITY),0) as MinimumOrderQuantity,
                       max(ifnull(ifnull(Transportation.TRANSPORTATIONLTMEAN, 
                            Inventory.PRODUCTIONLTMEAN),0)) as TransportationLTMean,
                       ifnull(ifnull(Transportation.TRANSPORTATIONLTSTDDEV, 
                            Inventory.PRODUCTIONLTSTDDEV),0) as TransportationLTStdDev,
                       ifnull(Inventory.DemandMean,0) as demand_mean,
                       ifnull(Inventory.DEMANDSTDDEV,0) as deman_std_dev,
                       ifnull(Inventory.R_Value,0) as r_value
                from Inventory
                LEFT JOIN Transportation
                ON Inventory.ITEMCODE = Transportation.ITEMCODE
                and Inventory.LOCATIONCODE = Transportation.TOLOCATIONCODE
                GROUP BY Inventory.ITEMCODE, Inventory.LOCATIONCODE"""

get_transportation_sql = """SELECT ItemCode, ToLocationCode, FromLocationCode
                            FROM Transportation"""

planning_insert_query = """insert into planning_data (ItemCode ,
                                LocationCode ,
                                FromLocationCode, 
                                Period, 
                                ForecastQuantity, 
                                OrderQuantity, 
                                InTransitQuantity, 
                                ReceivingQuantity,
                                OpeningInventory,
                                ClosingInventory,
                                ShipQuantity,
                                BackorderedQuantity)
                            Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""