delete_planning_data = "DELETE FROM T_PlanningData"

planning_insert_query = """Insert into T_PlanningData (ItemCode ,
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
                                BackorderQuantity,
                                WIPQuantity)
                            Values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

insert_forecast_query = """ INSERT INTO I_ActiveCombinations (ItemCode, LocationCode)
                            SELECT DISTINCT ItemCode, LocationCode
                            FROM D_Inventory
                            WHERE IFNULL(DemandMean,0) > 0 """

delete_active_combinations = "DELETE FROM I_ActiveCombinations"

tp_insert_query = """INSERT INTO I_ActiveCombinations (ItemCode, LocationCode)
                        SELECT DISTINCT dt.ItemCode, dt.FromLocationCode
                        FROM I_ActiveCombinations ia1,
                             D_Transportation dt
                        LEFT JOIN I_ActiveCombinations ia2
                        ON dt.ItemCode = ia2.ItemCode
                        AND dt.FromLocationCode = ia2.LocationCode
                        WHERE ia1.ItemCode = dt.ItemCode
                        AND   ia1.LocationCode = dt.ToLocationCode
                        and   ia2.LocationCode is NULL"""

bom_insert_query = """WITH T1
                        AS
                        (
                        SELECT distinct dop.ItemCode, dop.LocationCode, db.ItemCode FromItemCode
                        FROM D_OperationProcess dop,
                             D_BOMInventoryRequirement db
                        WHERE dop.LocationCode = db.LocationCode
                        and   dop.BOMCode = db.BOMCode
                        )
                        SELECT DISTINCT T1.FromItemCode, T1.LocationCode
                        FROM I_ActiveCombinations ia1,
                             T1
                        LEFT JOIN I_ActiveCombinations ia2
                        ON T1.FromItemCode = ia2.ItemCode
                        AND T1.LocationCode = ia2.LocationCode
                        WHERE ia1.ItemCode = T1.ItemCode
                        AND   ia1.LocationCode = T1.LocationCode
                        and   ia2.LocationCode is NULL"""

get_combinations_sql = """select  D_Inventory.ItemCode, 
                                D_Inventory.LOCATIONCODE,
                                D_Transportation.FromLocationCode,
                                ifnull(D_Inventory.DEMANDMEAN, 0) as DEMANDMEAN,
                                ifnull(D_Inventory.DEMANDSTDDEV,0) as DEMANDSTDDEV,
                                ifnull(D_Inventory.MINIMUMORDERQUANTITY,0) as MINIMUMORDERQUANTITY,
                                D_Inventory.R_Value as R_Value,
                                max(ifnull(ifnull(D_Transportation.TRANSPORTATIONLTMEAN, D_Inventory.PRODUCTIONLTMEAN),0)) as LTMean,
                                ifnull(ifnull(D_Transportation.TRANSPORTATIONLTSTDDEV, D_Inventory.PRODUCTIONLTSTDDEV),0) as LTStdDev
                        from I_ActiveCombinations,
                             D_Inventory
                        LEFT JOIN D_Transportation 
                        ON D_Inventory.ITEMCODE = D_Transportation.ITEMCODE
                        and D_Inventory.LOCATIONCODE = D_Transportation.TOLOCATIONCODE
                        WHERE I_ActiveCombinations.ItemCode = D_Inventory.ItemCode
                        and   I_ActiveCombinations.LocationCode = D_Inventory.LocationCode
                        GROUP BY D_Inventory.ItemCode, 
                                 D_Inventory.LOCATIONCODE"""

