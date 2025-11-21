"""
    System Trade Controller

    Functions that enable a controller to automatically use available haulers in a system to execute trades, using different strategies.
"""
import asyncio, random, time
from SpaceTraders import io, fleet_resource_manager, scripts, F_utils, F_nav, F_trade

### GLOBALS ###
BASE_PRIO_TRADERS = 300
BASE_CONTROLLER_ID = "TRADE-CONTROLLER"


### GETTERS ###


### HELPERS ###
async def naive_trader(ship, run_interval = None):
    """ Picks a 'sustainable' trade route and initiates it. Backs off by default to allow for markets to stabilise. """
    CONTROLLER_ID = "NAIVE-TRADER-" + ship
    loops            = 0
    interval_seconds = run_interval or (60 * 5)
    selection_query  = \
                        """
                        select
                            *
                        from TRADE_SYSTEM_MARGINS
                        where 1=1
                            and margin > 10
                            and source_volume >= 6 and sink_volume >= 6
                            and distance < 250
                            and src_supply in ("ABUNDANT", "HIGH", "MODERATE")
                            and sink_supply in ("SCARCE", "LIMITED")
                            and symbol not in ("FAB_MATS", "ADVANCED_CIRCUITRY", "QUANTUM_STABILIZERS")
                            order by margin desc
                        """
    
    while True:
        loops += 1

        # Sanity check - Ship has an empty hold
        ship_cargo = F_trade.get_ship_cargo(ship)
        cargo_held = ship_cargo['units']
        if cargo_held > 0:
            print(f"[INFO] {ship} is trying to trade with a non-empty hold. Clearing cargo first.")
            await clear_cargo(ship)
        # Sanity check - ensure that the ship isn't in transit
        await await_navigation(ship)

        # Try picking a route
        candidates = io.read_df(selection_query)

        route_data = None
        if len(candidates) > 0:
            route_data = candidates.iloc[0].to_dict()
        
        if route_data is not None:
            # If a route is found, start it
            fleet_res_mgr.lock_ship(ship, CONTROLLER_ID, 2)
            max_cargo = ship_cargo['capacity']
            max_purchase = min(route_data['source_volume'], route_data['sink_volume'])
            to_trade = min(max_purchase, max_cargo)
            exp_profit = to_trade * route_data['margin']
            print(f"[INFO] {ship} starting trade route: {to_trade} {route_data['symbol']} from {route_data['source']} to {route_data['sink']}. Expected profit is {exp_profit} cr.")
            trade_goods = {route_data['symbol']:  to_trade}
            success = await execute_trade(ship, route_data['source'], route_data['sink'], trade_goods)
            fleet_res_mgr.release_ship(ship)
        else:
            print(f"[INFO] {ship} found no suitable routes. Standing by.")

        # Idle until next loop
        await asyncio.sleep(interval_seconds)


### MAIN ENTRY ###
async def trade_in_system(system : str, max_haulers : int, strategy : str = "greedy"):
    """ Tries to acquire a fleet of haulers (up to given maximum) and has them execute trades following the given strategy. 
        Strategies:
            - greedy (default) : Traders will chase after highest profit margins
    """
    # TODO : implement this
    raise NotImplementedError("Trade fleet controller still under construction.")