import SpaceTraders as ST
from SpaceTraders import io, F_utils, F_nav, F_trade
from SpaceTraders import fleet_resource_manager as fleet_res_mgr
import pandas as pd
import time, os, asyncio
from datetime import datetime, timezone

"""
    Automation scripts for simple tasks.
"""

### NAVIGATION ###
async def await_navigation(ship):
    """ Idle loop while the ship is in transit. """
    eta_seconds = F_nav.get_transit_time_seconds(ship)
    while eta_seconds > 0:
        #print(f'[INFO] {ship} standing by during navigation ({int(eta_seconds)} seconds).')
        await asyncio.sleep(max((eta_seconds / 2), 0.25))
        eta_seconds = F_nav.get_transit_time_seconds(ship)

async def navigate(ship, destination_wp):
    """ Handles navigation from a ship to a given waypoint. 
        Will try to BURN or CRUISE, but will not DRIFT or use fuel from cargo.
        Returns True if navigation successful (ship at wp), or False if navigation failed (ship remains in place)
    """
    # TODO: Handle inter-system pathing

    # Once in the right system, find a path to the destination waypoint without drifting   
    cur_nav = F_nav.get_ship_nav(ship)

    # Sanity check / Early return for edge case scenarios incl. a redundant call to navigate
    if (destination_wp == cur_nav['waypointSymbol']) and (not cur_nav['status'] == 'IN_TRANSIT'):
        return True

    path = F_nav.get_path(ship, cur_nav['waypointSymbol'], destination_wp)
    #path = [(destination_wp, flightmode)]
    for wp, flmode, dist in path: 
        F_nav.refuel_ship(ship)
        if F_nav.navigate_in_system(ship, wp, flightmode=flmode):
            await await_navigation(ship)
        else:
            # Ship couldn't hop for some reason. It might already be in place, or experiencing some other issue -- break the navigation to diagnose & report to caller
            break
    
    # Update ship info again once arrived to ensure internal consistency
    F_nav._refresh_ship_nav(ship)

    # Final check -- navigate returns True iff ship at destination
    if F_nav.get_ship_waypoint(ship) == destination_wp:
        return True
    else:
        print(f"[ERROR] {ship} could not complete path to {destination_wp}.")
        return False


### MARKETS ###
#asyncio.run(sell_to_market("RYVIOS-1", "X1-TP30-E43", {"AMMUNITION": 20}))
async def sell_to_market(ship : str, market : str, goods : dict):
    """ Navigates to given market and sells the specified cargo there. """

    # TODO sanity checks on cargo etc?

    # Navigate to market
    if not (await navigate(ship, market)):
        print(f"[ERROR] {ship} couldn't sell to market: unable to reach {market}.")
        return False

    # Sanity check - only make this sale if the ship is at the intended market
    cur_wp = F_nav.get_ship_waypoint(ship)
    if not cur_wp == market:
        print(f"[ERROR] {ship} couldn't sell to market: unable to reach {market}.")
        return False
    
    # Sell cargo
    #print(f'[INFO] {ship} arrived at {cur_wp} and will sell {goods}.')
    if not F_trade.sell_goods(ship, goods):
        print(f"[ERROR] {ship} failed to complete sale of {goods}.")
        return False
    
    # Refresh the market after sale
    await asyncio.sleep(3)
    F_trade.refresh_tradegoods(ship)

    return True

async def offload_to_market(ship : str, market : str):
    """ Orders ship to try selling its entire cargo hold at a market. Will return True if everything was sold. Does not check budgets. """
    to_offload = dict()
    for i in F_trade.get_ship_cargo(ship).get('inventory', list()):
        to_offload[i["symbol"]] = i["units"]
    return await sell_to_market(ship, market, to_offload)

async def buy_from_market(ship : str, market : str, goods : dict):
    """ Navigates to given market and purchases the specified cargo there. """

    # TODO sanity checks on cargo etc?

    # Navigate to market
    if not (await navigate(ship, market)):
        print(f"[ERROR] {ship} couldn't buy from market: unable to reach {market}.")
        return False
    
    # Sanity check - only make this purchase if the ship is at the intended market
    cur_wp = F_nav.get_ship_waypoint(ship)
    if not cur_wp == market:
        print(f"[ERROR] {ship} couldn't buy from market: unable to reach {market}.")
        return False
    
    # Purchase cargo
    #print(f'[INFO] {ship} arrived at {cur_wp} and will purchase {goods}.')
    if not F_trade.buy_goods(ship, goods):
        print(f"[ERROR] {ship} failed to complete purchase of {goods}.")
        return False
    
    # Refresh the market after purchase
    await asyncio.sleep(3)
    F_trade.refresh_tradegoods(ship)

    return True

async def buy_from_shipyard(ship, shipyard, target_ship_type):
    """ Navigates to shipyard and tries to buy a ship there. """
    
    # Navigate to shipyard
    if not await navigate(ship, shipyard):
        print(f"[ERROR] {ship} failed to reach {shipyard}. Aborting purchase.")
        return False
    await await_navigation(ship)

    # Attempt to buy the ship
    if not F_trade.buy_ship(ship, shipyard, target_ship_type):
        print(f"[ERROR] {ship} failed to purchase {target_ship_type} at {shipyard}.")
        return False

async def update_market(ship, waypoint):
    """ Navigates to a waypoint and updates the market & shipyard there. """

    server_refresh_delay = 4 # Market refresh tends to fail if server is queried immediately after ship arrives. This delay is added between navigation & scan

    def refresh_market(ship):
        if not F_nav.dock_ship(ship):
            return False
        tg_success = F_trade.refresh_tradegoods(ship)
        sy_success = F_trade.refresh_shipyard(ship, verbose=False)
        return tg_success
    
    # Lock ship - This is a blocking action
    fleet_res_mgr.set_ship_blocked_status(ship, True)
        
    arrived = (F_nav.get_ship_waypoint(ship) == waypoint)
    
    if not arrived:
        # Navigate to the market
        if not await navigate(ship, waypoint):
            fleet_res_mgr.set_ship_blocked_status(ship, False) # Unlock early (on return)
            return False
        await await_navigation(ship)

    # Refresh market
    await asyncio.sleep(server_refresh_delay)
    success = refresh_market(ship)

    # Unlock ship
    fleet_res_mgr.set_ship_blocked_status(ship, False)

    return success

async def fetch_cargo_from_ship(sink_ship, source_ship, good, units=None):
    """ Sends sink_ship to go and fetch given good from source_ship. If units not specified, takes all units. """
    # Go to location
    if not await navigate(sink_ship, F_nav.get_ship_waypoint(source_ship)):
        print(f"[ERROR] {sink_ship} could not fetch cargo from {source_ship} : target ship unreachable.")
        return False
    await await_navigation(sink_ship)

    # Ensure identical status
    src_nav = F_nav.get_ship_nav(source_ship)
    if src_nav["status"] == "DOCKED":
        F_nav.dock_ship(sink_ship)
    
    to_transfer = units
    if to_transfer is None:
        src_cargo = F_trade.get_ship_cargo(source_ship)
        for i in src_cargo["inventory"]:
            if i["symbol"] == good:
                to_transfer = i["units"]

    success = F_trade.transfer_cargo(source_ship, sink_ship, good, to_transfer)
    if not success:
        print(f"[ERROR] {sink_ship} failed to fetch {units} {good} from {source_ship}.")
    else:
        print(f"[INFO] {sink_ship} fetched {to_transfer} {good} from {source_ship}.")

    return success

async def drain_cargo_from_ship(sink_ship, source_ship):
    """ Sends sink_ship to go fetch all cargo from source_ship. """
    # TODO: Implement checks for cargo
    target_goods = list()
    success = True
    for i in F_trade.get_ship_cargo(source_ship).get('inventory', list()):
        #target_goods.append({"good": i["symbol"], "units": i["units"]})
        success = success and await fetch_cargo_from_ship(sink_ship, source_ship, i["symbol"], i["units"])
    return success


# Market recon loop
async def market_update_loop(ship, path=None, loops=-1):
    """ Sends the ship on a continuous mission to visit every market in the system & refresh trade info. Also refreshes shipyard data if present. """
    if path is None:
        # Get a sorted path of markets from the current location 
        cur_nav = F_nav.get_ship_nav(ship)
        cur_sys = cur_nav['systemSymbol']
        cur_loc = cur_nav['waypointSymbol']
        market_wps = [w['symbol'] for w in F_nav.get_waypoints_in_system(cur_sys, traits=['MARKETPLACE'])]
        path = list()
        if cur_loc in market_wps:
            # Route starts here
            path = [cur_loc]
            market_wps.remove(cur_loc)
        else:
            # Path starts in closest market
            path = [F_nav.get_closest_wp(cur_loc, market_wps)]

        # Build path by greedily following shortest hops
        max_hop = 0
        while len(market_wps) > 0:
            next_market = F_nav.get_closest_wp(path[-1], market_wps)
            d= F_nav.wp_distance(path[-1], next_market)
            if d > max_hop:
                max_hop = d
            #print(f"Move from {path[-1]} to {next_market} in {F_nav.wp_distance(path[-1], next_market):.1f}")
            path.append(next_market)
            market_wps.remove(path[-1])
        print(f"[INFO] {ship} plotted market recon through {len(path)} markets, with a longest hop of {max_hop}.")

    # Loop over the path
    i = 0                           # Keeps track of iteration
    interval_seconds = 60 * 20      # Time between loops
    while loops == -1 or i < loops:
        loops += 1
        print(f"[INFO] {ship} starting market update loop ({i}).")
        try:
            # Sanity check - ensure that the ship isn't in transit before actually starting
            await await_navigation(ship)
            
            for ix, market in enumerate(path):
                try:
                    # Navigate to next market
                    if not await navigate(ship, market):
                        print(f"[ERROR] {ship} failed to reach market {market}.")
                        continue
                
                    print(f"[INFO] {ship} arrived at market {market} and is waiting to fetch data.")
                    time.sleep(6) # Server needs to register that we now have a ship here

                    # Try refreshing trade good info
                    success = F_trade.refresh_tradegoods(ship) # This persists to the database
                    if not success:
                        print(f"[ERROR] {ship} failed to refresh trade good info at {market}")

                    # Try refreshing shipyard info
                    success = F_trade.refresh_shipyard(ship, verbose=False)
                    if success:
                        print(f"[INFO] {ship} refreshed Shipyard data for {market}.")

                    print(f"[INFO] {ship} successfully refreshed data for {market}. Moving on to {path[(ix+1)%len(path)]}.")
                except Exception as e:
                    io.log_exception(e)
                    print(f"[ERROR] Exception during market update loop:")
                    print(e)
                    continue # Just try moving on to the next market
        except Exception as e:
            print(f"[ERROR] Exception during market update loop:")
            print(e)

        time.sleep(interval_seconds)

# Shipyard recon loop
def scan_shipyards(ship):
     
    # Get a list of shipyards
    cur_sys = F_nav.get_ship_nav(ship)['systemSymbol']
    wps = F_nav.get_waypoints_in_system(cur_sys, traits=['SHIPYARD'])

    # Visit & record each one
    while len(wps) > 0:
        cur_loc = F_nav.get_ship_waypoint(ship)
        wps = sorted(wps, key=lambda w : F_nav.wp_distance(cur_loc, w['symbol']), reverse=True)
        cur_sy = wps.pop()

        # Navigate to new shipyard
        m_wp_id = cur_sy['symbol']
        F_nav.refuel_ship(ship)
        F_nav.navigate_in_system(ship, m_wp_id)
        await_navigation(ship)
        
        print(f"[INFO] {ship} arrived at shipyard {m_wp_id} and is waiting to fetch data.")
        time.sleep(6) # Server needs to register that we now have a ship here

        # Try refreshing shipyard info
        success = F_trade.refresh_shipyard(ship) # This persists to the database
        if not success:
            print(f"[ERROR] {ship} failed to refresh shipyard info at {m_wp_id}")

        print(f"[INFO] {ship} successfully refreshed data for {m_wp_id}. Moving on.")

    print(f"[INFO] {ship} has finished shipyard recon. Pausing.")

        
async def execute_trade(ship : str, source_market : str, sink_market : str, goods : dict):
    """ Commands the ship to execute a trade. Handles the whole trade end-to-end, reports success. """
    # Sanity check - Ship has an empty hold
    cargo_held = F_trade.get_ship_cargo(ship)['units']
    if cargo_held > 0:
        print(f"[ERROR] {ship} is trying to trade with a non-empty hold. Standing by for intervention.")
        return False
    # Sanity check - ensure that the ship isn't in transit
    await await_navigation(ship)

    # Block ship during trade
    fleet_res_mgr.set_ship_blocked_status(ship, True)

    ts_start = datetime.now(timezone.utc) # Ship only does one thing at a time, so we can check the time window of this trade to figure out what the ship did in the logs later

    buy = await buy_from_market(ship, source_market, goods)
    if not buy:
        print(f"[ERROR] {ship} was unable to procure trade goods. Aborting trade.")
        return False

    sell = await sell_to_market(ship, sink_market, goods)
    if not sell:
        print(f"[ERROR] {ship} was unable to offload trade goods. Aborting trade.")
        return False

    # On success, report some statistics
    profit = F_trade.get_total_profit_from_trade(ship, source_market, sink_market, ts_start.strftime('%Y-%m-%dT%H:%M:%SZ'))
    if profit is not None:
        print(f"[INFO] {ship} finished trade. Total profit: {profit} credits.")
    else:
        print(f"[INFO] {ship} finished trade.")

    # Unblock ship during trade
    fleet_res_mgr.set_ship_blocked_status(ship, False)

    return True

async def naive_trader(ship):
    """ Picks a 'sustainable' trade route and initiates it. Backs off by default to allow for markets to stabilise. """
    CONTROLLER_ID = "NAIVE-TRADER-" + ship
    loops            = 0
    interval_seconds = 60 * 5
    selection_query  = \
                        """
                        select
                            *
                        from TRADE_SYSTEM_MARGINS
                        where 1=1
                            and margin > 10
                            and source_volume >= 6 and sink_volume >= 6
                            and distance < 250
                            and src_supply in ("ABUNDANT", "HIGH")
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
            print(f"[ERROR] {ship} is trying to trade with a non-empty hold. Standing by for intervention.")
            return False
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

async def boost_good_growth(ship, system, goods):
    """ Tries growing market volumes for given goods, in given system. """
    loops            = 0
    interval_seconds = 60 * 2
    selection_query  = \
                        f"""
                        select
                            *
                        from SUPPLY_CHAIN_TRADES
                        where 1=1
                            and target_good in ({', '.join([f'"{g}"' for g in goods])})
                            and target_market like "{system}-%"
                            and margin > 2
                            and imported_good_supply not in ("ABUNDANT", "HIGH")
                            and export_supply not in ("SCARCE", "LIMITED")
                        order by margin desc
                        limit 1
                        """
    
    ship_info = ST.get_ship_info(ship)
    max_cargo = ship_info['cargo']['capacity']

    while True:
        loops += 1

        # Sanity check - Ship has an empty hold
        cargo_held = F_trade.get_ship_cargo(ship)['units']
        if cargo_held > 0:
            print(f"[ERROR] {ship} is trying to trade with a non-empty hold. Standing by for intervention.")
            return False
        # Sanity check - ensure that the ship isn't in transit
        await await_navigation(ship)

        # Try picking a route
        candidates = io.read_dict(selection_query)
        route_data = None
        if len(candidates) > 0:
            route_data = candidates[0]
        
        if route_data is not None:
            # If a route is found, start it
            print(f"[INFO] {ship} starting trade route: {min(route_data['export_tradeVolume'], max_cargo)} {route_data['imported_good']} from {route_data['export_market']} to {route_data['target_market']}.")
            trade_goods = {route_data['imported_good']:  min(route_data['export_tradeVolume'], max_cargo)}
            success = await execute_trade(ship, route_data['export_market'], route_data['target_market'], trade_goods)
        else:
            print(f"[INFO] {ship} found no suitable routes. Standing by.")

        # Idle until next loop
        await asyncio.sleep(interval_seconds)


### MINING ###

async def mine_goods(ship, goods, verbose=True):
    """ Extracts from the current waypoint until cargo hold is filled, keeping only the desired goods.
        This function blocks the thread.
    """
    max_iterations = 50

    # Orbit location
    F_nav.orbit_ship(ship)

    for _it in range(max_iterations):
        r = ST.post_request(f'/my/ships/{ship}/extract')
        if r.status_code == 201:
            data = r.json()['data']

            # Check if the good is desired; if not, jettison it immediately.
            e_yield = data['extraction']['yield']

            if verbose:
                print(f"[INFO] Ship {ship} extracted {e_yield['units']} {e_yield['symbol']}.")

            if e_yield['symbol'] not in goods:
                ST.post_request(f'/my/ships/{ship}/jettison', data={'symbol': e_yield['symbol'], 'units': e_yield['units']})

                if verbose:
                    print(f"[INFO] Ship {ship} jettissoned {e_yield['units']} {e_yield['symbol']}.")
            
            if data['cargo']['capacity'] <= data['cargo']['units']:
                # Hold is full. Stop extracting.
                if verbose:
                    print(f'[INFO] Ship {ship} finished extracting (full hold).')
                return True
            # Otherwise, sleep until next extraction
            cd = data['cooldown']['remainingSeconds']
            await asyncio.sleep(cd)
        else:
            print(f'[ERROR] Ship {ship} failed to extract. Aborting operation.')
            print(f' [INFO]', r.json())
            return False

#scripts.mine_loop('RYVIOS-3', 'X1-TP30-AB5E', 'X1-TP30-H51', ['IRON_ORE', 'ALUMINUM_ORE'])
async def mine_loop(ship, source, market, resources):
    """ Continually extracts from the source, then sells selected resources to the market. """

    while True:
        # Navigate to asteroid
        if not await navigate(ship, source):
            print(f"[ERROR] {ship} was unable to reach extraction source. Standing by.")
            return False

        # Mine desired resources
        await mine_goods(ship, resources)

        # Sell to market
        cargo = F_trade.get_ship_cargo(ship)
        to_sell = dict()
        for g in cargo['inventory']:
            tg = g['symbol']
            if tg in resources:
                to_sell[tg] = g['units']
        if not await sell_to_market(ship, market, to_sell):
            print(f"[ERROR] {ship} was unable to sell cargo. Standing by.")
            return False

        await asyncio.sleep(10) # Small backoff between loops

### CONTRACTS ###

# scripts.delivery_loop('RYVIOS-3', 'cmhj5v575ir8eri738qzphmt3', 'X1-TP30-AB5E')
def delivery_loop(ship, contract_id, source):
    """ Continually extracts resource from the source, then delivers to the contract destination. """

    contract = ST.get_contract(contract_id)

    if contract['type'] != 'PROCUREMENT':
        print(f"[ERROR] Contract {contract_id} is not a procurement job. Aborting.")
        return False
    
    objective = contract['terms']['deliver'][0] # TODO Account for multiple requests per contract
    resource = objective['tradeSymbol']
    sink     = objective['destinationSymbol']
    
    for _ in range(100):
        # Navigate to asteroid
        ST.navigate_in_system(ship, source)
        while ST.check_in_transit(ship):
            time.sleep(2)

        # Refuel
        ST.refuel_ship(ship)

        # Mine desired resources
        target_resources = [resource]
        ST.mine_goods(ship, target_resources, verbose=True)

        # Navigate to mission delivery point
        ST.navigate_in_system(ship, sink)
        while ST.check_in_transit(ship):
            time.sleep(2)

        # Deliver cargo
        ST.deliver_cargo(contract_id, ship, resource, verbose=True)
