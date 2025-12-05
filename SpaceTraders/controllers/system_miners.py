"""
    System Excavator Controller

    Functions that enable a controller to automatically use available probes in a system to siphon resources from the local Gas Giant or mine resources from the local Engineered Asteroid.
"""
import asyncio, random, time
from SpaceTraders import io, fleet_resource_manager, scripts, F_utils, F_nav, F_extract, F_trade

### GLOBALS ###
BASE_PRIO_EXTRACTORS = 100
BASE_PRIO_HAULERS    = 350 
BASE_CONTROLLER_ID   = "EXTRACTION-CONTROLLER"


### GETTERS ###
def get_finished_ships(fleet):
    """ Returns a list of ship names that have finished their tasks. """
    return [s for s in fleet.keys() if fleet[s].get('task', None) is not None and fleet[s]['task'].done()]

def get_available_siphon_drones(system : str, priority : int, controller : str):
    available = fleet_resource_manager.get_available_ships_in_systems([system], 'EXCAVATOR', prio=priority, controller=controller)
    q_siphoners = f"""
        select
            distinct shipSymbol
            from 'ship.MOUNTS'
            where 1=1
            and symbol like "MOUNT_GAS_SIPHON%"
            and shipSymbol in ({', '.join([f'"{s}"' for s in available])})
    """
    return [r[0] for r in io.read_list(q_siphoners)]

def get_available_mining_drones(system : str, priority : int, controller : str):
    available = fleet_resource_manager.get_available_ships_in_systems([system], 'EXCAVATOR', prio=priority, controller=controller)
    q_miners = f"""
        select
            distinct shipSymbol
            from 'ship.MOUNTS'
            where 1=1
            and symbol like "MOUNT_MINING_LASER%"
            and shipSymbol in ({', '.join([f'"{s}"' for s in available])})
    """
    return [r[0] for r in io.read_list(q_miners)]

def get_closest_haulers_to_wp(waypoint : str, priority : int, controller : str):
    """ Returns list of haulers sorted by ascending distance to waypoint. Includes haulers who are currently busy. """
    q = f"""
        select
            reg.shipSymbol
            ,reg.role
            ,nav.waypointSymbol
            ,dists.dist
        from 'ship.NAV' nav

        inner join 'control.SHIP_LOCKS' ctrl
        on ctrl.shipSymbol = reg.shipSymbol

        inner join 'ship.REGISTRATION' reg
        on reg.shipSymbol = nav.symbol
        and reg.role = "HAULER"

        inner join 'WP_DISTANCES' dists
        on  dists.src = nav.waypointSymbol
        and dists.dst = "{waypoint}"

        order by dist asc
    """
    rows = io.read_dict(q)
    rows = [r['shipSymbol'] for r in rows]
    return rows or list()

def get_full_excavators_at_wp(waypoint : str, cargo_pct : float):
    """ Returns excavators in orbit around given waypoint, who have at least cargo_pct% of their cargo filled. """
    q = f"""
           select
            reg.shipSymbol
            ,reg.role
            ,nav.waypointSymbol
        from 'ship.NAV' nav

        inner join 'ship.REGISTRATION' reg
        on reg.shipSymbol = nav.symbol
        and reg.role = "EXCAVATOR"

        inner join 'ship.CARGO' cargo
        on cargo.shipSymbol = nav.symbol
        and cargo.totalUnits >= (cargo.capacity * {cargo_pct})

        where nav.waypointSymbol = "{waypoint}" 

        order by cargo.totalUnits desc
    """
    rows = io.read_dict(q)
    rows = [r['shipSymbol'] for r in rows]
    return rows or list()

def get_yield_since(ships, ts):
    """ Returns total yield of all ships since given timestamp (Unix). """
    q_yield = f"""
        select
            sum(units) as total
        from YIELDS
        where ship in ({', '.join([f'"{s}"' for s in ships])})
    """
    yields = io.read_dict(q_yield) or 0
    if len(yields) > 0:
        yields = yields[0]['total']
    else:
        yields = 0
    return yields

def get_ship_trade_profit_since(ship : str, ts_start : int, ts_end : int = None):
    """ Returns the total profit a ship has made selling hauls in the given time window. Timestamps are in unix format and do not account for server-client time offset. Fix your timestamps before calling this. """
    query = f"""
        select
            sum(totalPrice)
        from TRANSACTIONS t

        inner join 'control.EXCAVATOR_GOODS' wl
        on wl.symbol = t.tradeSymbol
    
        where shipSymbol = "{ship}"
        and ts_created >= {ts_start}
        and type = "SELL"
    """
    if ts_end: query += f"\nand ts_end <= {ts_end}"
    try:
        result = io.read_list(query)
        if result and result[0][0] is not None:
            return result[0][0]
        else:
            return 0
    except Exception as e:
        print(f"[ERROR] Unhandled exception while calculating total hauling profit for {ship} since {ts_start}.")
        io.log_exception(e)
        return 0

def get_ship_traded_units_since(ship : str, ts_start : int, ts_end : int = None):
    query = f"""
        select
            sum(units)
        from TRANSACTIONS t

        inner join 'control.EXCAVATOR_GOODS' wl
        on wl.symbol = t.tradeSymbol
    
        where shipSymbol = "{ship}"
        and ts_created >= {ts_start}
        and type = "SELL"
    """
    if ts_end: query += f"\nand ts_end <= {ts_end}"
    try:
        result = io.read_list(query)
        if result and result[0][0] is not None:
            return result[0][0]
        else:
            return 0
    except Exception as e:
        print(f"[ERROR] Unhandled exception while calculating total units sold for {ship} since {ts_start}.")
        io.log_exception(e)
        return 0

### HELPERS ###
def _log_sale(ship : str, profit : int, units : int, ts_start : int, ts_end : int, controller : str = None):
    """ Records a yield sale in the DB. """
    return io.write_data('YIELD_SALES', {"ship": ship, "controller": controller, "units": units, "profit": profit, "ts_start": ts_start, "ts_end": ts_end})

async def siphon_goods(ship: str, waypoint : str, goods : list = None):
    """ Siphon from a waypoint until cargo hold is filled, keeping only the desired goods.
    """
    refresh_period = 10 # Time between checks if ship gets locked (full cargo)

    # Navigate to waypoint if not there
    await scripts.navigate(ship, waypoint)

    # Orbit location
    F_nav.orbit_ship(ship)

    # Continually extract from destination
    while True:
        # Check if the hold is already full
        cargo = F_trade.get_ship_cargo(ship)
        if cargo['capacity'] <= cargo['units']:
        # Hold is full. Stop extracting and wait a while.
            await asyncio.sleep(refresh_period)
            continue

        if F_extract.siphon(ship, goods=goods):
            
            # Check cargo capacity
            cargo = F_trade.get_ship_cargo(ship)
            if cargo['capacity'] <= cargo['units']:
            # Hold is full. Stop extracting and wait a while.
                print(f"[INFO] {ship} has filled its hold. Standing by for pickup.")
                await asyncio.sleep(refresh_period)
            else:
            # Otherwise, sleep until next extraction
                cd = F_utils.get_ship_cooldown(ship)['remainingSeconds']
                #print(f"[INFO] {ship} cooling down for {cd} seconds.")
                await asyncio.sleep(cd+0.15)
        else:
        # Extraction failed for some reason; idle for a while and then try again
            cd = max(F_utils.get_ship_cooldown(ship)['remainingSeconds'], refresh_period)
            #print(f'[WARNING] Ship {ship} failed to siphon. Retrying in {cd} seconds.')
            await asyncio.sleep(cd)

async def extract_goods(ship: str, waypoint : str, goods : list = None):
    """ Extracts from a waypoint until cargo hold is filled, keeping only the desired goods.
    """
    refresh_period = 10 # Time between checks if ship gets locked (full cargo)

    # Navigate to waypoint if not there
    await scripts.navigate(ship, waypoint)

    # Orbit location
    F_nav.orbit_ship(ship)

    # Continually extract from destination
    while True:
        # Check if the hold is already full
        cargo = F_trade.get_ship_cargo(ship)
        if cargo['capacity'] <= cargo['units']:
        # Hold is full. Stop extracting and wait a while.
            await asyncio.sleep(refresh_period)
            continue

        if F_extract.extract(ship, goods=goods):
            
            # Check cargo capacity
            cargo = F_trade.get_ship_cargo(ship)
            if cargo['capacity'] <= cargo['units']:
            # Hold is full. Stop extracting and wait a while.
                print(f"[INFO] {ship} has filled its hold. Standing by for pickup.")
                await asyncio.sleep(refresh_period)
            else:
            # Otherwise, sleep until next extraction
                cd = F_utils.get_ship_cooldown(ship)['remainingSeconds']
                #print(f"[INFO] {ship} cooling down for {cd} seconds.")
                await asyncio.sleep(cd+0.15)
        else:
        # Extraction failed for some reason; idle for a while and then try again
            cd = max(F_utils.get_ship_cooldown(ship)['remainingSeconds'], refresh_period)
            await asyncio.sleep(cd)

async def haul_yields(ship : str, drones : list, controller : str = None):
    """ Orders a ship to go pick up the cargo held by target drones, and sell it to nearby markets. """
    # TODO properly implement this 
    fleet_resource_manager.set_ship_blocked_status(ship, blocked=True)

    # Await navigation if in-transit when receiving order
    await scripts.await_navigation(ship)

    # Collect yields from target drones
    ts_start = int(time.time())
    for d in drones:
        # Navigate to drone
        await scripts.navigate(ship, F_nav.get_ship_waypoint(d))

        # Drain its cargo
        if not await scripts.drain_cargo_from_ship(ship, d):
            print(f"[ERROR] {ship} was unable to drain cargo from {d}.")

    print(f"[INFO] {ship} picked up designated yields.")

    # Go sell off the collected haul
    if not await scripts.clear_cargo(ship):
        print(f"[ERROR] {ship} was unable to sell off its collected haul.")
    fleet_resource_manager.set_ship_blocked_status(ship, blocked=False)

    # Report
    profit = get_ship_trade_profit_since(ship, ts_start)
    units  = get_ship_traded_units_since(ship, ts_start)
    _log_sale(ship, profit, units, ts_start, int(time.time()), controller)
    print(f"[INFO] [{controller}] {ship} sold {units} extracted goods for {profit} credits.")

def dispatch_haulers(candidates : list, targets : list, fleet : dict, priority : int, controller : str):
    """ Attempts to cover all targets (drones) by dispatching candidate haulers to fetch & sell their yields. """
    MIN_HAUL_RATIO = 0.75 # Minimum % of hauler capacity that must be picked up before an order is actually given
    # Approach: pick up candidate haulers starting with the first in the list
    # If one is acquired, check how much cargo it can support & have it target as many drones as it can
    h_ix = 0
    while len(targets) > 0 and h_ix < len(candidates):
        h = candidates[h_ix]
        hauler_acquired = fleet_resource_manager.request_ship(h, controller, priority)

        # Early break: this hauler isn't available, but there are other candidates. Move on to the next.
        if not hauler_acquired:
            h_ix += 1
            continue

        # TODO maybe make haulers smarter about pre-existing cargo (selling off goods that are on the whitelist before moving on with the order?)
        # Check hauler's cargo capacity  
        h_cargo = F_trade.get_ship_cargo(h)
        capacity = h_cargo['capacity'] - h_cargo['units']

        # Round up targets until the capacity is reached
        h_targets   = list()
        total_yield = 0 
        for ix, d in enumerate(targets):
            yield_units = F_trade.get_ship_cargo(d)['units']
            if total_yield + yield_units <= capacity:
                h_targets.append(d)
                total_yield += yield_units

        # Optimization: the trip is only worth it if the hauler can sell enough goods.
        if total_yield < (capacity * MIN_HAUL_RATIO):
            # Remaining drones aren't sufficiently filled for this hauler. Release the hauler immediately and try the next one (which may have a smaller hold)
            fleet_resource_manager.release_ship(h)
            h_ix += 1
            continue

        # Actually dispatch the hauler
        print(f"[INFO] [{time.strftime('%H:%M:%S')}] {h} en-route to pick up {total_yield} units of mined goods from {h_targets}.")
        fleet[h] = {
            'targets': h_targets,
            'task': asyncio.create_task(haul_yields(h, h_targets, controller)),
            'ts_start': int(time.time())
        }

        # Bookkeeping
        targets = list(set(targets) - set(h_targets))
        h_ix += 1
    
    # At the end, if no more target drones remain unserviced, the dispatching was successful
    return len(targets) == 0



### MAIN ENTRY ###
async def extract_in_system(system):
    """ Acquires all excavators in the system and sends them to the appropriate extraction site for indefinite work. """

    # TODO: Consider incorporating surveys into this controller

    # Since there is a risk of over-mining, the actual max excavators is capped
    MAX_MINERS     = 8
    MAX_SIPHONERS  = 10 
    REFRESH_PERIOD = 15  # How often the controller updates its fleet
    STATUS_REPORT_PERIOD = 60 * 10 

    # Bookkeeping
    controller = BASE_CONTROLLER_ID + '-EXCAVATORS-' + system
    priority = BASE_PRIO_EXTRACTORS
    fleet_miners    = dict()
    fleet_siphoners = dict()
    ts_start = int(time.time())
    ts_last_report = time.time()

    # Extraction sites are static per system, so only need to be looked up on startup
    wp_miners = io.read_dict("SELECT symbol FROM 'nav.WAYPOINTS' WHERE type = \"ENGINEERED_ASTEROID\"")[0]['symbol']
    wp_siphon = io.read_dict("SELECT symbol FROM 'nav.WAYPOINTS' WHERE type = \"GAS_GIANT\"")[0]['symbol']

    while True:

        # Update desired resources
        # This only affects new ships -- if another controller wants to force a 'reset', the fleet should be forcibly released
        goods = [r[0] for r in io.read_list("SELECT symbol FROM 'control.EXCAVATOR_GOODS'")]

        # Acquire fleet if necessary
        if len(fleet_miners) < MAX_MINERS:
            candidates = get_available_mining_drones(system, priority, controller)
            to_acquire = min(MAX_MINERS - len(fleet_miners), len(candidates))
            for i in range(to_acquire):
                miner = candidates[i]
                if fleet_resource_manager.request_ship(miner, controller, priority):
                    # Lock ship since to indicate that the ship is busy
                    fleet_miners[miner] = {
                        "waypoint": wp_miners,
                        "task": asyncio.create_task(extract_goods(miner, wp_miners, goods)),
                        "time_start": int(time.time())
                    }

        if len(fleet_siphoners) < MAX_SIPHONERS:
            candidates = get_available_siphon_drones(system, priority, controller)
            to_acquire = min(MAX_SIPHONERS - len(fleet_siphoners), len(candidates))
            for i in range(to_acquire):
                siphoner = candidates[i]
                if fleet_resource_manager.request_ship(siphoner, controller, priority):
                    fleet_siphoners[siphoner] = {
                        "waypoint": wp_siphon,
                        "task": asyncio.create_task(siphon_goods(siphoner, wp_siphon, goods)),
                        "time_start": int(time.time())
                    }

        # Fleet cleanup
        # Note that this shouldn't really be necessary since excavators work their task forever
        for s in fleet_miners:
            if fleet_miners[s]['task'].done():
                fleet_resource_manager.release_ship(s)
                del fleet_miners[s]
        for s in fleet_siphoners:
            if fleet_siphoners[s]['task'].done():
                fleet_resource_manager.release_ship(s)
                del fleet_siphoners[s]

        if (time.time() - ts_last_report) >= STATUS_REPORT_PERIOD:
            # Avg yield since start
            all_ships = list(fleet_miners.keys()) + list(fleet_siphoners.keys())
            cur_yield = get_yield_since(all_ships, ts_start)
            # Yield per hour calculated as yield per minute * 60
            dt_minutes    = (int(time.time()) - ts_start) / 60
            yield_per_min = cur_yield / dt_minutes 
            avg_yield_per_hour = yield_per_min * 60
            # Report once a minute
            rep = f"[STATUS REPORT - {controller}] [{time.strftime('%H:%M:%S')}]\n"
            rep += f"\t  [INFO] Currently controlling {len(fleet_miners)} miners and {len(fleet_siphoners)} siphon drones.\n"
            rep += f"\t  [INFO] Total yield for job : {cur_yield} units.\n"
            rep += f"\t  [INFO] Projected units/hr  : {avg_yield_per_hour:.1f} u/hr.\n"
            rep += f"  Active since {F_utils.unix_to_ts(ts_start)}."
            print(rep)
            ts_last_report = time.time()

        await asyncio.sleep(REFRESH_PERIOD)


async def haul_yields_in_system(system : str, max_haulers : int):
    """ Periodically checks for excavators with full holds. If any are found, sends available haulers to collect & sell the mined goods. """
    
    controller = BASE_CONTROLLER_ID + '-HAULERS-' + system
    priority = BASE_PRIO_HAULERS
    REFRESH_PERIOD = 9

    # TODO account for max haulers!

    # Bookkeeping
    ts_start = int(time.time())
    fleet = dict()
    marked_drones = set()
    wp_miners = io.read_dict("SELECT symbol FROM 'nav.WAYPOINTS' WHERE type = \"ENGINEERED_ASTEROID\"")[0]['symbol']
    wp_siphon = io.read_dict("SELECT symbol FROM 'nav.WAYPOINTS' WHERE type = \"GAS_GIANT\"")[0]['symbol']

    # Every refresh
    while True:

        # TODO add better CLI logging for this controller
        # Check fleet & release finished haulers
        for s in get_finished_ships(fleet):
            print(f"[INFO] [{time.strftime('%H:%M:%S')}] {s} finished delivering mined goods.")
            fleet_resource_manager.release_ship(s)
            del fleet[s]

        if len(fleet) >= max_haulers:
            print(f"[INFO] {controller} is at fleet capacity ({len(fleet)} drones). Standing by.")
            await asyncio.sleep(REFRESH_PERIOD)
            continue

        # Check both extraction points      

        # Get all miners in orbit around the engineered asteroid with at least 60% cargo
        miners = get_full_excavators_at_wp(wp_miners, cargo_pct=0.85)
        miners = set(miners) - marked_drones

        # Get candidate haulers sorted by distance
        candidates = get_closest_haulers_to_wp(wp_miners, priority, controller)
        max_candidates = min(max_haulers - len(fleet), len(candidates))
        candidates = candidates[:max_candidates]

        # Try to service them
        miners_serviced = dispatch_haulers(candidates, miners, fleet, priority, controller)

        # Do the same thing for the siphon drones
        siphoners = get_full_excavators_at_wp(wp_siphon, cargo_pct=0.85)
        siphoners = set(siphoners) - marked_drones
        candidates = get_closest_haulers_to_wp(wp_siphon, priority, controller)
        max_candidates = min(max_haulers - len(fleet), len(candidates))
        candidates = candidates[:max_candidates]
        siphoners_serviced = dispatch_haulers(candidates, siphoners, fleet, priority, controller)

        # Update which miners are being serviced based on the fleet's targets
        marked_drones = list()
        for s in fleet:
            marked_drones.extend(fleet[s]['targets'])
        marked_drones = set(marked_drones)

        await asyncio.sleep(REFRESH_PERIOD)