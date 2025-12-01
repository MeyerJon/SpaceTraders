"""
    System Excavator Controller

    Functions that enable a controller to automatically use available probes in a system to siphon resources from the local Gas Giant or mine resources from the local Engineered Asteroid.
"""
import asyncio, random, time
from SpaceTraders import io, fleet_resource_manager, scripts, F_utils, F_nav, F_extract, F_trade

### GLOBALS ###
BASE_PRIO_EXTRACTOR = 100
BASE_CONTROLLER_ID  = "EXTRACTION-CONTROLLER"


### GETTERS ###
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
    """ Returns list of available haulers sorted by ascending distance to waypoint. """
    q = f"""
        select
            reg.shipSymbol
            ,reg.role
            ,nav.waypointSymbol
            ,dists.dist
        from 'ship.NAV' nav

        inner join 'control.SHIP_LOCKS' ctrl
        on ctrl.shipSymbol = reg.shipSymbol
        and blocked = 0

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
    yields = io.read_dict(q_yield)
    if len(yields) > 0:
        yields = yields[0]['total']
    else:
        yields = 0
    return yields


### HELPERS ###
async def siphon_goods(ship: str, waypoint : str, goods : list = None):
    """ Siphon from a waypoint until cargo hold is filled, keeping only the desired goods.
    """
    refresh_period = 10 # Time between checks if ship gets locked (full cargo)

    # Navigate to waypoint if not there
    if F_nav.get_ship_waypoint(ship) != waypoint:
        if not await scripts.navigate(ship, waypoint):
            return False
        await scripts.await_navigation(ship)

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
    if F_nav.get_ship_waypoint(ship) != waypoint:
        if not await scripts.navigate(ship, waypoint):
            return False
        await scripts.await_navigation(ship)

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
            #print(f'[WARNING] Ship {ship} failed to siphon. Retrying in {cd} seconds.')
            await asyncio.sleep(cd)



### MAIN ENTRY ###
async def extract_in_system(system):
    """ Acquires all excavators in the system and sends them to the appropriate extraction site for indefinite work. """

    # TODO: Consider incorporating surveys into this controller

    # Since there is a risk of over-mining, the actual max excavators is capped
    MAX_MINERS     = 8
    MAX_SIPHONERS  = 10 
    REFRESH_PERIOD = 15  # How often the controller updates its fleet

    # Bookkeeping
    controller = BASE_CONTROLLER_ID + '-EXCAVATORS-' + system
    priority = BASE_PRIO_EXTRACTOR
    fleet_miners    = dict()
    fleet_siphoners = dict()
    ts_start = int(time.time())

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
                    fleet_resource_manager.lock_ship(miner, controller, priority)
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
                    fleet_resource_manager.lock_ship(siphoner, controller, priority)
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

        if (int(time.time()) - ts_start) % 60:
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

        await asyncio.sleep(REFRESH_PERIOD)


async def haul_yields_in_system(system : str, max_haulers : int):
    """ Periodically checks for excavators with full holds. If any are found, sends available haulers to collect & sell the mined goods. """
    
    # Bookkeeping
    REFRESH_PERIOD = 10
    fleet = dict()
    marked_drones = set()

    # Every refresh
    while True:

        # Check both extraction points

        # Miners
        wp_miners = io.read_dict("SELECT symbol FROM 'nav.WAYPOINTS' WHERE type = \"ENGINEERED_ASTEROID\"")[0]['symbol']

        # Get all miners in orbit around the engineered asteroid
        # Filter out those with >60% filled cargo
        miners = get_full_excavators_at_wp(wp_miners, cargo_pct=0.6)

        # TODO come up with a way to have one hauler service multiple drones in one order
        # Try to service them
        # For each candidate miner that's not in marked drones:
            # Find the closest available hauler
            # Try to acquire it
                # If acquired: add drone to marked drones & add hauler to fleet

        await asyncio.sleep(REFRESH_PERIOD)