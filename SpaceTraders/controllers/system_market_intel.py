"""
    System Market Intel Probe Controller

    Functions that enable a controller to automatically use available probes in a system to smartly refresh trade good data in that system.
"""
import asyncio, random
from SpaceTraders import io, fleet_resource_manager, scripts, F_nav


### GLOBALS ###
BASE_PRIO_MGR_PROBES = 100
BASE_CONTROLLER_ID = "PROBE-MANAGER"


### TEMP ###
async def _dummy_update_market(ship, market):
    fleet_resource_manager.set_ship_blocked_status(ship, True)
    print(f"{ship} is en-route to {market}.")
    if random.random() < 0.7:
        # Faster task
        await asyncio.sleep(0.1 + (random.randint(10, 100) / 100.0))
    else:
        # Slower task
        await asyncio.sleep(1 + (random.randint(200, 400) / 100.0))
    success = random.random() < 0.85
    if success:
        print(f"{ship} refreshed market at {market}.")
    else:
        print(f"{ship} failed to refresh market at {market}.")
    fleet_resource_manager.set_ship_blocked_status(ship, False)
    return success    


### HELPERS ###
def get_finished_ships(fleet):
    """ Returns a list of ship names that have finished their tasks. """
    return [s for s in fleet.keys() if fleet[s]['task'] is not None and fleet[s]['task'].done()]

def find_closest_drones(candidates : list, market : str):
    """ Returns candidate list ordered by distance to market. First in list is closest drone. """
    return sorted(candidates, key=lambda c : F_nav.wp_distance(market, F_nav.get_ship_waypoint(c)))

def assign_probe_to_market(candidates : list, fleet : dict, market : str, controller : str, priority : int):
    """ Finds the most suitable drone & sends it to the market for refresh. """
    # Find best candidate
    # TODO: make this consider distance to market etc
    if len(candidates) < 1: return False
    probe = find_closest_drones(candidates, market)[0]
    acquired = fleet_resource_manager.request_ship(probe, controller, priority)
    if acquired:
        fleet[probe] = {
            "market": market,
            "task": asyncio.create_task(scripts.update_market(probe, market))
        }
        return True
    return False


async def dispatch_satellites(system : str, market_order : list, fleet : dict, controller : str, priority : int):
    """ Dispatches drones to all markets in the queue. Dynamically updates fleet when it detects finished tasks. """

    # Dispatch drones to all markets
    being_handled = [s['market'] for s in fleet.values() if s['market'] is not None]
    blocked = False
    while len(market_order) > 0:

        m_ix = 0
        market = market_order[m_ix]

        # If a ship in the fleet is already on this market/task, skip it this dispatch
        if market in being_handled:
            market_order.pop(m_ix)
            continue

        # Check all available probes
        probes = fleet_resource_manager.get_available_ships_in_systems([system], ship_role="SATELLITE", prio=priority, controller=controller)
        if len(probes) == 0:
            print(f"[INFO] {controller} found no available ships.")

        # Dispatch most applicable available drone
        # TODO: use distance to market
        if market not in being_handled:
            candidates = [s for s in probes if s not in fleet]
            assigned = assign_probe_to_market(candidates, fleet, market, controller, priority)
            if assigned:
                being_handled.append(market)
                market_order.pop(0)      
            else:  
                blocked = True # Dispatcher can't assign any more ships to this task
                
        # Release ships that are finished each iteration, so the list of available ships remains up to date
        finished_ships = get_finished_ships(fleet)
        for p in finished_ships:
            s = fleet[p]

            result = await s['task']

            if result is True:
                being_handled.remove(s['market'])
            else:
                print(f"[INFO] {controller} is reporting one failed refresh from {p}.")
            
            # Release the ship only if we're not blocked; else this ship might get reassigned to the blocking market soon
            if not blocked:
                fleet_resource_manager.release_ship(p)
                fleet.pop(p)

        # If blocked (no resources), dispatch has effectively failed. Defer to controller.
        if blocked:
            break

    return len(market_order) == 0

def _query_markets(q : str):
    return [r[0] for r in io.read_list(q)]

def get_all_markets_by_freshness(system : str):
    """ Returns list of all markets in system, sorted by ascending tradegood data freshness. """
    q_all_markets = f"""
        with market_update_times as (
        select
            marketSymbol,
            min(ts_created) as ts_last_update,
            datetime(min(ts_created), 'unixepoch', 'localtime') as last_update
        from tradegoods_current
        group by marketSymbol
        )

        select
            distinct wp.symbol
        from 'nav.WAYPOINTS' wp

        inner join 'nav.TRAITS' t
        on wp.symbol = t.waypointSymbol
        and t.symbol = "MARKETPLACE"

        left join market_update_times mu
        on mu.marketSymbol = wp.symbol

        where wp.systemSymbol = "{system}"

        order by mu.ts_last_update asc
        """
    return _query_markets(q_all_markets)

def get_non_fuel_markets_by_freshness(system : str, time_delta : int):
    """ Returns list of all markets that sell more than just fuel and are outdated by more than time_delta seconds, ordered by ascending data freshness."""
    q_exclude_fuel_only = f"""
        select
            distinct marketSymbol
        from tradegoods_current
        where symbol <> "FUEL"
        and ts_created < (strftime('%s', 'now') - {time_delta})
        and marketSymbol like "{system}-%"
        order by ts_created asc
    """
    return _query_markets(q_exclude_fuel_only)

def get_import_export_markets_by_freshness(system : str, time_delta : int):
    """ Returns list of all markets that both import and export goods and are outdated by more than time_delta seconds, ordered by ascending data freshness. """
    q_exclude_exchanges = f"""
        select
            distinct marketSymbol
        from tradegoods_current
        group by marketSymbol
        having sum(type = "IMPORT") > 0
           and sum(type = "EXPORT") > 0
           and ts_created < (strftime('%s', 'now') - {time_delta})
        order by ts_created asc
    """
    return _query_markets(q_exclude_exchanges)

### MAIN ENTRY ###

async def maintain_tradegood_data(system : str, refresh_freq : int = -1, mode : str = "all"):
    """ Uses available probes to continuously update markets in system.
        Parameters:
            - refresh_freq [int] : Minimum time before a market becomes eligible for scanning again, in seconds
            - mode [str] : ('all', 'no_fuel', 'no_exchanges') Sets a filter for which markets to include/exclude. 
    """
    controller_id = BASE_CONTROLLER_ID + "-" + system
    fleet = dict()
    try:
        while True:
            # Check market queue
            market_queue = list()
            if mode == "all":
                market_queue = get_all_markets_by_freshness(system)
            elif mode == "no_fuel":
                market_queue = get_non_fuel_markets_by_freshness(system, refresh_freq)
            elif mode == "no_exchanges":
                market_queue = get_import_export_markets_by_freshness(system, refresh_freq)
            else:
                print(f"[ERROR] Incorrect mode passed to {controller_id}: {mode}. Deferring control.")
                return False

            print(f"[INFO] {controller_id} is targeting {len(market_queue)} markets.")
    
            # Dispatch ships
            cleared = await dispatch_satellites(system, market_queue, fleet, controller_id, BASE_PRIO_MGR_PROBES)

            # If the queue was cleared, we can wait until the next refresh window
            if cleared and refresh_freq > 0:
                print(f"[INFO] {controller_id} scheduled scans for all markets. Standing by.")
                await asyncio.sleep(refresh_freq)

            # If not cleared but a fleet is working, wait for the first ship to finish its task
            elif not cleared:
                fleet_tasks = [s["task"] for s in fleet.values() if s.get("task", None) is not None]
                print(f"[INFO] {controller_id} was unable to clear its queue. Waiting for {len(fleet_tasks)} ships to report back for reassignment.")
                done, ongoing = await asyncio.wait(fleet_tasks, return_when=asyncio.FIRST_COMPLETED)

            # Release finished ships & report
            failures, successes = 0, 0
            finished_ships = get_finished_ships(fleet)
            for p in finished_ships:
                s = fleet[p]

                result = await s['task']

                if result is False:
                    failures += 1
                else:
                    successes += 1
                
                # Release the ship only if we're not blocked; else this ship might get reassigned to the blocking market soon
                fleet_resource_manager.release_ship(p)
                fleet.pop(p)
            
            print(f"[INFO] {controller_id} succesfully refreshed {successes} markets.")
            if failures > 0: print(f"[INFO] {controller_id} is reporting {failures} failures to refresh.")

            await asyncio.sleep(1) # Brief breather for the main loop

    except KeyboardInterrupt as e:
        print(f"[INFO] User interruption caught. Releasing fleet and exiting gracefully.")
        for s in fleet:
            fleet_resource_manager.release_ship(s)
    except Exception as e:
        for s in fleet:
            fleet_resource_manager.release_ship(s)
        print(f"[ERROR] Unhandled exception in {controller_id}. Aborting.")
        print(e)
        raise e
        