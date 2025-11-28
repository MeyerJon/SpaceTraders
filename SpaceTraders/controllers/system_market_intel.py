"""
    System Market Intel Probe Controller

    Functions that enable a controller to automatically use available probes in a system to smartly refresh trade good data in that system.
"""
import asyncio, random, time
from SpaceTraders import io, fleet_resource_manager, scripts, F_nav


### GLOBALS ###
BASE_PRIO_MGR_PROBES = 100
BASE_CONTROLLER_ID = "PROBE-MANAGER"


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
            "task": asyncio.create_task(scripts.update_market(probe, market)),
            "time_start": int(time.time())
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

def get_all_markets_by_freshness(system : str, time_delta : int, **kwargs):
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
        and (ts_last_update is null or ts_last_update < (strftime('%s', 'now') - {time_delta}))

        order by mu.ts_last_update asc
        """
    return _query_markets(q_all_markets)

def get_non_fuel_markets_by_freshness(system : str, time_delta : int, **kwargs):
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

def get_import_export_markets_by_freshness(system : str, time_delta : int, **kwargs):
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

def get_prioritised_markets(market_selector, **kwargs) -> list[str]:
    """ Returns the candidates selected by market_selector function, sorted by priority. 
        Factors in how outdated the data is, as well as how far away the market is from currently available probes.
        Slightly prefers closer markets to improve refresh throughput.
    """
    # TODO : Make this independent of TRADEGOODS_CURRENT. Currently, this can't bootstrap the tradegoods table because it has no market freshness data 
    candidates      = market_selector(**kwargs)
    available_ships = fleet_resource_manager.get_available_ships_in_systems([kwargs["system"]], ship_role="SATELLITE", prio=kwargs.get("priority", BASE_PRIO_MGR_PROBES), controller=kwargs.get("controller", None))
    q_prio_markets = f"""
            with ship_dists as (
            -- Start from ship-to-market distances
                select
                src, dst, dist, symbol
                from WP_DISTANCES wp_dists
                
                inner join 'ship.NAV' nav
                on nav.waypointSymbol = wp_dists.src
                and nav.symbol in ({', '.join(f'"{s}"' for s in available_ships)})
            )

            ,market_update_times as (
            -- Add update times for markets
            -- This selects from TRADEGOODS_CURRENT, so it might not have all info for all markets (or any info at all)
                select
                    distinct marketSymbol,
                    ts_created,
                    strftime('%s', 'now') - ts_created as time_since_update,
                    datetime(ts_created, 'unixepoch', 'localtime') as last_update
                from tradegoods_current
                group by marketSymbol
                having marketSymbol in ({', '.join(f'"{c}"' for c in candidates)})
            )

            ,scored_markets as (
                select
                    ship_dists.dst as market
                    -- Score weighs 'outdatedness' and distance almost equally, but prefers closer waypoints, and much prefers current waypoint
                    ,(
                    ship_dists.dist 
                    + (ship_dists.dist * (coalesce((select max(time_since_update) from market_update_times), 1) - coalesce(time_since_update, 0))) 
                    + iif(ship_dists.dst = next_mkt.marketSymbol, -1, 0)
                    ) as score
                
                from ship_dists
                
                -- Add market locations & update times when available
                left join market_update_times next_mkt
                on ship_dists.dst = next_mkt.marketSymbol
                
                where ship_dists.dst in ({', '.join(f'"{c}"' for c in candidates)})
            )

            select
                distinct market, min(score)
            from scored_markets
            group by market
            order by score asc
    """
    return [r[0] for r in io.read_list(q_prio_markets)]


### MAIN ENTRY ###

async def maintain_tradegood_data(system : str, refresh_freq : int = -1, mode : str = "all"):
    """ Uses available probes to continuously update markets in system.
        Parameters:
            - refresh_freq [int] : Minimum time before a market becomes eligible for scanning again, in seconds
            - mode [str] : ('all', 'no_fuel', 'no_exchanges') Sets a filter for which markets to include/exclude. 
    """
    # Preconditions
    modes = ["all", "no_fuel", "no_exchanges"]
    selector_func = None
    if mode not in modes:
        print(f"[ERROR] Incorrect mode passed to {controller_id}: {mode}. Deferring control.")
        return False
    else:
        funcs = [get_all_markets_by_freshness, get_non_fuel_markets_by_freshness, get_import_export_markets_by_freshness]
        selector_func = funcs[modes.index(mode)]

    # Bookkeeping
    controller_id = BASE_CONTROLLER_ID + "-" + system
    fleet = dict()
    slowest_completion = -1
    try:
        while True:
            # Check market queue
            market_queue = get_prioritised_markets(market_selector=selector_func, 
                                                   system=system, 
                                                   priority=BASE_PRIO_MGR_PROBES, 
                                                   controller=controller_id, 
                                                   time_delta = refresh_freq)

            if len(market_queue) >= len(fleet): print(f"[INFO] [{time.strftime('%H:%M:%S')}] {controller_id} is targeting {len(market_queue)} markets.")
    
            # Dispatch ships
            cleared = await dispatch_satellites(system, market_queue, fleet, controller_id, BASE_PRIO_MGR_PROBES)

            # If the queue was cleared, we can wait until the next refresh window
            if cleared and refresh_freq > 0:
                #print(f"[INFO] {controller_id} scheduled scans for all markets. Standing by.")
                await asyncio.sleep(15) # Sleep for some time before checking the queue again. TODO: Once robust, this can be increased to match refresh rate
            
            elif not cleared:
                fleet_tasks = [s["task"] for s in fleet.values() if s.get("task", None) is not None]
                if len(fleet_tasks) > 0:
                # If not cleared but a fleet is working, wait for the first ship to finish its task
                    print(f"[INFO] {controller_id} was unable to clear its queue. Waiting for {len(fleet_tasks)} ships to report back for reassignment.")
                    done, ongoing = await asyncio.wait(fleet_tasks, return_when=asyncio.FIRST_COMPLETED)
                else:
                # If not cleared and no ships were available, wait an arbitrary time before retrying
                    print(f"[INFO] {controller_id} is waiting to acquire a fleet.")
                    await asyncio.sleep(2)

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
                    time_taken = time.time() - s['time_start']
                    if time_taken > slowest_completion:
                        print(f"[INFO] {controller_id} is reporting a new slowest recon from {p}: {time_taken:.1f} seconds.")
                        slowest_completion = time_taken
                
                # Release the ship only if we're not blocked; else this ship might get reassigned to the blocking market soon
                fleet_resource_manager.release_ship(p)
                fleet.pop(p)
            
            if successes > 0: print(f"[INFO] [{time.strftime('%H:%M:%S')}] {controller_id} succesfully refreshed {successes} markets.")
            if failures > 0: print(f"[INFO] [{time.strftime('%H:%M:%S')}] {controller_id} is reporting {failures} failures to refresh.")

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
        