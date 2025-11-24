"""
    System Trade Controller

    Functions that enable a controller to automatically use available haulers in a system to execute trades, using different strategies.
"""
import asyncio, random, time
from dataclasses import dataclass
from SpaceTraders import io, fleet_resource_manager, scripts, F_utils, F_nav, F_trade

### GLOBALS ###
BASE_PRIO_TRADERS = 300
BASE_CONTROLLER_ID = "TRADE-CONTROLLER"


### OBJECT CLASSES ###
@dataclass
class TaskTrade():

    tradeSymbol : str
    source      : str
    sink        : str
    units       : str

### GETTERS ###
def get_finished_ships(fleet):
    """ Returns a list of ship names that have finished their tasks. """
    return [s for s in fleet.keys() if fleet[s].get('task', None) is not None and fleet[s]['task'].done()]

def find_closest_hauler(candidates : list, market : str):
    """ Returns candidate list ordered by distance to market. First in list is closest. """
    return sorted(candidates, key=lambda c : F_nav.wp_distance(market, F_nav.get_ship_waypoint(c)))


### TEMP - DEBUG ###
async def execute_trade(ship, src, sink, goods):
    print(f"[DEBUG] {ship} would trade {goods} from {src} to {sink}.")
    await asyncio.sleep(random.randint(100, 300) / 100.0)
    print(f"[DEBUG] {ship} has finished its mock trade.")
    return True


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

def assign_hauler_to_trade(candidates : list, fleet : dict, trade : TaskTrade, controller : str, priority : int):
    """ Finds the most suitable drone & sends it to execute the trade. """
    # Find best candidate
    if len(candidates) < 1: return False
    probe = find_closest_hauler(candidates, trade.source)[0]
    acquired = fleet_resource_manager.request_ship(probe, controller, priority)
    if acquired:
        fleet[probe] = {
            "trade": trade,
            "task": asyncio.create_task(execute_trade(probe, trade.source, trade.sink, {trade.tradeSymbol: trade.units})),
            "time_start": int(time.time())
        } # TODO: switch back to scripts.execute_trade for actual effect
        return True
    return False


### TASK SELECTION ###
def get_greedy_trades(ship=None):
    """ Returns list of trades (tradeSymbol, source, sink, units, max_traders) ordered by their profitability. """
    # max_traders is calculated based on ROI - for every 30%, one extra trader is allowed to run the trade concurrently (since we assume the margin is large enough to guarantee profitability)
    ship_fuel = 300
    if ship is not None:
        ship_fuel = F_nav.get_fuel_capacity(ship)
    selection_query  = \
                        f"""
                        select
                            *
                            ,cast(sellPrice as float) / cast(purchasePrice as float) as ROI
                            ,round((((cast(sellPrice as float) / cast(purchasePrice as float)) - 1) * 100) / 30) as max_traders
                        from TRADE_SYSTEM_MARGINS
                        where 1=1
                            and source_volume >= 6 and sink_volume >= 6
                            and distance < {int(ship_fuel-1)}
                            and src_supply in ("ABUNDANT", "HIGH", "MODERATE")
                            and sink_supply in ("SCARCE", "LIMITED", "MODERATE")
                            and symbol not in ("FAB_MATS", "ADVANCED_CIRCUITRY", "QUANTUM_STABILIZERS")
                            and max_traders > 0
                            order by net_profit desc
                        """
    return io.read_dict(selection_query)


### MAIN ENTRY ###
async def trade_in_system(system : str, max_haulers : int, strategy : str = "greedy"):
    """ Tries to acquire a fleet of haulers (up to given maximum) and has them execute trades following the given strategy. 
        Strategies:
            - greedy (default) : Traders will chase after highest profit margins
    """
    
    # Bookkeeping
    priority = BASE_PRIO_TRADERS
    controller = BASE_CONTROLLER_ID + '-' + system
    refresh_period = 15
    ongoing_trades = dict() # {item : {src : {sink : n_ongoing}}}
    fleet = dict()

    # Main loop
    while True:

        # Check trades according to strategy
        trades = list()
        if strategy == "greedy":
            trades = get_greedy_trades()

        # Release finished ships
        for s in get_finished_ships(fleet):
            # Mark the ongoing trade as finished
            n_ongoing = ongoing_trades.get(t['symbol'], dict()).get(t['source'], dict()).get(t['sink'], 0)
            if n_ongoing < 2:
                # This ship was the last trader on this route, so delete it from the ongoing list
                if n_ongoing != 0:
                    del ongoing_trades[t['symbol']][t['source']][t['sink']]
            else:
                # There are still active traders on this route, just decrement the counter
                ongoing_trades[trade.tradeSymbol][trade.source][trade.sink] = n_ongoing - 1

            # Release ship
            fleet_resource_manager.release_ship(s)
            del fleet[s]


        # Try to clear all trades
        while len(trades) > 0:
            t = trades[0]

            n_ongoing = ongoing_trades.get(t['symbol'], dict()).get(t['source'], dict()).get(t['sink'], 0)
            cur_haulers = len(fleet)
            
            # If trade not already being executed (by max haulers):
            # And If not yet at max haulers
            if n_ongoing < t['max_traders'] and cur_haulers < max_haulers:
                # Check the list of available haulers
                haulers = fleet_resource_manager.get_available_ships_in_systems([system], ship_role="HAULER", prio=priority, controller=controller)
                haulers = [h for h in haulers if h not in fleet]

                # Send closest hauler to source to execute the trade
                # Mark trade as being executed by one extra hauler
                trade = TaskTrade(t['symbol'], t['source'], t['sink'], t['trade_volume'])
                success = assign_hauler_to_trade(haulers, fleet, trade, controller, priority)

                if success:
                    # Mark trade as ongoing (or having one extra trader)
                    if n_ongoing == 0:
                        ongoing_trades[trade.tradeSymbol] = {trade.source: {trade.sink: 1}}
                    else:
                        ongoing_trades[trade.tradeSymbol][trade.source][trade.sink] = n_ongoing + 1
                    # Pop from the queue if all available trades assigned to this trade
                    if ongoing_trades[trade.tradeSymbol][trade.source][trade.sink] >= t['max_traders']:
                        trades.pop(0)
                else:
                    # Something is blocking the queue from being consumed -- probably a lack of available trades
                    print(f"[INFO] {controller} was unable to clear all trades. Currently waiting for {len(fleet)} traders to report back.")
                    break

                # Small random delay to allow traders to spread out temporally
                await asyncio.sleep(random.randint(20, 300) / 1000.0) 
        
        # Politely wait until the next iteration
        await asyncio.sleep(refresh_period)