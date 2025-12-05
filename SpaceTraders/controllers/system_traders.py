"""
    System Trade Controller

    Functions that enable a controller to automatically use available haulers in a system to execute trades, using different strategies.
"""
import asyncio, random, time
from dataclasses import dataclass
from datetime import datetime, timezone
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
    controller  : str
    repeats     : int

### GETTERS ###
def get_finished_ships(fleet):
    """ Returns a list of ship names that have finished their tasks. """
    return [s for s in fleet.keys() if fleet[s].get('task', None) is not None and fleet[s]['task'].done()]

def find_closest_hauler(candidates : list, market : str):
    """ Returns candidate list ordered by distance to market. First in list is closest. """
    return sorted(candidates, key=lambda c : F_nav.wp_distance(market, F_nav.get_ship_waypoint(c)))

def get_ship_transaction_total(ship : str, ts_start : int, ts_end : int):
    """ Gets total profit/loss for a ship from transactions during the given time window. """
    q = f"""
        with mutations as (
            select
                *
                ,case type
                    when "PURCHASE" then -totalPrice 
                    when "SELL" then totalPrice    
                end as mutation
            from transactions
            where shipSymbol = "{ship}"
            and ts_created >= {ts_start}
            and ts_created <= {ts_end}
        )

        select
            sum(mutation)
        from mutations
    """
    rows = io.read_list(q)
    if rows and len(rows) > 0 and rows[0][0] is not None:
        return rows[0][0]
    else:
        return 0

def get_ship_trade_profit_since(ship : str, ts_start : int, ts_end : int = None):
    """ Returns the total profit a ship has made actually trading in the given time window. Timestamps are in unix format and do not account for server-client time offset. Fix your timestamps before calling this. """
    # Substract 1h because of the timezone difference with the server
    #ts_start = ts_start-3600
    #if ts_end: ts_end = ts_end-3600
    query = f"""
        select
            sum(profit)
        from TRADES
        where ship = "{ship}"
        and ts_start >= {ts_start}
    """
    if ts_end: query += f"\nand ts_end <= {ts_end}"
    try:
        result = io.read_list(query)
        if result:
            return result[0][0]
        else:
            return 0
    except Exception as e:
        print(f"[ERROR] Unhandled exception while calculating total trade profit for {ship} since {ts_start}.")
        io.log_exception(e)
        return None
    
def get_controller_trade_profit_since(controller : str, ts_start : int, ts_end : int = None):
    """ See get_ship_trade_profit_since, but for a controller instead of a ship. """
    query = f"""
        select
            sum(profit)
        from TRADES
        where controller = "{controller}"
        and ts_start >= {ts_start}
    """
    if ts_end: query += f"\nand ts_end <= {ts_end}"
    try:
        result = io.read_list(query)
        if result:
            return result[0][0]
        else:
            return 0
    except Exception as e:
        print(f"[ERROR] Unhandled exception while calculating total trade profit for {controller} since {ts_start}.")
        io.log_exception(e)
        return None

def get_projected_trade_profit(tradeSymbol : str, units : int, source : str, sink : str):
    """ Returns the expected profit for trading a given volume of goods between source and sink, based on current market data. """
    q = f"""
        select
            symbol
            ,({units}*sellPrice) - ({units}*purchasePrice) as projected_profit
        from trade_system_margins
        where symbol = "{tradeSymbol}" 
        and source like "{source}" 
        and sink like "{sink}"
    """
    rows = io.read_dict(q)
    if rows and len(rows) > 0:
        return rows[0]['projected_profit'] or 0
    else:
        print(f"[WARNING] Could not get projected profit for {units} {tradeSymbol} from {source} to {sink}.")
        return None


### TEMP - DEBUG ###
async def execute_trade(ship, src, sink, goods):
    print(f"[DEBUG] {ship} would trade {goods} from {src} to {sink}.")
    # Since the controller refreshes tasks every 15 seconds, some of these should take longer (to fully demonstrate functionality)
    await asyncio.sleep(random.random())
    if random.random() < 0.66:
        dt = random.randint(100, 300) / 100.0
        print(f"[DEBUG] {ship} is executing its trade ({dt:.2f} seconds).")
        await asyncio.sleep(dt)
    else:
        print(f"[DEBUG] {ship} is executing a long trade (17 seconds).")
        await asyncio.sleep(16.5)
    print(f"[DEBUG] {ship} has finished its mock trade.")
    return True


### HELPERS ###
def _log_trade(ship : str, trade : TaskTrade, profit : int, ts_start : int, ts_end : int):
    """ Writes trade info to database. """
    # Reminder that we're 1h off the server time, so that's corrected here for the trade calculation
    data = {"ship": ship,
            "controller": trade.controller,
            "tradeSymbol": trade.tradeSymbol,
            "source": trade.source,
            "sink": trade.sink,
            "units": trade.units * trade.repeats,
            "ts_start": ts_start,
            "ts_end": ts_end,
            "profit": profit
            }
    io.write_data('TRADES', data)

async def execute_trade(ship : str, trade : TaskTrade):
    """ Task implementation: Handles the trade end-to-end, including recovery & persistence. """

    fleet_resource_manager.set_ship_blocked_status(ship, blocked=True)

    # Sanity check - ensure that the ship isn't in transit
    await scripts.await_navigation(ship)

    # Sanity check - Ship has an empty hold
    ship_cargo = F_trade.get_ship_cargo(ship)
    cargo_held = ship_cargo['units']
    if cargo_held > 0:
        print(f"[INFO] {ship} is trying to trade with a non-empty hold. Clearing cargo first.")
        await scripts.clear_cargo(ship)

    # Move to the market first before actually starting the trade
    # This allows the ship to check price changes between now and when it would actually buy the goods
    if not (await scripts.navigate(ship, trade.source)):
        print(f"[WARNING] {ship} couldn't execute trade: unable to reach source market {trade.source}.")
        fleet_resource_manager.set_ship_blocked_status(ship, blocked=False)
        return False

    # Actually execute the trade
    ts_start = int(time.time())

    goods = {trade.tradeSymbol: min(trade.units * trade.repeats, ship_cargo['capacity'])}
    success = True

    # Before buying goods, check if the trade still makes sense given current knowledge. This is to avoid trade 'collisions' that go unnoticed by the controller
    projected_profit = get_projected_trade_profit(trade.tradeSymbol, units=goods[trade.tradeSymbol], source=trade.source, sink=trade.sink) or -1
    if projected_profit < 50:
        # This trade would lose money if executed right now, so it's aborted right away so the ship can be freed up
        print(f"[INFO] {ship} detected a losing trade order. Aborting trade.")
        success = False

    if success:
        buy = await scripts.buy_from_market(ship, trade.source, goods)
        if not buy:
            print(f"[ERROR] {ship} was unable to procure trade goods. Aborting trade.")
            success = False

    if success:
        sell = await scripts.sell_to_market(ship, trade.sink, goods)
        if not sell:
            print(f"[ERROR] {ship} was unable to offload trade goods. Aborting trade.")
            success = False

    # On success, report some statistics
    if success:
        ts_end = int(time.time())
        profit = get_ship_transaction_total(ship, ts_start, ts_end)
        if profit is not None:
            print(f"[INFO] {ship} finished trade. Total profit: {profit} credits.")       
            _log_trade(ship, trade, profit, ts_start, ts_end)
        else:
            print(f"[INFO] {ship} finished trade.")

    fleet_resource_manager.set_ship_blocked_status(ship, blocked=False)

    return success


def assign_hauler_to_trade(candidates : list, fleet : dict, trade : TaskTrade, controller : str, priority : int):
    """ Finds the most suitable drone & sends it to execute the trade. Returns assigned ship name on success, None otherwise. """
    # Find best candidate
    if len(candidates) < 1: return False
    ship = find_closest_hauler(candidates, trade.source)[0]
    acquired = fleet_resource_manager.request_ship(ship, controller, priority)
    if acquired:
        # Optimization: if the trade may be executed multiple times and this hauler has the capacity for it, order it to do multiple trades
        # Here we calculate how often the trade can actually be executed by this ship in one trip
        n_repeats = max(1, min(F_trade.get_ship_cargo(ship)['capacity'] // trade.units, trade.repeats))
        trade.repeats = n_repeats

        fleet[ship] = {
            "trade": trade,
            "task": asyncio.create_task(execute_trade(ship, trade)),
            "time_start": int(time.time())
        }
        return ship
    return None


### TASK SELECTION ###
def get_greedy_trades(ship=None):
    """ Returns list of trades (tradeSymbol, source, sink, units, max_traders) ordered by their profitability. """
    # max_traders is calculated based on ROI - for every 30%, one extra trader is allowed to run the trade concurrently (since we assume the margin is large enough to guarantee profitability)
    ship_fuel = 600
    if ship is not None:
        ship_fuel = F_nav.get_fuel_capacity(ship)
    selection_query  = \
                        f"""
                        select
                            *
                            ,cast(sellPrice as float) / cast(purchasePrice as float) as ROI
                            ,case
                                when src_supply in ("ABUNDANT", "HIGH") and sink_supply in ("LIMITED", "SCARCE")
                                    then ceil((((cast(sellPrice as float) / cast(purchasePrice as float)) - 1) * 100) / 49)
                                when src_supply in ("MODERATE") or sink_supply in ("MODERATE")
                                    then ceil((((cast(sellPrice as float) / cast(purchasePrice as float)) - 1) * 100) / 51)
                                else
                                    1
                            end as max_traders
                            ,net_profit / distance as profit_over_distance
                        from TRADE_SYSTEM_MARGINS
                        where 1=1
                            and source_volume >= 6 and sink_volume >= 6
                            and distance < {int((ship_fuel-1) * 1.5)}
                            and src_supply in ("ABUNDANT", "HIGH", "MODERATE", "LIMITED")
                            and sink_supply in ("SCARCE", "LIMITED", "MODERATE")
                            and symbol not in ("FAB_MATS", "ADVANCED_CIRCUITRY", "QUANTUM_STABILIZERS", "FUEL")
                            and net_profit >= 500
                            and max_traders > 0
                            order by profit_over_distance desc
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
    refresh_period = 12
    ongoing_trades = dict() # {item : {src : {sink : n_ongoing}}}
    fleet = dict()
    time_start = time.time()

    # Main loop
    cycle_profit = {'current': 0, 'previous': 0}
    while True:
        cycle_profit['current'] = 0

        # Check trades according to strategy
        trades = list()
        if strategy == "greedy":
            trades = get_greedy_trades()

        # Release finished ships
        for s in get_finished_ships(fleet):
            # Mark the ongoing trade as finished
            finished_trade = fleet[s]['trade']
            n_ongoing = ongoing_trades.get(finished_trade.tradeSymbol, dict()).get(finished_trade.source, dict()).get(finished_trade.sink, 0)
            if n_ongoing < 2:
                # This ship was the last trader on this route, so delete it from the ongoing list
                if n_ongoing != 0:
                    del ongoing_trades[finished_trade.tradeSymbol][finished_trade.source][finished_trade.sink]
            else:
                # There are still active traders on this route, just decrement the counter
                ongoing_trades[finished_trade.tradeSymbol][finished_trade.source][finished_trade.sink] = n_ongoing - 1

            # Record the ship's profitability
            ship_profit = get_ship_trade_profit_since(s, fleet[s]['time_start']-3600)
            cycle_profit['current'] += ship_profit or 0

            # Release ship
            fleet_resource_manager.release_ship(s)
            del fleet[s]


        # Try to clear all trades
        t_ix = 0 # Start at the beginning of the queue
        while len(trades) > 0 and t_ix < len(trades):

            # First of all, check if our fleet is at capacity
            if len(fleet) >= max_haulers:
                #print(f"[INFO] {controller} is executing {len(fleet)} trades.")
                break           
            
            t = trades[t_ix]
            n_ongoing = ongoing_trades.get(t['symbol'], dict()).get(t['source'], dict()).get(t['sink'], 0)
            
            # If trade not already being executed (by max haulers):
            if n_ongoing < t['max_traders']:
                # Check the list of available haulers
                haulers = fleet_resource_manager.get_available_ships_in_systems([system], ship_role="HAULER", prio=priority, controller=controller)
                haulers = [h for h in haulers if h not in fleet]

                # Send closest hauler to source to execute the trade
                trade = TaskTrade(t['symbol'], t['source'], t['sink'], t['trade_volume'], controller, max(1, t['max_traders'] - n_ongoing)) # Will try to have one trader cover all allowed iterations of this trade
                assigned_ship = assign_hauler_to_trade(haulers, fleet, trade, controller, priority)

                if assigned_ship:
                    # Mark trade as ongoing (or having one extra trader)
                    n_covered = fleet[assigned_ship]['trade'].repeats
                    if n_ongoing == 0:
                        ongoing_trades[trade.tradeSymbol] = {trade.source: {trade.sink: n_covered}}
                    else:
                        ongoing_trades[trade.tradeSymbol][trade.source][trade.sink] = n_ongoing + n_covered
                    # Pop from the queue if all available trades assigned to this trade
                    if ongoing_trades[trade.tradeSymbol][trade.source][trade.sink] >= t['max_traders']:
                        trades.pop(0)
                else:
                    # Something is blocking the queue from being consumed -- probably a lack of available trades
                    print(f"[INFO] {controller} was unable to clear all trades. Currently waiting for {len(fleet)} traders to report back.")
                    break

                # Small random delay to allow traders to spread out temporally
                await asyncio.sleep(random.randint(20, 300) / 1000.0) 
            
            elif t_ix < len(trades)-1:
                # This trade is already being served by max haulers, so move down the queue if possible
                t_ix += 1
                continue

            else:
                # None of the trades in the queue can be served, so stop trying
                break
        
        # Profit report
        if cycle_profit['current'] != 0:
            job_profit = get_controller_trade_profit_since(controller, time_start-3600) # Adjust ts_start for 1h time difference
            total_profit   = get_controller_trade_profit_since(controller, 0)
            rep = f"[PROFIT REPORT - {controller}] [{time.strftime('%H:%M:%S')}]\n"
            rep += f"       HOURLY PROFIT :  {job_profit / ((time.time() - time_start) / 3600):.0f} cr/h.\n"
            rep += f"        TOTAL PROFIT :  {total_profit} cr.\n"
            rep += f"          JOB PROFIT :  {job_profit} cr. "

            if False and len(fleet) > 0:
                rep += "\n\t  <FLEET>"
                for s in fleet:
                    s_profit = get_ship_trade_profit_since(s, time_start)
                    rep += f"\n\t\t     {s} : {s_profit} cr."
            rep += f"\n\t Active since {F_utils.unix_to_ts(time_start)}"
            print(rep)
 
        # Politely wait until the next iteration
        await asyncio.sleep(refresh_period)