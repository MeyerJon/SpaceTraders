"""
    SpaceTraders - Trade Functions

    This file contains functionality for:
        - Getting data related to trade/markets (trade goods, market info ...)
        - Taking trade-related actions (buying/selling cargo, ...)

    All data is passed in json/dict format, unless specified otherwise.
    Functions that trigger actions should return a boolean indicating success unless specified otherwise.
"""
import SpaceTraders as ST
from SpaceTraders import io, F_utils, F_nav
import pandas as pd
import math, datetime, time

### PERSISTENCE ###
def _log_trade(market_transaction : dict):
    """ Records the given transaction to the database. """
    return io.write_data('TRADES', market_transaction)

### GETTERS ###
def get_ship_cargo(ship):
    cargo_r = ST.get_request(f'/my/ships/{ship}/cargo')
    if cargo_r.status_code != 200:
        print(f'[ERROR] Failed to get cargo for ship {ship}.')
        print(f' [INFO]', cargo_r.json())
    return cargo_r.json()['data']    

def get_shipyard_info(waypoint, verbose=True):
    """ Returns shipyard info from given waypoint if available. """
    sys = F_utils.system_from_wp(waypoint)
    r = ST.get_request(f'/systems/{sys}/waypoints/{waypoint}/shipyard')
    if r.status_code != 200:
        if verbose:
            print(f'[ERROR] Unable to fetch shipyard info for {waypoint}.')
            print(f' [INFO]', r.json())
        return False
    return r.json()['data']

def get_market_info(waypoint):
    """ Returns market info from given waypoint if available. """
    sys = F_utils.system_from_wp(waypoint)
    r = ST.get_request(f'/systems/{sys}/waypoints/{waypoint}/market')
    if r.status_code != 200:
        print(f'[ERROR] Unable to fetch market info for {waypoint}.')
        print(f' [INFO]', r.json())
        return False
    return r.json()['data']

def get_trade_good(good, market):
    """ Returns trade good info for a market if known. """
    # Try getting it from the database
    rows = io.read_dict(f'SELECT symbol, type, tradeVolume, supply, activity, purchasePrice, sellPrice FROM TRADEGOODS_CURRENT WHERE symbol = "{good}" and marketSymbol = "{market}"')
    if rows: 
        return rows[0]

    # If that fails, try getting it using the API
    market_info = get_market_info(market)
    if not market_info:
        return False
    if 'tradeGoods' not in market_info:
        print(f"[ERROR] Can't get trade goods data for {market}. No visibility?")
        return False
    
    tg = list(filter(lambda t : t['symbol'] == good, market_info['tradeGoods']))
    if len(tg) == 0:
        print(f"[ERROR] Market {market} does not trade {good}. Can't fetch info.")
        return False

    return tg

def get_total_profit_from_trade(ship, source_market, sink_market, ts_start):
    query = """
            with trade_transactions as (
                select
                    *,
                    -1 * totalPrice as credit_mutation
                    from trades
                where 1=1
                and shipSymbol = :ship
                and waypointSymbol = :source_market
                and type = "PURCHASE"
                and timestamp >= :ts_start

                union all

                select
                    *,
                    totalPrice as credit_mutation
                from trades
                where 1=1
                and shipSymbol = :ship
                and waypointSymbol = :sink_market
                and type = "SELL"
                and timestamp >= :ts_start
                )
            select
            sum(credit_mutation) as total_profit
            from trade_transactions
            """
    try:
        result = io.read_list(query, {'ship': ship, 'source_market': source_market, 'sink_market': sink_market, 'ts_start': ts_start})
        if result:
            return result[0][0]
        else:
            return None
    except Exception as e:
        print(f"[ERROR] Unhandled exception while calculating trade profit for {ship}. Route data:")
        print(f"  params: {source_market=}, {sink_market=}, {ts_start=}")
        return None


### ACTIONS ###

def sell_cargo(ship, good, units, verbose=True):
    """ Sells the specified volume of a good. 
        Returns status [boolean] - True if sale successfully executed.
    """

    # Dock first
    F_nav.dock_ship(ship)

    # Sell
    r = ST.post_request(f'/my/ships/{ship}/sell', data={'symbol': good, 'units': units})
    if r.status_code != 201:
        print(f"[ERROR] Ship {ship} failed to sell ({units}) {good}.")
        print(f' [INFO]', r.json())
        return False
    # Log sale
    t = r.json()['data']['transaction']
    _log_trade(t)
    if verbose:
        print(f"[INFO] Ship {ship} sold {t['units']} {good} @ {t['pricePerUnit']} for a total of {t['totalPrice']} credits.")
    
    return True

def sell_goods(ship : str, goods : dict, verbose=True):
    """ Sells the specified goods. """

    # Dock first
    F_nav.dock_ship(ship)
    ship_wp = F_nav.get_ship_waypoint(ship)
    
    # For each good, check how many there are & sell
    all_sold = True
    cargo = get_ship_cargo(ship)
    for c in cargo['inventory']:
        if c['symbol'] in goods:
            # Limit the units per transaction to the trade volume
            tg = get_trade_good(c['symbol'], ship_wp)
            if not tg:
                print(f"[ERROR] {ship} could not fetch trade info for {c['symbol']} at market {ship_wp}.")
                continue
            
            # Sell all goods in increments of max trade volume
            max_u = min(tg['tradeVolume'], goods[c['symbol']])
            cur_u = c['units']
            while cur_u > 0:
                to_sell = min(cur_u, max_u)
                if sell_cargo(ship, c['symbol'], to_sell):
                    cur_u -= to_sell
                else:
                    print(f'[ERROR] Ship {ship} failed to sell {to_sell} {c["symbol"]}.')
                    all_sold = False

    return all_sold

def _purchase_cargo(ship, good, units, verbose=True):
    """ Purchases up to a certain number of a good from the current location. If units > trade volume, units are capped and this transaction must be called again. 
        Return Transaction if successful, False otherwise.
    """
    # Dock first
    F_nav.dock_ship(ship)

    # Limit the units per transaction to the trade volume
    ship_wp = F_nav.get_ship_waypoint(ship)
    tg = get_trade_good(good, ship_wp)
    if not tg:
        print(f"[ERROR] {ship} could not fetch trade info for {good} at market {ship_wp}.")
        return False
    units = min(units, tg['tradeVolume'])
    
    # Make the purchase
    r = ST.post_request(f'/my/ships/{ship}/purchase', data={'symbol': good, 'units': units})
    if r.status_code != 201:
        print(f"[ERROR] {ship} could not buy ({units}) {good}.")
        print(f" [INFO]", r.json())
        return False
    
    # Log sale
    t = r.json()['data']['transaction']
    _log_trade(t)

    if verbose:
        print(f"[INFO] Ship {ship} bought {t['units']} {t['tradeSymbol']} @ {t['pricePerUnit']} for a total of {t['totalPrice']} credits.")

    return t

def purchase_cargo(ship : str, good : str, units : int, verbose=True):
    """ Purchases units of a given good. Does not check budget. 
        Returns success [boolean] 
    """    
    # Buy all goods in increments. Underlying trade function caps volume, so we can just try until failure.
    cur_u = units
    while cur_u > 0:
        trade_result = _purchase_cargo(ship, good, cur_u, verbose=verbose)
        if not trade_result:
            # Sale failed. If an actual issue occurred, the trade function will report it. 
            return False
        else:
            # Sale succeeded; check remaining cargo.
            cur_u -= trade_result['units']
            if cur_u < 1:
                return True
    return True

def buy_goods(ship : str, goods : dict, verbose = True):
    """ Buys all of the specified goods at the current market. """
    all_bought = True
    for g in goods:
        if not purchase_cargo(ship, g, goods[g], verbose=verbose):
            all_bought = False
    return all_bought

def buy_ship(ship : str, shipyard : str, ship_type : str, verbose = True):
    """ Purchases a ship at given shipyard using given ship. Fails if no shipyard available. """
    if not F_nav.dock_ship(ship):
        return False
    
    r = ST.post_request(f'/my/ships', {"shipType": ship_type, "waypointSymbol": shipyard})
    if not r.status_code == 201:
        if verbose:
            print(f"[ERROR] {ship} failed to buy {ship_type} at {shipyard}:")
            print(f"       ", r.json())
        return False
    resp_data = r.json()['data']
    if verbose:
        print(f"[INFO] {ship} bought a new {ship_type} for {resp_data['transaction']['price']} credits (at {shipyard})")
    # TODO: Update the cache with this new ship's info: ship.NAV, ship.REGISTRATION; releasing ship lock should probably be delegated to the controller that's issueing the buy order
    new_ship = resp_data['ship']['symbol']
    F_nav._refresh_ship_nav(new_ship, resp_data['ship']['nav'])
    F_nav._refresh_ship_registration(new_ship, resp_data['ship']['registration'])
    return True

### SETTERS ###

def refresh_tradegoods(ship):
    """ Fetches market data (trade goods) from ship's current location and persists it to database. """
    cur_wp = F_nav.get_ship_waypoint(ship)
    market_data = get_market_info(cur_wp)
    if not market_data:
        print(f"[ERROR] {ship} failed to refresh market data for {cur_wp}.")
        return False
    if 'tradeGoods' not in market_data:
        print(f"[ERROR] {ship} wants to refresh market data for {cur_wp}, but failed to fetch trade good details.")
        return False

    # The TRADEGOODS table keeps a history, so the new data can just be appended. Callers need to filter for the right period.
    tgs_df = None
    try:
        tgs = market_data['tradeGoods']
        tgs_df = pd.DataFrame.from_dict(tgs)
        tgs_df['marketSymbol'] = cur_wp
        tgs_df['ts_created']   = int(time.time())
    except Exception as e:
        print(f"[ERROR] Exception while getting trade good data.")
        print(e)
        return False

    if not io.write_data('TRADEGOODS', tgs_df):
        print(f"[ERROR] Failed to write trade good data.")
        return False
    
    return True

def _parse_ship_data(shipyard_data):
    """ Parses shipyard datamodel into lists of ships and modules. """
    ship_data = list()
    module_data = list()

    # For each ship
    for s in shipyard_data['ships']:
        # Extract ship features
        ship_data.append({
            'shipyardSymbol': shipyard_data['symbol'],
            'type': s['type'],
            'name': s['name'],
            'description': s['description'],
            'purchasePrice': s['purchasePrice'],
            'supply': s['supply'],
            'frameSymbol': s['frame']['symbol'],
            'reactorSymbol': s['reactor']['symbol'],
            'engineSymbol': s['engine']['symbol'],
            'crewRequired': s['crew']['required'],
            'crewCapacity': s['crew']['capacity'],
            'activity': s['activity']
        })

        # Extract module features
        for m in s['modules']:
            module_data.append({
                'shipType': s['type'],
                'symbol': m['symbol'],
                'name': m['name'],
                'description': m['description'],
                'reqPower': m['requirements']['power'],
                'reqCrew': m['requirements']['crew'],
                'reqSlots': m['requirements']['slots'],
                'capacity': m.get('capacity', None)
            })
    
    return {
        'ships': ship_data,
        'modules': module_data
    }

def refresh_shipyard(ship, verbose=True):
    """ Fetches shipyard data from ship's current location and persists it to database. """

    # Dock first
    if not F_nav.dock_ship(ship):
        return False

    cur_wp  = F_nav.get_ship_waypoint(ship)
    sy_data = get_shipyard_info(cur_wp, verbose)
    if not sy_data:
        if verbose: print(f"[ERROR] {ship} failed to refresh shipyard data for {cur_wp}.")
        return False
    if 'ships' not in sy_data:
        if verbose: print(f"[ERROR] {ship} wants to refresh shipyard data for {cur_wp}, but failed to fetch ship details.")
        return False
    
    ships_df     = None
    modules_df   = None
    try:
        parsed_data = _parse_ship_data(sy_data)
        ships_df    = pd.DataFrame.from_dict(parsed_data['ships'])
        modules_df  = pd.DataFrame.from_dict(parsed_data['modules'])

        ships_df['ts_created']     = int(time.time())
        modules_df['ts_created']   = int(time.time())
    except Exception as e:
        if verbose: 
            print(f"[ERROR] Exception while parsing shipyard data.")
            print(e)
        return False
    
    write_ships   = io.write_data('shipyard.SHIPS', ships_df)
    write_modules = io.write_data('shipyard.MODULES', modules_df)
    return (write_ships and write_modules)
