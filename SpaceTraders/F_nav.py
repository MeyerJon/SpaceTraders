"""
    SpaceTraders - Navigation Functions

    This file contains functionality for:
        - Getting data related to fleet navigation (positions, distances, fuel consumption, ...)
        - Taking navigation-related actions (moving ships, refueling, docking/orbiting, ...)

    All data is passed in json/dict format, unless specified otherwise.
    Functions that trigger actions should return a boolean indicating success unless specified otherwise.
"""
import SpaceTraders as ST
from SpaceTraders import io, F_utils
import math, time
from datetime import datetime
import pandas as pd

### GETTERS ###

def __get_ship_nav_old(ship, verbose=True):
    """ Returns ship's nav info. """
    r = ST.get_request(f'/my/ships/{ship}/nav')
    if r.status_code != 200:
        print(f"[ERROR] Failed to get nav info for ship {ship}.")
        print(f' [INFO]', r.json())
        return False
    return r.json()['data']

def _ship_nav_cache(ship):
    """ Returns cached ship nav if valid. If not cached, or cache outdated, returns None. """
    # nav contains arrival time. if the NAV table has a ts_created timestamp, a record is 'outdated' if it was created before arrival, so we need to refresh the nav info. Otherwise, we're still in sync -- nothing has moved the ship
    sn = io.read_dict(f"SELECT * FROM 'ship.NAV' WHERE symbol = \"{ship}\"")
    if sn and len(sn) > 0:
        # If ship is still showing as IN_TRANSIT even if it was supposed to arrive in the past, the cache is invalid
        nav = sn[0]
        if not ((nav['status'] == 'IN_TRANSIT') and (F_utils.ts_delta_seconds(nav['arrival']) <= 0)):
            return F_utils.exclude_dict(nav, ['ts_created'])

def get_ship_nav(ship, verbose=True):
    """ Returns ship's nav info. """
    # Try the DB
    sn = _ship_nav_cache(ship)
    if sn:
        return sn
    
    # On cache miss, refresh cache & return data
    _refresh_ship_nav(ship)
    return _ship_nav_cache(ship)

def get_transit_time_seconds(ship, verbose=True):
    """ Returns the amount of seconds left until ship reaches destination. 0 if not in transit or arrived in the past. """
    nav = get_ship_nav(ship)
    if not nav:
        if verbose:
            print(f"[ERROR] Could not get transit time for {ship}.")
        return False
    
    if nav['status'] != 'IN_TRANSIT':
        return 0
    
    ts_arrival = nav['arrival']

    return max(0, F_utils.ts_delta_seconds(ts_arrival))

def get_ship_waypoint(ship):
    """ Returns waypointSymbol of ship's currently location. """
    r = get_ship_nav(ship)
    return r['waypointSymbol']

def get_waypoints_in_system(system, type=None, traits=None):
    """ Returns info on all waypoints in given system. May show waypoints as 'Uncharted'. 
        Parameters:
            - types [str]  : filters type of waypoint
            - traits [str] : filters waypoints with given traits
    
    """        
    filter_params = dict()
    if type is not None:
        filter_params['type'] = type
    if traits is not None:
        filter_params['traits'] = traits

    nextpage = 1
    data = list()
    total = -1
    while total < 0 or len(data) < total:
        r = ST.get_request(f'/systems/{system}/waypoints', params={'page': nextpage, **filter_params})
        if r.status_code == 200:
            data.extend(r.json()['data'])
            nextpage += 1
            total = r.json()['meta']['total']
        else:
            print(f'[WARNING] Failed to fetch page {nextpage} of waypoints in system {system}.')
            print(f'   [INFO]', r.json())
            return data
        
    return data

def _get_known_fuel_stops(system : str):
    """ Returns list of cached waypoints that exchange fuel. """
    wps = io.read_list(f"""select distinct marketSymbol
                          from tradegoods
                          where symbol = "FUEL" 
                          and activity is NULL
                          and marketSymbol like "{system}-%"
                       """)
    return [r[0] for r in wps]

def get_waypoint_info(wp):
    sys = F_utils.system_from_wp(wp)
    r = ST.get_request(f'/systems/{sys}/waypoints/{wp}')
    if r.status_code != 200:
        print(f'[ERROR] Failed to fetch info for waypoint {wp}.')
        print(f' [INFO]', r.json())
        return False

    return r.json()['data']

def get_waypoint_coords(wp):
    """ Returns the waypoints coordinates as a dict. Uses cached coordinates if possible. """
    wp_data = io.read_dict(f'SELECT x, y FROM \'nav.WAYPOINTS\' WHERE symbol = "{wp}"')
    if wp_data:
        return wp_data[0]

    # If not cached, we need to grab it from the API
    wp_data = get_waypoint_info(wp)
    return {'x': wp_data['x'], 'y': wp_data['y']}

def _wp_distance_cache(wp1, wp2):
    """ Tries returning the distance between to cached waypoints. Returns False if no distance cached. """
    row = io.read_list(f'SELECT dist FROM WP_DISTANCES WHERE (src = "{wp1}" and dst = "{wp2}") or (src = "{wp2}" and dst = "{wp1}")')
    if row: 
        return row[0][0]
    else: 
        return False

def wp_distance(wp1, wp2):
    """ Returns the Euclidean distance between two given waypoints in the same system. """

    # Look up in cache first
    cached_dist = _wp_distance_cache(wp1, wp2)
    if cached_dist:
        return cached_dist
    
    sys1 = F_utils.system_from_wp(wp1)
    sys2 = F_utils.system_from_wp(wp2)
    if sys1 != sys2:
        print(f'[ERROR] Distance calculation failed between {wp1} and {wp2}. Not in the same system.')
        return False
    
    # Get waypoint info/coordinates
    wp1_data = get_waypoint_coords(wp1)
    wp2_data = get_waypoint_coords(wp2)

    # Calc distance
    dist = math.sqrt(((wp1_data['x'] - wp2_data['x'])*(wp1_data['x'] - wp2_data['x'])) + ((wp1_data['y'] - wp2_data['y'])*(wp1_data['y'] - wp2_data['y'])))
    return dist

def coords_to_wp_dist(x, y, wp):
    """ Returns the distance from the waypoint to the given coordinates in the same system. """
    # Get waypoint info/coordinates
    sys1 = F_utils.system_from_wp(wp)
    wp1_data = get_waypoint_info(wp)
    # Calc distance
    dist = math.sqrt(((wp1_data['x'] - x)*(wp1_data['x'] - x)) + ((wp1_data['y'] - y)*(wp1_data['y'] - y)))
    return dist

def get_closest_wp(waypoint : str, targets : list = None):
    """ Returns closest Waypoint to given location. If no targets given, defaults to cached waypoints in system. """
    if targets is None:
        targets = [r[0] for r in io.read_list("SELECT DISTINCT symbol FROM WAYPOINTS")]
    
    min_d   = float('inf')
    closest = None
    for t in targets:
        d = wp_distance(waypoint, t)
        if d < min_d:
            min_d = d
            closest = t

    return closest 

def get_fuel_required(wp1, wp2, flightmode='CRUISE'):
    """ Returns units of fuel needed to travel between two (same-system) waypoints. """
    # Info from https://github.com/SpaceTradersAPI/api-docs/wiki/Travel-Fuel-and-Time
    d = wp_distance(wp1, wp2)
    if d is False:
        # Not in the same system, unsupported for now
        print(f'[ERROR] Cannot calculate fuel cost for inter-system travel ({wp1} to {wp2}).')
        return False

    if flightmode == 'DRIFT': return 1
    elif flightmode == 'CRUISE': return round(d)
    elif flightmode == 'BURN': return 2*round(d)
    elif flightmode == 'STEALTH': return round(d)

def get_ship_fuel(ship):
    # Try the cache
    cache_q = f"select * from 'ship.FUEL' where shipSymbol = \"{ship}\""
    fuel = io.read_dict(cache_q)
    if len(fuel) > 0:
        fuel = fuel[0]
    else:
        # Refresh the cache
        if not _refresh_ship_fuel(ship):
            print(f"[ERROR] Could not get fuel for {ship} : invalid shipSymbol.")
            return False
        fuel = io.read_dict(cache_q)[0]
    return fuel

def get_fuel_capacity(ship):
    """ Returns the ship's fuel capacity. """
    fuel = get_ship_fuel(ship)
    if fuel:
        return fuel['capacity']
    else:
        return False

def check_in_transit(ship, verbose=True):
    """ Returns True if a ship is currently in transit/navigating somewhere. """
    r = ST.get_request(f'/my/ships/{ship}')
    if r.status_code != 200:
        print(f'[ERROR] Could not get info for ship {ship}.')
        print(f" [INFO]", r.json())
    else:
        return r.json()['data']['nav']['status'] == 'IN_TRANSIT'

def get_path(ship, src, dst, ignore_current_fuel=True):
    """ Returns a list (hop, flightmode) that allow the ship to reach the destination waypoint in the least time. If no path available, returns empty list. 
        Parameters:
            - ignore_current_fuel [bool] : If False, takes into account how much fuel the ship currently has instead of assuming a full tank.
    """
    fuelcap = get_fuel_capacity(ship) - 1.0
    burncap = math.floor(fuelcap / 2.0) - 1.0 # Pessimistic estimate of how much fuel can be used to burn
    cur_fuel = fuelcap
    if not ignore_current_fuel:
        cur_fuel = None # TODO: Make this function fuel-aware to avoid edge-cases where the ship can't actually follow the path
    
    # Probes / Satellites have to drift everywhere, but other ships should give a warning if they have no fuel capacity
    if fuelcap < 1:
        # Satellites don't have fuel, so just hop directly
        if ST.get_ship_info(ship)['registration']['role'] == 'SATELLITE':
            return [(dst, "BURN", wp_distance(src, dst))]
        else:
            # Otherwise, something strange is afoot and pathing should fail
            print(f"[ERROR] {ship} failed to find a path to {dst}: fuel capacity too low ({fuelcap}).")
            return list()

    cur_nav   = get_ship_nav(ship)
    cur_node  = src
    fuel_nodes = _get_known_fuel_stops(cur_nav['systemSymbol'])
    nodes = [cur_node, dst] + fuel_nodes
    nodes = list(set(nodes))
    path = list()
    while True:
        # If a direct path is possible, just use that. This is a separate case because some waypoints have the same location (planet & its moons), which can mess with the distance-ordering below otherwise
        dst_dist = wp_distance(cur_node, dst)
        if dst_dist < fuelcap:
            flightmode = "BURN" if ((dst_dist < burncap) and dst in fuel_nodes) else "CRUISE" # Don't burn to places you can't refuel
            path.append((dst, flightmode, dst_dist))
            break

        # Pathing can happen greedily, since we're in 'open space': the shortest path is a straight line, or something approaching it.
        # Find the next node by trying to go as far as fuel allows
        reachable = list(filter(lambda wp : get_fuel_required(cur_node, wp) < fuelcap, nodes))
        if len(reachable) == 0:
            # We've hit a dead end -- pathing failed
            return list()
        
        next_node = sorted(reachable, key=lambda wp : wp_distance(wp, dst))[0]
        if wp_distance(next_node, dst) >= dst_dist:
            # If the best we can do isn't an improvement, the greedy approach has failed and we can't path
            return list()
        
        next_hop_dist = wp_distance(cur_node, next_node)
        flightmode = "BURN" if next_hop_dist < burncap else "CRUISE"
        path.append((next_node, flightmode, next_hop_dist))

        # We never want to revisit a node, so remove it from the options
        nodes.remove(cur_node)
        cur_node = next_node

    return path


### ACTIONS ###

def dock_ship(ship):
    """ Docks ship at the current waypoint. """
    if get_ship_nav(ship)['status'] == 'DOCKED':
        return True
    
    r = ST.post_request(f'/my/ships/{ship}/dock')
    if r.status_code != 200:
        print(f'[ERROR] Ship {ship} failed to dock.')
        print(f' [INFO]', r.json())
        return False
    
    # Update ship nav record
    new_status = r.json()['data']['nav']['status']
    io.update_records('ship.NAV', {'symbol': ship, 'status': new_status}, ['symbol'])
    return True

def orbit_ship(ship):
    """ Launches ship into orbit of the current waypoint. """
    if get_ship_nav(ship)['status'] == 'IN_ORBIT':
        return True
    
    r = ST.post_request(f'/my/ships/{ship}/orbit')
    if r.status_code != 200:
        print(f'[ERROR] Ship {ship} failed to orbit.')
        print(f' [INFO]', r.json())
        return False

    # Update ship nav record
    new_status = r.json()['data']['nav']['status']
    io.update_records('ship.NAV', {'symbol': ship, 'status': new_status}, ['symbol'])
    return True

def set_flight_mode(ship, mode):
    """ Sets the flight mode for the ship. """
    # Sanity check - only valid modes
    if mode not in ['CRUISE', 'BURN', 'DRIFT', 'STEALTH']:
        print(f"[ERROR] Invalid flight mode '{mode}' for {ship}. Aborting.")
        return False
    
    # Check cache - if already in this mode, no API request is needed
    cur_mode = io.read_list(f'SELECT flightMode from \'ship.NAV\' where symbol = "{ship}"')
    if len(cur_mode) and cur_mode[0][0] == mode:
        return True

    r = ST.patch_request(
        f"/my/ships/{ship}/nav",
        data={"flightMode": mode}
    )
    if r.status_code != 200:
        print(f'[ERROR] Failed to update {ship}\' flight mode to {mode}.')
        print(f' [INFO]', r.json())
        return False
    # Update ship nav record
    io.update_records('ship.NAV', {'symbol': ship, 'flightMode': mode}, ['symbol'])
    return True

def navigate_in_system(ship, waypoint, flightmode='CRUISE', verbose=True):
    """ Sets a ship on course for a waypoint within the same system. Does not dock. 
        Returns status [boolean] - True if succesfully navigating to destination.
    """
    # Check ship status before trying to take off
    ship_r = ST.get_request(f'/my/ships/{ship}')

    # Check whether ship is already in transit
    if ship_r.json()['data']['nav']['status'] == 'IN_TRANSIT':
        print(f'[ERROR] Ship {ship} already in transit. Navigation cancelled.')
        return False
    
    # Check whether ship is already at the destination
    if ship_r.json()['data']['nav']['waypointSymbol'] == waypoint:
        print(f'[ERROR] Ship {ship} is already at destination {waypoint}. Navigation cancelled.')
        return False

    # Check whether target ship is in the target system
    target_sys = F_utils.system_from_wp(waypoint) # Waypoints take the form SECTOR-SYSTEM-WAYPOINT
    cur_sys = ship_r.json()['data']['nav']['systemSymbol']
    if ship_r.status_code != 200:
        print(f'[ERROR] Could not fetch info for ship {ship}.')
        print(' [INFO]', ship_r.json())
        return False
    elif cur_sys != target_sys:
        print(f'[ERROR] Ship {ship} not in system {target_sys} (currently in {cur_sys}). Navigation cancelled.')
        return False
        
    # Set travel mode
    set_flight_mode(ship, flightmode)
    # Go to orbit
    orbit_ship(ship)

    # Navigate to waypoint
    nav_r = ST.post_request(f'/my/ships/{ship}/navigate', data={'waypointSymbol': waypoint})
    
    if nav_r.status_code != 200:
        print(f'[ERROR] Ship {ship} failed to set course for {waypoint}.')
        print(f' [INFO]', nav_r.json())
        return False

    # Update the database
    nav_r = nav_r.json()['data']
    _refresh_ship_nav(ship, nav_r['nav'])
    _refresh_ship_fuel(ship, nav_r['fuel'])

    if verbose:
        # Check navigation time
        status = nav_r['nav']
        dept_time = status['route']['departureTime'][:-1] # Removes the 'Z' at the end to parse properly
        arrival_time = status['route']['arrival'][:-1]
        delta_time = datetime.fromisoformat(arrival_time) - datetime.fromisoformat(dept_time)
        print(f'[INFO] Ship {ship} en-route to {waypoint}. Arrival at {arrival_time} ({delta_time.total_seconds()} seconds).')
    
    return True
    
def refuel_ship(ship, units=None, from_cargo=None, verbose=False):
    """ Tries to refuel the ship at the current location. 
        Return status [boolean] - True if refueled successfully.
    """

    # Dock at location
    dock_ship(ship)

    # Try refueling
    body = dict()
    if units is not None:
        body['units'] = units
    if from_cargo is not None:
        body['fromCargo'] = from_cargo
    fuel_r = ST.post_request(f'/my/ships/{ship}/refuel', data=body)

    if fuel_r.status_code == 400: 
        # No fuel sold in this location -- probably a navigate call trying and failing to auto-refuel (code 4601)
        return False
    elif fuel_r.status_code != 200:
        print(f'[ERROR] Ship {ship} failed to refuel.')
        print(f' [INFO]', fuel_r.json())
        return False
    
    # Refresh database
    fuel_r = fuel_r.json()['data']
    _refresh_ship_fuel(ship, fuel_r['fuel'])

    if verbose:
        t = fuel_r['transaction']
        print(f"[INFO] Ship {ship} refueled {t['units']} units. Total cost: {t['totalPrice']} cr ({t['pricePerUnit']} cr/u).")

    return True


### PERSISTENCE ###
def _refresh_ship_nav(ship : str, nav : dict = None):
    """ Updates the nav data for given ship in the database. If 'nav' is passed a Navigation object, uses that to update instead of the API. """
    if nav is None:
        r = ST.get_request(f'/my/ships/{ship}/nav')
        if r.status_code != 200:
            print(f"[ERROR] Failed to get nav info for ship {ship}.")
            print(f' [INFO]', r.json())
            return False
        nav = r.json()['data']
    
    to_write = dict()
    try:
        
        to_write = {
            "symbol": ship,
            "systemSymbol": nav["systemSymbol"],
            "waypointSymbol": nav["waypointSymbol"],
            "departureTime": nav["route"]["departureTime"],
            "arrival": nav["route"]["arrival"],
            "status": nav["status"],
            "flightMode": nav["flightMode"],
            "ts_created": int(time.time())
        }
    except Exception as e:
        print(f"[ERROR] Failed to refresh nav for {ship}. Exception:")
        print(e)
        return False
    
    try:
        nav_table = "ship.NAV"
        io.write_data(nav_table, to_write, mode='update', key=['symbol'])
    except Exception as e:
        print(f"[ERROR] Failed to write nav data for {ship}. Exception:")
        print(e)
        raise e    

    return True

def _refresh_ship_registration(ship : str, reg : dict = None):
    """ Updates the registration data for a ship. If 'reg' is passed a Registration object, uses that to update instead of the API. """
    if reg is None:
        r = ST.get_request(f'/my/ships/{ship}')
        if not r.status_code == 200:
            print(f"[ERROR] Failed to refresh registration for {ship} : could not fetch ship info.")
            return False
        reg = r.json()['data']['registration']
    
    return io.write_data('ship.REGISTRATION', {'shipSymbol': ship, **reg}, mode="update", key=["shipSymbol"])

def _refresh_ship_mounts(ship : str, mounts : list = None):
    """ Updates the installed mount data for a ship. If 'mounts' is passed a list of Mounts object, uses that to update instead of the API. """
    if mounts is None:
        r = ST.get_request(f'/my/ships/{ship}')
        if not r.status_code == 200:
            print(f"[ERROR] Failed to refresh mounts for {ship} : could not fetch ship info.")
            return False
        mounts = r.json()['data']['mounts']

    success = True
    for m in mounts:
        enriched = {"shipSymbol": ship, "symbol": m["symbol"], "strength": m.get("strength", None), 
                 "power": m["requirements"].get("power", None), "crew": m["requirements"].get("crew", None), "slots": m["requirements"].get("slots", None)}
        success = io.write_data('ship.MOUNTS', enriched, mode="update", key=["shipSymbol", "symbol"]) and success
    return success

def _refresh_ship_fuel(ship : str, fuel : dict = None):
    """ Updates the ship's fuel. If 'fuel' is passed a Fuel object, uses that to update instead of the API. """
    if fuel is None:
        r = ST.get_request(f'/my/ships/{ship}')
        if not r.status_code == 200:
            print(f"[ERROR] Failed to refresh fuel status for {ship} : could not fetch ship info.")
            return False
        fuel = r.json()['data']['fuel']
    
    return io.write_data('ship.FUEL', {'shipSymbol': ship, 'current': fuel['current'], 'capacity': fuel['capacity']}, mode="update", key=["shipSymbol"])

def _refresh_waypoints(system):
    """ Refresh the cache for the details of all (charted) waypoints in a system.
        Writes to
            nav.WAYPOINTS 
            nav.TRAITS
            nav.MODIFIERS
    """

    # Get the paginated waypoint data
    nextpage = 1
    data = list()
    total = -1
    while total < 0 or len(data) < total:
        # Fetch next page
        r = ST.get_request(f'/systems/{system}/waypoints', params={'page': nextpage})
        if r.status_code == 200:
            data.extend(r.json()['data'])
            nextpage += 1
            total = r.json()['meta']['total']
        else:
            print(f'[WARNING] Failed to fetch page {nextpage} of waypoints in system {system}.')
            print(f'   [INFO]', r.json())
            return False
        
    # Process nested response into tabular models
    wp_data = list()
    trait_data = list()
    modifier_data = list()

    for wp in data:
        parsed = io.parse_nested_obj(wp, "waypoint")

        df_wp = parsed["waypoint"]
        if len(df_wp): wp_data.append(df_wp)
        df_traits = parsed["traits"]
        df_traits["waypointSymbol"] = wp["symbol"]
        if len(df_traits): trait_data.append(df_traits)
        df_modifiers = parsed["modifiers"]
        df_modifiers["waypointSymbol"] = wp["symbol"]
        if len(df_modifiers): modifier_data.append(parsed["modifiers"])

    # Append key if necessary & write to DB
    if len(wp_data):
        df_wps = pd.concat(wp_data)
        df_wps['ts_created'] = int(time.time())
        io.write_data("nav.WAYPOINTS", df_wps, mode="update", key=["symbol"])

    if len(trait_data):
        df_traits = pd.concat(trait_data)
        df_traits['ts_created'] = int(time.time())
        io.write_data("nav.TRAITS", df_traits, mode="update", key=["waypointSymbol", "symbol"])

    if len(modifier_data):
        df_modifiers = pd.concat(modifier_data)
        df_modifiers['ts_created'] = int(time.time())
        io.write_data("nav.MODIFIERS", df_modifiers, mode="update", key=["waypointSymbol", "symbol"])

def _refresh_ships(ships=None):
    """ Refreshes ship info. If 'ships' is passed as a Ships array, updates from that instead of the API. """
    if ships is None:
        # TODO include pagination
        r = ST.get_request('/my/ships')
        if r.status_code != 200:
            print(f"[ERROR] Error while refreshing ship info:")
            print(f"       ", r.json())
            return False
        
        ships = r.json()['data']

    data_registration   = list()
    for s in ships:

        # Update ship registration
        df_registration = s['registration']
        df_registration["shipSymbol"] = s['symbol']
        data_registration.append(df_registration)

        # Update other models
        _refresh_ship_nav(s["symbol"], s['nav'])
        _refresh_ship_registration(s["symbol"], s["registration"])
        _refresh_ship_mounts(s["symbol"], s["mounts"])
        _refresh_ship_fuel(s['symbol'], s['fuel'])

    io.write_data("ship.REGISTRATION", data_registration, mode="update", key=["shipSymbol"])