"""
    SpaceTraders - Common utilities

    This file contains functionality that's used across the rest of the module.

    All data is passed in json/dict format, unless specified otherwise.
    Functions that trigger actions should return a boolean indicating success unless specified otherwise.
"""
import SpaceTraders as ST
from SpaceTraders import io
import pytz

from datetime import datetime

### GENERIC ###
def system_from_wp(wp):
    """ Returns just the system part of the waypoint symbol. """
    return '-'.join(wp.split('-')[:-1]) # Waypoints take the form SECTOR-SYSTEM-WAYPOINT

def ts_to_dt(timestamp):
    """ Converts server timestamp (UTC/iso) into Python datetime object"""
    return datetime.fromisoformat(timestamp.replace('Z', '+00:00')).astimezone(pytz.UTC)

def ts_delta_seconds(timestamp):
    """ Returns amount of seconds between given timestamp and current time. Negative result if timestamp is in the past. """
    if isinstance(timestamp, str):
        timestamp = ts_to_dt(timestamp)
    return (timestamp - datetime.now(pytz.utc)).total_seconds()

def exclude_dict(d : dict, excl_keys : list):
    """ Returns copy of dictionary without specified keys. """
    return {k: d[k] for k in set(list(d.keys())) - set(excl_keys)}


### PERSISTENCE - GETTERS ###
def get_ship_cooldown(ship : str):
    """ Returns the Cooldown object for a ship. """
    q_valid_cds = f"""
        select
            *
        from 'ship.COOLDOWN'
        where (expiration is null or datetime(expiration) >= datetime('now'))
        and shipSymbol = \"{ship}\"
        """
    cd = io.read_dict(q_valid_cds)
    if not cd:
        _refresh_ship_cooldown(ship)
    cd = io.read_dict(f"SELECT * FROM 'ship.COOLDOWN' where shipSymbol = \"{ship}\"")
    if not cd:
        print(f"[ERROR] Failed to fetch cooldown info for {ship}.")
        return False
    return cd[0]


### PERSISTENCE - REFRESHES ###
def _refresh_ship_cooldown(ship : str, cd : dict = None):
    """ Updates the cooldown data for a ship. If 'cd' is passed a Cooldown object, uses that to update instead of the API. """
    if cd is None:
        r = ST.get_request(f'/my/ships/{ship}/cooldown')
        if r.status_code == 200:
            cd = r.json()['data']
        elif r.status_code == 204:
            # API returns 204 if no cooldown
            cd = {"shipSymbol": ship, "totalSeconds": 0, "remainingSeconds": 0, "expiration": None}
        else:
            return False
    
    return io.write_data('ship.COOLDOWN', cd, mode="update", key=["shipSymbol"])