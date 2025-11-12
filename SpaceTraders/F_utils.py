"""
    SpaceTraders - Common utilities

    This file contains functionality that's used across the rest of the module.

    All data is passed in json/dict format, unless specified otherwise.
    Functions that trigger actions should return a boolean indicating success unless specified otherwise.
"""
import SpaceTraders as ST
import pytz

from datetime import datetime


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