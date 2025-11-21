"""
    SpaceTraders - Extraction Functions

    This file contains functionality for:
        - Getting data related to extraction (surveys etc)
        - Taking mining-related actions (extraction, survey, ...)

    All data is passed in json/dict format, unless specified otherwise.
    Functions that trigger actions should return a boolean indicating success unless specified otherwise.
"""
import SpaceTraders as ST
from SpaceTraders import io, F_utils, F_nav, F_trade
import pandas as pd
import math, datetime, time

### GETTERS ###

### SETTERS ###

### ACTIONS ###
def extract(ship : str, goods : list = None):
    """ Orders ship to extract at current location. Returns success status. Updates cargo & cooldown for ship. 
        Parameters:
            - goods [list<str>] : If provided, only keeps the specified goods (jettisons any others in its inventory)
    """
    r = ST.post_request(f'/my/ships/{ship}/extract')
    if r.status_code == 201:
        data = r.json()['data']
        e_yield = data["extraction"]["yield"]

        # Refresh cargo
        F_trade._refresh_cargo(ship, data['cargo'])

        # Refresh cooldown
        F_utils._refresh_ship_cooldown(ship, data['cooldown'])

        # Check the goods filter
        if goods is not None and e_yield['symbol'] not in goods:
            # If undesired good was extracted, jettison immediately
            F_trade.jettison_cargo(ship, e_yield['symbol'], e_yield['units'])           

        return True
    else:
        return False
    
def siphon(ship : str, goods : list = None):
    """ Orders ship to siphon at current location. Returns success status. Updates cargo & cooldown for ship. 
        Parameters:
            - goods [list<str>] : If provided, only keeps the specified goods (jettisons any others in its inventory)
    """
    r = ST.post_request(f'/my/ships/{ship}/siphon')
    if r.status_code == 201:
        data = r.json()['data']
        e_yield = data["siphon"]["yield"]

        # Refresh cargo
        F_trade._refresh_cargo(ship, data['cargo'])

        # Refresh cooldown
        F_utils._refresh_ship_cooldown(ship, data['cooldown'])

        # Check the goods filter
        if goods is not None and e_yield['symbol'] not in goods:
            # If undesired good was extracted, jettison immediately
            F_trade.jettison_cargo(ship, e_yield['symbol'], e_yield['units'])

        return True
    else:
        return False

### PERSISTENCE ###