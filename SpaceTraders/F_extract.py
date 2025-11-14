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
def extract(ship : str):
    """ Orders ship to extract at current location. Returns success status. Updates cargo & cooldown for ship. """
    r = ST.post_request(f'/my/ships/{ship}/extract')
    if r.status_code == 201:
        data = r.json()['data']

        # Refresh cargo
        F_trade._refresh_cargo(ship, data['cargo'])

        # Refresh cooldown
        F_utils._refresh_ship_cooldown(ship, data['cooldown'])

        return True
    else:
        return False
    
def siphon(ship : str):
    """ Orders ship to siphon at current location. Returns success status. Updates cargo & cooldown for ship. """
    r = ST.post_request(f'/my/ships/{ship}/siphon')
    if r.status_code == 201:
        data = r.json()['data']

        # Refresh cargo
        F_trade._refresh_cargo(ship, data['cargo'])

        # Refresh cooldown
        F_utils._refresh_ship_cooldown(ship, data['cooldown'])

        return True
    else:
        return False

### PERSISTENCE ###