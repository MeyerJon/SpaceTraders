"""
    System Siphon Probe Controller

    Functions that enable a controller to automatically use available probes in a system to siphon resources from the local Gas Giant.
"""
import asyncio, random, time
from SpaceTraders import io, fleet_resource_manager, scripts, F_utils, F_nav, F_extract, F_trade

### GLOBALS ###
BASE_PRIO_SIPHONER = 100
BASE_CONTROLLER_ID = "SIPHON-CONTROLLER"


### GETTERS ###
def get_available_siphon_drones(system : str, priority : int, controller : str):
    available = fleet_resource_manager.get_available_ships_in_systems([system], 'EXCAVATOR', prio=priority, controller=controller)
    q_siphoners = f"""
        select
            distinct shipSymbol
            from 'ship.MOUNTS'
            where 1=1
            and symbol like "MOUNT_GAS_SIPHON%"
            and shipSymbol in ({', '.join([f'"{s}"' for s in available])})
    """
    return [r[0] for r in io.read_list(q_siphoners)]


### HELPERS ###
async def mine_goods(ship: str, waypoint : str, goods : list = None):
    """ Extracts from a waypoint until cargo hold is filled, keeping only the desired goods.
    """
    refresh_period = 10 # Time between checks if ship gets locked (full cargo)

    # Navigate to waypoint if not there
    if F_nav.get_ship_waypoint(ship) != waypoint:
        if not await scripts.navigate(ship, waypoint):
            return False
        await scripts.await_navigation(ship)

    # Orbit location
    F_nav.orbit_ship(ship)

    # Continually extract from destination
    while True:
        # Check if the hold is already full
        cargo = F_trade.get_ship_cargo(ship)
        if cargo['capacity'] <= cargo['units']:
        # Hold is full. Stop extracting and wait a while.
            await asyncio.sleep(refresh_period)
            continue

        if F_extract.siphon(ship):
            # Check if the good is desired; if not, jettison it immediately.
            # TODO: make this work with the cached cargo data (which is written on extraction)
            """
            if goods is not None:
                e_yield = data['extraction']['yield']
                if e_yield['symbol'] not in goods:
                    ST.post_request(f'/my/ships/{ship}/jettison', data={'symbol': e_yield['symbol'], 'units': e_yield['units']})
            """
            # Check cargo capacity
            cargo = F_trade.get_ship_cargo(ship)
            if cargo['capacity'] <= cargo['units']:
            # Hold is full. Stop extracting and wait a while.
                print(f"[INFO] {ship} has filled its hold. Standing by for pickup.")
                await asyncio.sleep(refresh_period)
            else:
            # Otherwise, sleep until next extraction
                cd = F_utils.get_ship_cooldown(ship)['remainingSeconds']
                print(f"[INFO] {ship} cooling down for {cd} seconds.")
                await asyncio.sleep(cd)
        else:
        # Extraction failed for some reason; idle for a while and then try again
            cd = max(F_utils.get_ship_cooldown(ship)['remainingSeconds'], refresh_period)
            print(f'[WARNING] Ship {ship} failed to siphon. Retrying in {cd} seconds.')
            await asyncio.sleep(cd)
            
### MAIN ENTRY ###