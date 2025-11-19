"""
    SpaceTraders Fleet Resource Manager

    This file contains functionality that is the exclusive interface with fleet resource management.
    Controllers are able to request & release ships, and ships are able to indicate their state.
"""
from SpaceTraders import io, F_nav


### GETTERS ###

def get_ship_blocked_status(ship):
    """ Returns True if the ship is currently busy with an uninterruptible order. """
    records = io.read_list(f"SELECT blocked FROM 'control.SHIP_LOCKS' WHERE shipSymbol = \"{ship}\"")
    if len(records):
        return bool(records[0][0])
    else:
        return False # No entry in the DB means there is no known control over the ship
    
def get_ship_controller(ship):
    """ Returns current controller & priority for given ship. """
    records = io.read_list(f"SELECT controller, priority FROM 'control.SHIP_LOCKS' WHERE shipSymbol = \"{ship}\"")
    if len(records):
        return records[0][0], records[0][1]
    else:
        return None, -1 # No entry in the DB means there is no known control over the ship

def get_available_ships_in_systems(systems : list, ship_role : str = None, prio = 0, controller : str = None):
    """ Returns list of currently available ships in given sectors. Optionally uses ship type & priority to filter ships that could be released. """

    # Controller must either be NULL or same as the one specified, or have a lower priority than specified
    condition_ctrl = "locks.controller is NULL "
    if controller:
        condition_ctrl += f"or locks.controller = \"{controller}\" "
    condition_ctrl += f"or locks.priority < {prio}"

    q = f"""
        select
            nav.symbol
        from 'control.SHIP_LOCKS' locks

        inner join 'ship.NAV' nav
            on locks.shipSymbol = nav.symbol
            and nav.systemSymbol in ({', '.join([f'"{s}"' for s in systems])})

        inner join 'ship.REGISTRATION' reg
            on locks.shipSymbol = reg.shipSymbol

        where 1=1
        and ({condition_ctrl}) 
        and locks.blocked = 0
        """
    if ship_role is not None:
        q += f'\nand reg.role = "{ship_role}"'

    records = io.read_list(q)
    if len(records):
        return [r[0] for r in records]
    else:
        return list()

def get_controller_fleet(controller : str):
    """ Returns list of ships currently claimed by controller. """
    q = f"""
        select
            distinct shipSymbol
        from 'control.SHIP_LOCKS' l
        where controller = "{controller}"
    """
    return [r[0] for r in io.read_list(q)]

### SETTERS ###

def set_ship_blocked_status(ship : str, blocked : bool):
    """ Sets the ship's 'BLOCKED' status. This is used to signal that a ship is unavailable for reassignment. """
    return io.update_records("control.SHIP_LOCKS", {"shipSymbol": ship, "blocked": blocked}, key_cols=["shipSymbol"])


### LOCKING FUNCTIONALITY ###

def release_ship(ship : str, force=False):
    """ Sets a ship's status to released, meaning it's ready to be picked up by other controllers. If force=True, releases even when ship is blocked. """
    if not force and get_ship_blocked_status(ship):
        print(f"[ERROR] Can't release {ship}: currently blocked.")
        return False
    success = io.write_data('control.SHIP_LOCKS', {"shipSymbol": ship, "controller": None, "priority": -1, "blocked": False}, mode="update", key=["shipSymbol"])
    return success

def lock_ship(ship : str, controller : str, priority : int):
    """ Sets a ship's status to locked, meaning it cannot be controlled by other controllers until handover has taken place. """
    if get_ship_blocked_status(ship):
        print(f"[ERROR] Can't lock {ship}: currently blocked.")
        return False
    success = io.write_data('control.SHIP_LOCKS', {"shipSymbol": ship, "controller": controller, "priority": priority, "blocked": False}, mode="update", key=["shipSymbol"])
    return success


### REQUEST INTERFACE ###

def request_ship(ship : str, controller : str, priority : int):
    """ Signals to the resource manager that the controller wants control of the ship.
        A ship can be assigned if it is unblocked, and currently has no controller with higher priority.
    """
    # Check blocked state
    if get_ship_blocked_status(ship):
        return False
    
    # If the ship isn't blocked, but showing as in-transit, it may have lost its controller without being released. This should be flagged
    if F_nav.get_ship_nav(ship)['status'] == "IN_TRANSIT":
        print(f"[WARNING] Fleet resources has detected a moving ship without controller: {ship}.")
        F_nav._refresh_ship_nav(ship) # Attempt self-repair by forcing a nav reset
        return False

    
    # Check current controller
    cur_ctrl, cur_prio = get_ship_controller(ship)
    if cur_ctrl is None or cur_ctrl == controller:
        # Assign ship
        return lock_ship(ship, controller, priority)
    elif cur_prio < priority:
        # Handover: forcibly release previous control, then set new controller
        return (release_ship(ship) and lock_ship(ship, controller, priority))
    else:
        return False

def release_fleet(controller : str, force=False):
    """ Releases all ships owned by the controller. If force=True, also releases locked ships. """
    success = True
    for s in get_controller_fleet(controller):
        success = success and release_ship(s, force)

    return success