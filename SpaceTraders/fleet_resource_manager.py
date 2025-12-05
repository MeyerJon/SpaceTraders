"""
    SpaceTraders Fleet Resource Manager

    This file contains functionality that is the exclusive interface with fleet resource management.
    Controllers are able to request & release ships, and ships are able to indicate their state.
"""
from SpaceTraders import io, F_nav
import time


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


### REQUEST QUEUE MANAGEMENT ###
def get_request_timeout() -> int:
    """ Returns how long a request remains valid (in seconds). """
    return 40

def enqueue_request(ship: str, controller : str, priority : int):
    """ Registers a controller's request for the given ship. Requests remain valid for a short period, and will guarantee assignment if the requester is at the front of the queue at the time of the request. """
    return io.write_data('control.SHIP_REQUESTS', {'ship': ship, 'controller': controller, 'priority': priority, 'ts_created': int(time.time())}, mode="update", key=['ship', 'controller'])

def pop_request(ship : str, controller : str):
    """ Removes the controller from the ship's request queue. """
    return io.update_records_custom(f"DELETE FROM 'control.SHIP_REQUESTS' where ship=\"{ship}\" and controller=\"{controller}\"")

def peek_request_queue(ship):
    """ Returns the first ship in queue for the ship, or None if no controllers have valid requests for it. """
    q = f"""
        select
            *
        from 'control.SHIP_REQUESTS'
        where (unixepoch('now') - ts_created) <= {get_request_timeout()}
        order by priority desc, [order] asc
    """
    queue = io.read_dict(q)
    if len(queue) > 0:
        return queue[0]['controller']
    elif len(queue) == 0:
        return None
    else:
        print(f"[ERROR] Fleet management failed to check request queue for {ship}.")
        return False


### REQUEST INTERFACE ###

def request_ship(ship : str, controller : str, priority : int):
    """ Signals to the resource manager that the controller wants control of the ship.
        A ship can be assigned if it is unblocked, and currently has no controller with higher priority.
    """
    # Check blocked state
    if get_ship_blocked_status(ship):
        # Log the failed request for the future
        enqueue_request(ship, controller, priority)
        return False
    
    # If the ship isn't blocked, but showing as in-transit, it may have lost its controller without being released. This should be flagged
    # In these cases, the requesting controller may get the ship. It's assumed that the previous one shut down unexpectedly and the ship is ready for new orders
    if F_nav.get_ship_nav(ship)['status'] == "IN_TRANSIT":
        print(f"[WARNING] Fleet resources has detected a moving ship without controller: {ship}.")
        F_nav._refresh_ship_nav(ship) # Attempt self-repair by forcing a nav reset
    
    # Check current controller
    cur_ctrl, cur_prio = get_ship_controller(ship)
    if cur_ctrl == controller:
        # Assign ship (again)
        return True
    
    # Check priority: if a more urgent request comes in, it must be granted immediately
    if cur_prio < priority:
        # Handover: forcibly release previous control, then set new controller
        return (release_ship(ship) and lock_ship(ship, controller, priority))        
    
    # If request hasn't been granted due to priority, the queue should be checked
    queued_controller = peek_request_queue(ship)
    if (queued_controller is None) or (queued_controller == controller):
        # There is no other controller in queue, or this controller is first in queue: request is granted
        assignment = lock_ship(ship, controller, priority)
        if assignment: pop_request(ship, controller)
        return assignment
    else:
        # Another controller is first in queue, request denied but logged for future tries
        enqueue_request(ship, controller, priority)
        return False 
    

def release_fleet(controller : str, force=False):
    """ Releases all ships owned by the controller. If force=True, also releases locked ships. """
    success = True
    for s in get_controller_fleet(controller):
        success = success and release_ship(s, force)

    return success