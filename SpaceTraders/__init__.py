import requests, time, math, sqlite3, json
import pandas as pd
from datetime import datetime
from SpaceTraders import io

### GLOBALS ###
BASE_URL    = 'https://api.spacetraders.io/v2'

# Get account token
ACCOUNT_TOKEN = None
with open('./token.txt', 'r') as ifile:
    ACCOUNT_TOKEN = ifile.read()

# Get agent token
AGENT_TOKEN = None
with open('./agent_token.txt', 'r') as ifile:
    AGENT_TOKEN = ifile.read()


### REQUESTS ###
def get_auth_header():
    return {'Authorization': f'Bearer {ACCOUNT_TOKEN}', 'Content-Type': 'application/json'}

def get_agent_header():
    return {'Authorization': f'Bearer {AGENT_TOKEN}', 'Content-Type': 'application/json'}

def _generic_get_request(url, params=None, headers=None):
    headers = headers or get_agent_header()
    r = requests.get(url=url, headers=headers, params=params)
    _log_request(url, r.status_code, data=params)
    return r

def _generic_post_request(url, data=None, headers=None):
    headers = headers or get_agent_header()
    data = data or dict()
    r = requests.post(url=url, headers=headers, json=data)
    _log_request(url, r.status_code, data=data)
    return r

def _generic_patch_request(url, data=None, headers=None):
    headers = headers or get_agent_header()
    data = data or dict()
    r = requests.patch(url=url, headers=headers, json=data)
    _log_request(url, r.status_code, data=data)
    return r

def _cleaned_endpoint(base, endpoint):
    """ Returns the full url after concatenating base with endpoint. """
    if base[-1] == '/' and endpoint[0] == '/':
        return base[:-1] + endpoint
    elif base[-1] != '/' and endpoint[0] != '/':
        return base + '/' + endpoint
    else:
        return base + endpoint
    
def _request_with_retries(req_f, params):
    """ Makes a given request, and retries if it fails. """
    max_retries     = 4
    backoff_seconds = 0.35
    resp = None
    for r in range(max_retries):
        try:
            resp = req_f(**params)
            if resp.status_code == 429:
                # Rate limit hit, back off before retrying
                backoff_resp = resp.json()['error']['data']['retryAfter']
                time.sleep(backoff_seconds + backoff_resp)
            else:
                return resp
        except Exception as e:
            print(f"[ERROR] Unhandled exception during API request:")
            print(e)
            io.log_exception(e)
            time.sleep(backoff_seconds)
            continue # Move on to retry
    # If loop ends without return, max retries was hit and we've failed
    # TODO how to handle? for now just return a custom 5XX status code that indicates this generic failure
    print("[WARNING] Rate limit hit and retries failed. Returning response as-is, which may result in errors.")
    return {"status_code": 599}

def get_request(url, params=None, headers=None):
    """ Makes a GET request to the SpaceTraders API. """
    return _request_with_retries(_generic_get_request, {'url': _cleaned_endpoint(BASE_URL, url), 'params': params, 'headers': headers}) 
    #return _generic_get_request(_cleaned_endpoint(BASE_URL, url), params, headers)

def post_request(url, data=None, headers=None):
    """ Makes a POST request to the SpaceTraders API. """
    return _request_with_retries(_generic_post_request, {'url': _cleaned_endpoint(BASE_URL, url), 'data': data, 'headers': headers}) 
    #return _generic_post_request(_cleaned_endpoint(BASE_URL, url), data, headers)

def patch_request(url, data=None, headers=None):
    """ Makes a PATCH request to the SpaceTraders API. """
    return _request_with_retries(_generic_patch_request, {'url': _cleaned_endpoint(BASE_URL, url), 'data': data, 'headers': headers}) 
    #return _generic_patch_request(_cleaned_endpoint(BASE_URL, url), data, headers)


### LOGGING / MONITORING ###
def _log_request(url, response, data=None):
    """ Logs the request into a logging table. """   
    row = {'url': url, 'status_code': response, 'request_body': json.dumps(data) if data is not None else None, 'timestamp': time.time()}
    io.write_rows('logs.REQUESTS', row)


### MINING & TRADING ###

def survey_waypoint(ship):
    """ Tries surveying the given ship's current location. On success, returns latest survey object. """
    r = post_request(BASE_URL + f'/my/ships/{ship}/survey')
    if r.status_code != 201:
        print(f'[ERROR] Ship {ship} failed to survey.')
        print(f' [INFO]', r.json())
        return False
    return r.json()['data']['surveys'][-1]

def extract_until_full(ship, verbose=True):
    """ Extracts from the current waypoint until cargo hold is filled. 
        This function blocks the thread since it uses a timed loop.
    """
    max_iterations = 50

    for _it in range(max_iterations):
        r = post_request(BASE_URL + f'/my/ships/{ship}/extract')
        if r.status_code == 201:
            data = r.json()['data']
            
            if verbose:
                e_yield = data['extraction']['yield']
                print(f"[INFO] Ship {ship} extracted {e_yield['units']} {e_yield['symbol']}.")
            
            if data['cargo']['capacity'] <= data['cargo']['units']:
                # Hold is full. Stop extracting.
                if verbose:
                    print(f'[INFO] Ship {ship} finished extracting (full hold).')
                return True
            
            # Otherwise, sleep until next extraction
            cd = data['cooldown']['remainingSeconds']
            time.sleep(cd)

        else:
            print(f'[ERROR] Ship {ship} failed to extract. Aborting operation.')
            print(f' [INFO]', r.json())
            return False
        
    # If the loop isn't exited on either path, we've hit the iteration limit
    print(f'[WARNING] Ship {ship} is aborting extraction -- operation timed out.')
    return False


### CONTRACTS ###

def get_contracts():
    """ Returns list of all contracts. """
    # TODO Account for pagination

    r = get_request(BASE_URL + f'/my/contracts')
    if r.status_code != 200:
        print(f'[ERROR] Failed to fetch contracts.')
        print(f' [INFO]', r.json())
        return list()
    
    return r.json()['data']

def get_open_contracts():
    """ Returns list of open (accepted & valid) contracts. """
    cts = get_contracts()
    return list(filter(lambda c : c['accepted'] and (not c['fulfilled']), cts))

def get_contract(contract_id):
    """ Returns specific contract details. """
    r = get_request(BASE_URL + f'/my/contracts/{contract_id}')
    if r.status_code != 200:
        print(f'[ERROR] Failed to fetch contract info for {contract_id}.')
        print(f' [INFO]', r.json())
        return False
    
    return r.json()['data']

def fulfill_contract(contract_id, verbose=True):
    """ Fulfills a given contract. """
    r = post_request(BASE_URL + f'/my/contracts/{contract_id}/fulfill')
    if r.status_code != 200:
        print(f"[ERROR] Failed to fulfill contract {contract_id}.")
        print(f" [INFO]", r.json())
        return False
    
    if verbose:
        reward = r.json()['data']['contract']['terms']['payment']['onFulfilled']
        print(f'[INFO] Contract {contract_id} fulfilled for {reward} credits.')

    return True

def negotiate_contract(ship):
    """ Tries negotiating a new contract from the given ship. """
    r = post_request(BASE_URL + f'/my/ships/{ship}/negotiate/contract')
    if r.status_code != 201:
        print(f"[ERROR] {ship} failed to negotiate a new contract.")
        print(f' [INFO]', r.json())
        return False
    return r.json()['data']['contract']

def accept_contract(contract_id):
    """ Tries to accept given contract. """
    r = post_request(BASE_URL + f'/my/contracts/{contract_id}/accept') 
    if r.status_code != 200:
        print(f"[ERROR] Failed to accept contract {contract_id}.")
        print(f' [INFO]', r.json())
        return False
    return r.json()['data']['contract']

def deliver_cargo(contract_id, ship, good, verbose=True):
    """ Delivers the specified good for the contract. Delivers entire inventory if possible.
        Returns status [boolean] - True if delivery successful.
    """

    # Determine units to deliver
    # Units in inventory
    cargo = get_ship_cargo(ship)['inventory']
    in_hold = list(filter(lambda i : i['symbol'] == good, cargo))
    if len(in_hold) == 0:
        print(f'[ERROR] Ship {ship} has no {good} to deliver. Delivery aborted.')
        return False
    else:
        in_hold = in_hold[0]['units']

    # Units needed by contract
    contract_r = get_request(BASE_URL + f'/my/contracts/{contract_id}')
    if contract_r.status_code != 200:
        print(f'[ERROR] Failed to fetch contract {contract_id}. Ship {ship} is aborting delivery.')
        return False
    
    delivery = list(filter(lambda t : t['tradeSymbol'] == good, contract_r.json()['data']['terms']['deliver']))
    if len(delivery) == 0:
        # Delivered good doesn't fit the contract
        print(f"[ERROR] Contract {contract_id} doesn't accept {good}. Aborting delivery.")
        return False
    delivery = delivery[0]
    required = delivery['unitsRequired'] - delivery['unitsFulfilled']
    to_deliver = min(in_hold, required)

    # Dock
    dock_ship(ship)

    # Attempt delivery
    r = post_request(BASE_URL + f'/my/contracts/{contract_id}/deliver', data={'shipSymbol': ship, 'tradeSymbol': good, 'units': to_deliver})
    if r.status_code != 200:
        print(f'[ERROR] Ship {ship} failed to deliver ({to_deliver}) {good}. Aborting delivery.')
        print(f' [INFO]', r.json())
        return False
    elif verbose:
        print(f'[INFO] Ship {ship} delivered {to_deliver} {good} for contract {contract_id}.')
        if in_hold >= required:
            print(f'[INFO] Ship {ship} completed contract {contract_id}.')
    return True



### Generic data endpoints ###

def get_ship_info(ship):
    """ Fetches all data on a ship. """
    r = get_request(f'/my/ships/{ship}')
    if r.status_code != 200:
        print(f"[ERROR] Failed to fetch info for {ship}")
        print(f" [INFO]", r.json())
        return False
    return r.json()['data']