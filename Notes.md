# SpaceTraders notes

Callsign : RYVIOS


## Interfaces
https://staffordwilliams.com/spacetraders/status
https://cartography.ancalagon.black/
https://galaxia-haven.vercel.app/


## TODO
**Automated mining fleets**
- Siphoning drones
    -> 'set and forget': navigate to target asteroid, and extract there endlessly, hanging when their cargo hold is full

- Haulers
    -> have to identify drone clusters with decent hauls/cargo
    -> pick up the haul (get cargo from each drone)
    -> sell cargo to any target market

Auto-siphon around gas giant in system
Requirements:
    - Controller continuously acquires up to 8 siphoning drones
    - Drones navigate to gas giant and continuously extract there
    - Controller continuously checks if enough gasses have been extracted in the drone fleet
    - While the fleet has filled & is not yet being served: 
        Continuously acquire a hauler and send it to pick up & sell off the gasses

## Progress report

**2025-11-07**
- IO Framework  : Functional - No immediate attention needed
- Trading       : Functional - No immediate attention needed
- Navigation    : Functional - Improvements to pathing efficiency available
- Mining        : Underdeveloped - Working, but target for development
- Exploration   : Underdeveloped - No dedicated functionality yet

Goals:
- Functionally: 
    - create top-level scripts that can assist in system bootstrapping (market growth, gate construction, cashflow maintenance)
    - create visualisations that facilitate fleet management (profit tracking etc)
- Use-cases:
    - Automated mining : Setting up drones around asteroids that mine continuously, serviced by a hauler that gathers & sells cargo 
    - Trade manager    : System-level orchestrator that has access to multiple traders & manages them simultaneously
    - Fleet management : Framework for managing fleet resources (flagging ship availability & distributing ships to handlers)



## Expansion strategy

1. Phase 1 - Aggressively build funds
    Goals:
     Acquire a fleet to support construction & cash flow
     - Miners around the engineered roid + surveyor + haulers on ore delivery duty
     - Greedy traders + satellite network to quickly cover all profitable trades & grow some markets

2. Phase 2 - Market growth & Gate construction
    Prerequisites:
     Cashflow is drying up (greedy traders are idling)
    Goals:
     Provide base materials (ore & liquids) from miners to markets
     Improve supply of FAB_MATS and ADVANCED_CIRCUITRY
        Sell import materials to those markets, regardless of profitability (but maintain positive cashflow)
     Construct the Gate
        Buy construction materials & deliver to construction site (but don't jeopardize cashflow)

3. Phase 3 - The Final Frontier
    Explore space