from SpaceTraders.controllers import system_market_intel as MI
from SpaceTraders.controllers import system_miners as MINERS
from SpaceTraders.controllers import system_traders as TRADERS
from SpaceTraders import scripts, io
import asyncio

async def siphoner_squad(haulers, drones, goods=None):
    gas_giant_wp = 'X1-ZZ30-C43'
    haulers = [asyncio.create_task(scripts.haul_ore(h)) for h in haulers]
    drones = [asyncio.create_task(MINERS.mine_goods(d, gas_giant_wp, goods=goods)) for d in drones]
    tasks = haulers + drones
    await asyncio.gather(*tasks)

async def satellite_squad():
    await MI.maintain_tradegood_data('X1-SR92', 60*3, mode='no_fuel')

async def greedy_squad(n_haulers):
    await TRADERS.trade_in_system('X1-SR92', n_haulers, "greedy")

async def booster_squad(n_haulers):
    haulers = [asyncio.create_task(scripts.naive_trader(h)) for h in haulers]
    await asyncio.gather(*haulers)

async def main():
    await asyncio.gather(
        #siphoner_squad(haulers=['RYVIOS-6', 'RYVIOS-D'], drones=['RYVIOS-5', 'RYVIOS-7', 'RYVIOS-9', 'RYVIOS-E'], goods=["LIQUID_NITROGEN", "LIQUID_HYDROGEN"]),
        greedy_squad(n_haulers=4),
        satellite_squad()
    )

def shutdown():
    io.update_records_custom("""UPDATE 'control.SHIP_LOCKS' 
                                SET 
                                    controller = NULL,
                                    priority = -1,
                                    blocked = 0
                                WHERE
                                    controller <> "USER"
                                    and priority < 10000
                            """)

if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        print(f"[INFO] KeyboardInterrupt caught. Releasing ships & shutting down.")
    except Exception as e:
        print(f"[ERROR] Uncaught exception. Shutting down.")
        print(e)
        io.log_exception(e)
    finally:
        shutdown()
    
    #asyncio.run(SIPHON.mine_goods('RYVIOS-5', 'X1-ZZ30-C43', goods=None))
    #asyncio.run(scripts.fetch_cargo_from_ship('RYVIOS-1', 'RYVIOS-4', 'LIQUID_HYDROGEN'))
    #asyncio.run(MI.maintain_tradegood_data('X1-ZZ30', refresh_freq=60*30, mode="no_exchanges"))