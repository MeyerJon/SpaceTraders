from SpaceTraders.controllers import system_market_intel as MI
from SpaceTraders.controllers import system_siphoners as SIPHON
from SpaceTraders import scripts, io
import asyncio

async def siphoner_squad(haulers, drones, goods=None):
    gas_giant_wp = 'X1-ZZ30-C43'
    haulers = [asyncio.create_task(scripts.haul_ore(h)) for h in haulers]
    drones = [asyncio.create_task(SIPHON.mine_goods(d, gas_giant_wp, goods=goods)) for d in drones]
    tasks = haulers + drones
    await asyncio.gather(*tasks)

async def satellite_squad():
    await MI.maintain_tradegood_data('X1-ZZ30', 60*10, mode='no_exchanges')

async def greedy_squad(haulers):
    haulers = [asyncio.create_task(scripts.naive_trader(h)) for h in haulers]
    await asyncio.gather(*haulers)

async def booster_squad(haulers):
    haulers = [asyncio.create_task(scripts.boost_good_growth(h, 'X1-ZZ30', ["FAB_MATS", "ADVANCED_CIRCUITRY"])) for h in haulers]
    await asyncio.gather(*haulers)

async def main():
    await asyncio.gather(
        siphoner_squad(haulers=['RYVIOS-6'], drones=['RYVIOS-5', 'RYVIOS-7', 'RYVIOS-9'], goods=["LIQUID_NITROGEN", "LIQUID_HYDROGEN"]),
        greedy_squad(haulers=["RYVIOS-4", "RYVIOS-8"]),
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