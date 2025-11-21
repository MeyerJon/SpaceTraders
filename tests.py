import asyncio
from SpaceTraders import F_trade, scripts

async def do_something():
    print("Starting some work.")
    await asyncio.sleep(5)
    print("Still working on something.")
    await asyncio.sleep(5)
    print("Pretty much done now.")
    return True

async def canceltest():
    print("About to start some work.")
    task = asyncio.create_task(do_something())
    await asyncio.sleep(6)
    print("Cancelling task!")
    task.cancel()
    await asyncio.sleep(11)
    print("Goodbye!")

async def main():
    ship = "RYVIOS-6"

    # If there's room for more cargo, keep filling up
    cur_cargo = F_trade.get_ship_cargo(ship)
    if cur_cargo['units'] < cur_cargo['capacity']:
        # Find closest probes with full holds
        drones = scripts.find_nearby_drones(ship)

        if len(drones) > 0:
            # Drain its cargo
            if not await scripts.drain_cargo_from_ship(ship, drones[0]['shipSymbol']):
                print(f"[ERROR] {ship} was unable to drain cargo from {drones[0]['shipSymbol']}.")

    # Otherwise, go sell it off
    else:
        if not await scripts.clear_cargo(ship):
            print(f"[ERROR] {ship} was unable to sell off its collected haul.")


if __name__ == "__main__":
    asyncio.run(main())