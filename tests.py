import asyncio
from SpaceTraders import F_trade, scripts, fleet_resource_manager
from SpaceTraders.controllers import system_traders as TRADERS
from SpaceTraders.controllers import system_miners as MINERS

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

    await MINERS.haul_yields_in_system('X1-GS33', max_haulers=3)
    


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("Unhandled exception:")
        raise e
    except KeyboardInterrupt as e:
        print("Exiting.")
    finally:
        fleet_resource_manager.release_fleet('EXTRACTION-CONTROLLER-HAULERS-X1-GS33', force=True)