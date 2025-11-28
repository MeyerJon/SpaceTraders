import asyncio
from SpaceTraders import F_trade, scripts
from SpaceTraders.controllers import system_traders as TRADERS

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

    await TRADERS.trade_in_system('X1-SR92', 4)
    


if __name__ == "__main__":
    asyncio.run(main())