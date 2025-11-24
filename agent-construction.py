from SpaceTraders import scripts, io
import asyncio


async def main():
    #await scripts.boost_good_growth('RYVIOS-A', 'X1-ZZ30', goods=["FAB_MATS"])
    await scripts.construction_loop('RYVIOS-A', 60*10)


if __name__ == "__main__":
    asyncio.run(main())