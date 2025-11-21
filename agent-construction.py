from SpaceTraders import scripts, io
import asyncio


async def main():
    await scripts.construction_loop('RYVIOS-A', 60*50)


if __name__ == "__main__":
    asyncio.run(main())