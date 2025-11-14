from SpaceTraders.controllers import system_market_intel as MI
from SpaceTraders.controllers import system_siphoners as SIPHON
from SpaceTraders import scripts
import asyncio

if __name__ == "__main__":
    asyncio.run(SIPHON.mine_goods('RYVIOS-4', 'X1-QT34-C39', goods=None))
    #asyncio.run(scripts.fetch_cargo_from_ship('RYVIOS-1', 'RYVIOS-4', 'LIQUID_HYDROGEN'))