from SpaceTraders import scripts
from SpaceTraders.controllers import system_market_intel as MI
import asyncio

if __name__ == "__main__":
    asyncio.run(MI.maintain_tradegood_data('X1-QT34', 5, mode="no_exchanges"))