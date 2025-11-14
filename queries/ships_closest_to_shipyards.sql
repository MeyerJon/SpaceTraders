-- Ships closest to shipyard
select distinct
sy.shipyardSymbol,
sy.type,
sy.purchasePrice,
sy.supply,
nav.symbol as shipSymbol,
dists.dist as distance
from 'shipyard.SHIPS' sy

inner join WP_DISTANCES dists
on sy.shipyardSymbol = dists.dst

inner join 'ship.NAV' nav
on dists.src = nav.waypointSymbol

-- Filter by (shipyard) type & optionally by shipSymbol for use in selections
where 1=1
and sy.type = "SHIP_SIPHON_DRONE"

order by dists.dist asc