-- Filter the markets by desired traits & select update times in refresh window
-- This part can be replaced by a different 'market selection' query depending on the controller's mode/objectives
with market_update_times as (
select
	distinct marketSymbol,
	ts_created,
	strftime('%s', 'now') - ts_created as time_since_update,
	datetime(ts_created, 'unixepoch', 'localtime') as last_update
from tradegoods_current
group by marketSymbol
having sum(type = "IMPORT") > 0
   and sum(type = "EXPORT") > 0
   and ts_created < (strftime('%s', 'now') - (60*5))
)

select
    nav.waypointSymbol as ship_wp
    ,next_mkt.marketSymbol
    ,next_mkt.time_since_update
    ,round(wp_dists.dist) as distance
    -- Score weighs 'outdatedness' and distance almost equally, but prefers closer waypoints
    ,wp_dists.dist + (wp_dists.dist * ((select max(time_since_update) from market_update_times) - time_since_update)) as score
    ,nav.symbol

-- Start from all waypoint distances
from WP_DISTANCES wp_dists

-- Add current locations of ships
inner join 'ship.NAV' nav
on nav.waypointSymbol = wp_dists.src
and nav.symbol in ("RYVIOS-2", "RYVIOS-3")

-- Add market locations & update times 
inner join market_update_times next_mkt
on wp_dists.dst = next_mkt.marketSymbol

order by score asc
 