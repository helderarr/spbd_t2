-- Question 2
-- For each pickup region, report the list of unique dropoff regions?
-- Output is expected to have two columns: (pickup_region_ID, list_of_dropoff_region_ID).
select PickupRegionID as pickup_region_ID,
    collect_set(DropoffRegionID) as list_of_dropoff_region_ID
from   (select distinct PickupRegionID,DropoffRegionID  from taxi_trips
        where PickupRegionID is not null and DropoffRegionID is not null) x
group by PickupRegionID;
