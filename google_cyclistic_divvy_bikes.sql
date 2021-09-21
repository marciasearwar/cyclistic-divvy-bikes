/*

This is the SQL script used to import, compile and analyse the Cyclistic Case 
Study for the Capstone Project of the Google Data Analytics Professional Certificate.

It uses the data from Divvy Bikes (https://www.divvybikes.com/system-data).

The database used is Oracle Database 18c Express Edition.

The steps followed:

1. Download the individual CSV documents
2. Import each into separate tables, regularising data types
3. Combine all the data into one table
4. Inspect data for anomalies
5. Identify and exclude data with anomalies
6. Create queries for data visualisations

The code below starts at step 3.

*/

----Step 3: Combine all the data into one table---------------------------------


drop table divvy_tripdata;

create table divvy_tripdata as
(select *
from divvy_tripdata_202008

union

select *
from divvy_tripdata_202009
union

select *
from divvy_tripdata_202010
union

select *
from divvy_tripdata_202011
union

select *
from divvy_tripdata_202012
union

select *
from divvy_tripdata_202101
union

select *
from divvy_tripdata_202102
union

select *
from divvy_tripdata_202103
union

select *
from divvy_tripdata_202104
union

select *
from divvy_tripdata_202105
union

select *
from divvy_tripdata_202106
union

select *
from divvy_tripdata_202107
union

select *
from divvy_tripdata_202108
);


select count(1)
from divvy_tripdata;

--calculate trip length
select ended_at, started_at, (ended_at - started_at) * 1440, round((ended_at - started_at) * 1440) 
from divvy_tripdata;

update divvy_tripdata
set trip_length_mins = round((ended_at - started_at) * 1440);


----Step 4: Inspect data for anomalies------------------------------------------

select distinct member_casual
from divvy_tripdata;

select min (end_lng), max(end_lng), min (end_lat), max(end_lat), min (start_lng), max(start_lng), min (start_lat), max(start_lat)
from divvy_tripdata;

select end_station_id, end_station_name, count(1)
from divvy_tripdata
group by end_station_id, end_station_name;

select rideable_type, count(1)
from divvy_tripdata
group by rideable_type;

select ride_id, count(1)
from divvy_tripdata
group by ride_id
having count(1) > 1;

select *
from divvy_tripdata
where started_at is null and ended_at is null;

select *
from divvy_tripdata
where started_at is null or ended_at is null;

select *
from divvy_tripdata
where exclude is null
and (start_lat is null or end_lat is null);

select *
from divvy_tripdata
where exclude is null
and (start_lng is null or end_lng is null);

select *
from divvy_tripdata
where exclude is null
and member_casual is null;

select *
from divvy_tripdata
where exclude is null
and rideable_type is null;

----Step 5: Identify and exclude data with anomalies----------------------------


--exclude cases where start time is greater than end time
select count(1)
from divvy_tripdata
where started_at >= ended_at;

update divvy_tripdata
set exclude = 'Y'
where started_at >= ended_at;


--exclude cases where trip length less than or equal to 0
select *
from divvy_tripdata
order by trip_length_mins asc;

update divvy_tripdata
set exclude = 'Y'
where trip_length_mins <= 0;

select *
from divvy_tripdata
where exclude is null
order by trip_length_mins asc;

select trip_length_mins, count(1)
from divvy_tripdata
where exclude is null
group by trip_length_mins;


--checking on start and end stations
select count(1)
from divvy_tripdata
where start_station_name is null and end_station_name is null;

select *
from divvy_tripdata
where start_station_name is null or end_station_name is null;


update divvy_tripdata
set exclude = 'Y'
where start_station_name is null or end_station_name is null;

--checking station names
select start_station_name, count(1)
from divvy_tripdata
where exclude is null
group by start_station_name;

select *
from divvy_tripdata
where exclude is null
and (upper(start_station_name) like '%BASE%WAREHOUSE%' or upper(end_station_name) like '%BASE%WAREHOUSE%');

update divvy_tripdata
set exclude = 'Y'
where exclude is null
and (upper(start_station_name) like '%BASE%WAREHOUSE%' or upper(end_station_name) like '%BASE%WAREHOUSE%');


----Step 6: Create queries for data visualisations------------------------------

select count(1)
from divvy_tripdata
where exclude is null;

select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
from divvy_tripdata
where exclude is null;

-------------

with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null)
            
    select member_casual, count(id)
    from dt
    group by member_casual;
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select member_casual, to_char(started_at, 'mon-yyyy'), count(id)
    from dt
    group by member_casual, to_char(started_at, 'mon-yyyy');       
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select member_casual, to_char(started_at, 'day'), count(id)
    from dt
    group by member_casual, to_char(started_at, 'day');     
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select member_casual, count(id)
    from dt
    where trip_length_mins >= 60 and trip_length_mins <= 119
    group by member_casual;
    
----------------


with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select *
    from 
    (
        select 'Origin' origin_destination, start_station_name station_name, id path_id, start_lat lat, start_lng lng, member_casual
        from dt
        
        union
        
        select 'Destination' origin_destination, end_station_name station_name, id path_id, end_lat lat, end_lng lng, member_casual
        from dt
    )
    order by path_id;
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select rownum, i.*
    from (
           select member_casual, start_station_name, end_station_name, num, rank() over (partition by member_casual order by num desc) num_rank
           from (
            select member_casual, start_station_name, end_station_name, count(1) num
            from dt
            group by member_casual,  start_station_name, end_station_name
            ) 
    ) i
    where num_rank <= 30;
--    order by num_rank
  
--  fetch first 100 rows only
  
set define off;

with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
    select distinct round(start_lat, 2), round(start_lng, 2)
    from dt
    where start_station_name = 'Streeter Dr & Grand Ave';
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from divvy_tripdata
            where exclude is null) 
            
            select member_casual, start_station_name, end_station_name, count(1) num
            from dt
            group by member_casual,  start_station_name, end_station_name    ;