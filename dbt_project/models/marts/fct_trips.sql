{{
    config(
        materialized= 'incremental',
        unique_key='tripid',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_tripdata as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
trips as(
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
dim_zones as(
    select * from {{ ref('dim_zones') }}
    where borough!='Unknown'
)

select     -- Trip identifiers
    trips.tripid,
    trips.vendorid,
    trips.service_type,
    trips.ratecodeid,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }} as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.airport_fee,
    trips.improvement_surcharge,
    trips.congestion_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from trips
inner join dim_zones as pz
on trips.pickup_location_id=pz.location_id
inner join dim_zones as dz
on trips.dropoff_location_id=dz.location_id