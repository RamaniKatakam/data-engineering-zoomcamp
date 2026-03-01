
with source as (
    select * ,
    row_number () over( partition by vendorid, lpep_pickup_datetime,lpep_dropoff_datetime order by lpep_pickup_datetime)
    as rn
    from {{ source('staging', 'green_tripdata_2019_2020_clean') }}
    where vendorid is not null
),

renamed as (
    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['vendorid','lpep_pickup_datetime','lpep_dropoff_datetime','trip_distance',"'Green'"]) }} as tripid,
        cast(vendorid as INT64) as vendorid,
        {{ safe_cast('ratecodeid', 'INT64') }} as ratecodeid,
        cast(pulocationid as INT64) as pickup_location_id,
        cast(dolocationid as INT64) as dropoff_location_id,

        -- timestamps
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as INT64) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        {{ safe_cast('trip_type' , 'INT64') }} as trip_type,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(null as numeric) as airport_fee,
        cast(ehail_fee as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(null as numeric) as congestion_surcharge,
        {{ safe_cast('payment_type', 'INT64') }} as payment_type,
        {{ get_payment_type_description('payment_type')}} as payment_type_description

    from source
    where rn=1

)

select * from renamed

-- Sample records for dev environment using deterministic date filter
/*{% if target.name != 'prod' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}*/