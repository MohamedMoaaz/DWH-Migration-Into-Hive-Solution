SET hive.support.concurrency=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;
SET hive.exec.dynamic.partition=true;
SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
SET hive.auto.convert.join=true;
SET parquet.compression=SNAPPY;

CREATE TABLE scd_passengers (
    passenger_key INT,
    passenger_id STRING,
    passenger_national_id STRING,
    passenger_firstname STRING,
    passenger_lastname STRING,
    passenger_dob DATE,
    passenger_nationality STRING,
    passenger_email STRING,
    passenger_phoneno STRING,
    passenger_gender STRING,
    passenger_status STRING,
    effective_date TIMESTAMP,
    expiry_date TIMESTAMP,
    is_current BOOLEAN,
    version_number INT
)
PARTITIONED BY (frequent_flyer_tier STRING)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    "transactional"="true",
    "orc.compress"="SNAPPY",
    "orc.create.index"="true",
    "orc.bloom.filter.columns"="passenger_id,passenger_key"
);

CREATE TABLE scd_promotions (
    promotion_key INT,
    promotion_id STRING,
    promotion_name STRING,
    promotion_target_segment STRING,
    promotion_channel STRING,
    promotion_start_date DATE,
    promotion_end_date DATE,
    discount_value DECIMAL(10,2),
    discount_type STRING,
    max_discount_amount DECIMAL(10,2),
    effective_date TIMESTAMP,
    expiry_date TIMESTAMP,
    is_current BOOLEAN,
    version_number INT
)
PARTITIONED BY (promotion_type STRING)
CLUSTERED BY (promotion_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    "transactional"="true",
    "orc.compress"="SNAPPY",
    "orc.bloom.filter.columns"="promotion_key"
);

CREATE EXTERNAL TABLE fact_flight_reservations_bigtable (
    ticket_id STRING,
    reservation_date DATE,
    departure_date DATE,
    booking_class STRING,
    seat_number STRING,
    promotion_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    operational_fees DECIMAL(10,2),
    cancellation_fees DECIMAL(10,2),
    fare_price DECIMAL(10,2),
    final_price DECIMAL(10,2),
    is_cancelled BOOLEAN,
    cancellation_reason STRING,
    passenger_id STRING,
    passenger_name STRING,
    passenger_dob DATE,
    passenger_nationality STRING,
    passenger_gender STRING,
    frequent_flyer_tier STRING,
    source_airport_code STRING,
    source_airport_name STRING,
    source_city STRING,
    source_country STRING,
    source_region STRING,
    destination_airport_code STRING,
    destination_airport_name STRING,
    destination_city STRING,
    destination_country STRING,
    destination_region STRING,
    fare_basis_code STRING,
    fare_class STRING,
    refundable BOOLEAN,
    baggage_allowance STRING,
    promotion_name STRING,
    promotion_type STRING,
    discount_value DECIMAL(10,2),
    discount_type STRING,
    channel_name STRING,
    channel_type STRING,
    commission_rate DECIMAL(5,2),
    reservation_quarter INT,
    departure_year INT,
    departure_month INT,
    departure_quarter INT,
    is_weekend BOOLEAN
)
PARTITIONED BY (reservation_year INT, reservation_month INT)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION '/data/airline/analytics/reservations_bigtable'
TBLPROPERTIES (
    "parquet.compression"="SNAPPY",
    "parquet.enable.dictionary"="true",
    "parquet.dictionary.page.size"="1048576",
    "parquet.bloom.filter.columns"="passenger_id,ticket_id",
    "auto.purge"="true"
);

INSERT OVERWRITE TABLE fact_flight_reservations_bigtable
PARTITION (reservation_year, reservation_month)
SELECT
    fr.ticket_id,
    CAST(rd.full_date AS DATE) AS reservation_date,
    CAST(dd.full_date AS DATE) AS departure_date,
    fr.booking_class,
    fr.seat_number,
    COALESCE(fr.promotion_amount, 0) AS promotion_amount,
    COALESCE(fr.tax_amount, 0) AS tax_amount,
    COALESCE(fr.operational_fees, 0) AS operational_fees,
    COALESCE(fr.cancelation_fees, 0) AS cancellation_fees,
    COALESCE(fr.fare_price, 0) AS fare_price,
    COALESCE(fr.final_price, 0) AS final_price,
    COALESCE(fr.is_cancelled, false) AS is_cancelled,
    fr.cancellation_reason,
    COALESCE(p.passenger_id, 'UNKNOWN') AS passenger_id,
    COALESCE(CONCAT(p.passenger_firstname, ' ', p.passenger_lastname), 'Unknown Passenger') AS passenger_name,
    p.passenger_dob,
    COALESCE(p.passenger_nationality, 'Unknown') AS passenger_nationality,
    COALESCE(p.passenger_gender, 'U') AS passenger_gender,
    COALESCE(p.frequent_flyer_tier, 'NONE') AS frequent_flyer_tier,
    sa.airport_code AS source_airport_code,
    sa.airport_name AS source_airport_name,
    sa.airport_city AS source_city,
    sa.airport_country AS source_country,
    sa.airport_region AS source_region,
    da.airport_code AS destination_airport_code,
    da.airport_name AS destination_airport_name,
    da.airport_city AS destination_city,
    da.airport_country AS destination_country,
    da.airport_region AS destination_region,
    fb.fare_basis_code,
    fb.fare_class,
    COALESCE(fb.refundable, false) AS refundable,
    COALESCE(fb.baggage_allowance, '0') AS baggage_allowance,
    pr.promotion_name,
    pr.promotion_type,
    COALESCE(pr.discount_value, 0) AS discount_value,
    COALESCE(pr.discount_type, 'None') AS discount_type,
    sc.channel_name,
    sc.channel_type,
    COALESCE(sc.commission_rate, 0) AS commission_rate,
    QUARTER(rd.full_date) AS reservation_quarter,
    YEAR(dd.full_date) AS departure_year,
    MONTH(dd.full_date) AS departure_month,
    QUARTER(dd.full_date) AS departure_quarter,
    COALESCE(rd.is_weekend, false) AS is_weekend,
    fr.reservation_year,
    fr.reservation_month
FROM dwh_staging.fact_reservations fr
LEFT JOIN scd_passengers p ON fr.passenger_key = p.passenger_key AND p.is_current = true
LEFT JOIN dwh_staging.dim_airports sa ON fr.source_airport = sa.airport_key
LEFT JOIN dwh_staging.dim_airports da ON fr.destination_airport = da.airport_key
LEFT JOIN dwh_staging.dim_fare_basis_codes fb ON fr.fare_basis_key = fb.fare_basis_key
LEFT JOIN scd_promotions pr ON fr.promotion_key = pr.promotion_key AND pr.is_current = true
LEFT JOIN dwh_staging.dim_sales_channels sc ON fr.channel_key = sc.channel_key
LEFT JOIN dwh_staging.dim_date rd ON fr.reservation_date_key = rd.date_key
LEFT JOIN dwh_staging.dim_date dd ON fr.departure_date_key = dd.date_key
WHERE fr.reservation_year BETWEEN 2020 AND YEAR(CURRENT_DATE) + 1
  AND fr.reservation_month BETWEEN 1 AND 12;