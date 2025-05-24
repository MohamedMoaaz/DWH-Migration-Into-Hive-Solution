-- FACT TABLE: fact_reservation (CSV)
CREATE EXTERNAL TABLE fact_reservations (
    ticket_id               STRING,
    channel_key             INT,
    promotion_key           INT,
    passenger_key           INT,
    fare_basis_key          INT,
    aircraft_key            INT,
    source_airport          INT,
    destination_airport     INT,
    reservation_date_key    INT,  
    departure_date_key      INT,
    booking_class           STRING,
    seat_number             STRING,
    promotion_amount        DECIMAL(10,2),
    tax_amount              DECIMAL(10,2),
    operational_fees        DECIMAL(10,2),
    cancelation_fees        DECIMAL(10,2),
    fare_price              DECIMAL(10,2),
    final_price             DECIMAL(10,2),
    is_cancelled            BOOLEAN,
    cancellation_reason     STRING,
    reservation_year        INT,
    reservation_month       INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/fact_reservations'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: PASSENGER (CSV)
CREATE EXTERNAL TABLE dim_passengers (
    passenger_key           INT,
    passenger_id            STRING,
    passenger_national_id   STRING,
    passenger_firstname     STRING,
    passenger_lastname      STRING,
    passenger_dob           STRING,
    passenger_nationality   STRING,
    passenger_email         STRING,
    passenger_phoneno       STRING,
    passenger_gender        STRING,
    passenger_status        STRING,
    frequent_flyer_tier     STRING,
    effective_date          STRING,
    expiry_date             STRING,
    is_current             BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_passengers'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: PROMOTIONS (CSV)
CREATE EXTERNAL TABLE dim_promotions (
    promotion_key           INT,
    promotion_id            STRING,
    promotion_name          STRING,
    promotion_type          STRING,
    promotion_target_segment STRING,
    promotion_channel       STRING,
    promotion_start_date    STRING,
    promotion_end_date      STRING,
    discount_value          DECIMAL(10,2),
    discount_type           STRING,
    max_discount_amount     DECIMAL(10,2),
    effective_date          STRING,
    expiry_date             STRING,
    is_current             BOOLEAN,
    promotion_year          INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_promotions'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: AIRPORT (CSV)
CREATE EXTERNAL TABLE dim_airports (
    airport_key             INT,
    airport_id              STRING,
    airport_name            STRING,
    airport_code            STRING,
    airport_city            STRING,
    airport_country         STRING,
    airport_region          STRING,
    airport_timezone        STRING,
    airport_latitude        DOUBLE,
    airport_longitude       DOUBLE,
    airport_no_of_runways   INT,
    airport_size_category   STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_airports'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: DATE (CSV)
CREATE EXTERNAL TABLE dim_date (
    date_key                INT,
    full_date               STRING,
    day_number              INT,
    day_name                STRING,
    month_name              STRING,
    year_no                 INT,
    quarter                 INT,
    week_of_year            INT,
    is_weekend             BOOLEAN,
    is_holiday             BOOLEAN,
    holiday_name            STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_date'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: FARE BASIS (CSV)
CREATE EXTERNAL TABLE dim_fare_basis_codes (
    fare_basis_key          INT,
    fare_basis_code         STRING,
    fare_class              STRING,
    refundable             BOOLEAN,
    changeable             BOOLEAN,
    fare_description        STRING,
    baggage_allowance       STRING,
    meal_included          BOOLEAN,
    upgrade_eligible       BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_fare_basis_codes'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: CHANNEL (CSV)
CREATE EXTERNAL TABLE dim_sales_channels (
    channel_key             INT,
    channel_name            STRING,
    channel_type            STRING,
    channel_category        STRING,
    commission_rate         DECIMAL(5,2),
    is_active              BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_sales_channels'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DIMENSION TABLE: AIRCRAFT (CSV)
CREATE EXTERNAL TABLE dim_aircraft (
    aircraft_key            INT,
    aircraft_model          STRING,
    aircraft_manufacturer   STRING,
    aircraft_capacity       INT,
    economy_seats           INT,
    business_seats          INT,
    firstclass_seats        INT,
    aircraft_age            INT,
    fuel_efficiency         DECIMAL(5,2),
    maintenance_status      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/airline/dim_aircraft'
TBLPROPERTIES ("skip.header.line.count"="1");