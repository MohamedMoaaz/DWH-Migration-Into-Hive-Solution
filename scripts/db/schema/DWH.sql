-- FACT TABLE: fact_reservation
CREATE TABLE fact_reservations (
    ticket_id               VARCHAR,
    channel_key             INT,
    promotion_key           INT,
    passenger_key           INT,
    fare_basis_key          INT,
    aircraft_key            INT,
    source_airport          INT,
    destination_airport     INT,
    reservation_date_key    INT,  
    departure_date_key      INT,
    booking_class           VARCHAR,
    seat_number             VARCHAR,
    promotion_amount        NUMERIC(10,2),
    tax_amount              NUMERIC(10,2),
    operational_fees        NUMERIC(10,2),
    cancelation_fees        NUMERIC(10,2),
    fare_price              NUMERIC(10,2),
    final_price             NUMERIC(10,2),
    is_cancelled            BOOLEAN,
    cancellation_reason     VARCHAR,
    reservation_year        INT,
    reservation_month       INT
);

-- DIMENSION TABLE: PASSENGER
CREATE TABLE dim_passengers (
    passenger_key           SERIAL PRIMARY KEY,
    passenger_id            VARCHAR,
    passenger_national_id   VARCHAR,
    passenger_firstname     VARCHAR,
    passenger_lastname      VARCHAR,
    passenger_dob           DATE,
    passenger_nationality   VARCHAR,
    passenger_email         VARCHAR,
    passenger_phoneno       VARCHAR,
    passenger_gender        VARCHAR,
    passenger_status        VARCHAR,
    frequent_flyer_tier     VARCHAR,
    effective_date          DATE,
    expiry_date             DATE,
    is_current              BOOLEAN
);

-- DIMENSION TABLE: PROMOTIONS
CREATE TABLE dim_promotions (
    promotion_key           SERIAL PRIMARY KEY,
    promotion_id            VARCHAR,
    promotion_name          VARCHAR,
    promotion_type          VARCHAR,
    promotion_target_segment VARCHAR,
    promotion_channel       VARCHAR,
    promotion_start_date    DATE,
    promotion_end_date      DATE,
    discount_value          NUMERIC(10,2),
    discount_type           VARCHAR,
    max_discount_amount     NUMERIC(10,2),
    effective_date          DATE,
    expiry_date             DATE,
    is_current              BOOLEAN,
    promotion_year          INT
);

-- DIMENSION TABLE: AIRPORT
CREATE TABLE dim_airports (
    airport_key             SERIAL PRIMARY KEY,
    airport_id              VARCHAR,
    airport_name            VARCHAR,
    airport_code            VARCHAR,
    airport_city            VARCHAR,
    airport_country         VARCHAR,
    airport_region          VARCHAR,
    airport_timezone        VARCHAR,
    airport_latitude        DOUBLE PRECISION,
    airport_longitude       DOUBLE PRECISION,
    airport_no_of_runways   INT,
    airport_size_category   VARCHAR
);

-- DIMENSION TABLE: DATE
CREATE TABLE dim_date (
    date_key                INT PRIMARY KEY,
    full_date               DATE,
    day_number              INT,
    day_name                VARCHAR,
    month_name              VARCHAR,
    year_no                 INT,
    quarter                 INT,
    week_of_year            INT,
    is_weekend              BOOLEAN,
    is_holiday              BOOLEAN,
    holiday_name            VARCHAR
);

-- DIMENSION TABLE: FARE BASIS
CREATE TABLE dim_fare_basis_codes (
    fare_basis_key          SERIAL PRIMARY KEY,
    fare_basis_code         VARCHAR,
    fare_class              VARCHAR,
    refundable              BOOLEAN,
    changeable              BOOLEAN,
    fare_description        VARCHAR,
    baggage_allowance       VARCHAR,
    meal_included           BOOLEAN,
    upgrade_eligible        BOOLEAN
);

-- DIMENSION TABLE: CHANNEL
CREATE TABLE dim_sales_channels (
    channel_key             SERIAL PRIMARY KEY,
    channel_name            VARCHAR,
    channel_type            VARCHAR,
    channel_category        VARCHAR,
    commission_rate         NUMERIC(5,2),
    is_active               BOOLEAN
);

-- DIMENSION TABLE: AIRCRAFT
CREATE TABLE dim_aircraft (
    aircraft_key            SERIAL PRIMARY KEY,
    aircraft_model          VARCHAR,
    aircraft_manufacturer   VARCHAR,
    aircraft_capacity       INT,
    economy_seats           INT,
    business_seats          INT,
    firstclass_seats        INT,
    aircraft_age            INT,
    fuel_efficiency         NUMERIC(5,2),
    maintenance_status      VARCHAR
);