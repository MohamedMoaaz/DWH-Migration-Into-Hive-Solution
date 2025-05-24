-- Passengers table
CREATE EXTERNAL TABLE passengers (
    passenger_id STRING,
    national_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    nationality STRING,
    email STRING,
    phone_number STRING,
    gender STRING,
    status STRING,
    frequent_flyer_number STRING,
    frequent_flyer_tier STRING,
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/passengers'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Sales Channels table
CREATE EXTERNAL TABLE sales_channels (
    channel_id INT,
    channel_name STRING,
    channel_type STRING,
    category STRING,
    commission_rate DECIMAL(5,2),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/sales_channels'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Promotions table
CREATE EXTERNAL TABLE promotions (
    promotion_id STRING,
    promotion_name STRING,
    promotion_type STRING,
    target_segment STRING,
    channel STRING,
    start_date DATE,
    end_date DATE,
    discount_value DECIMAL(10,2),
    discount_type STRING,
    max_discount_amount DECIMAL(10,2),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/promotions'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Airports table
CREATE EXTERNAL TABLE airports (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    country STRING,
    region STRING,
    timezone STRING,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    runway_count INT,
    size_category STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/airports'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Fare Basis Codes table
CREATE EXTERNAL TABLE fare_basis_codes (
    fare_basis_id STRING,
    fare_basis_code STRING,
    fare_class STRING,
    is_refundable BOOLEAN,
    is_changeable BOOLEAN,
    description STRING,
    baggage_allowance STRING,
    meal_included BOOLEAN,
    upgrade_eligible BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/fare_basis_codes'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Aircraft table
CREATE EXTERNAL TABLE aircraft (
    aircraft_id STRING,
    model STRING,
    manufacturer STRING,
    total_capacity INT,
    economy_seats INT,
    business_seats INT,
    first_class_seats INT,
    manufacture_year INT,
    fuel_efficiency DECIMAL(5,2),
    maintenance_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/aircraft'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Flights table
CREATE EXTERNAL TABLE flights (
    flight_id STRING,
    flight_number STRING,
    aircraft_id STRING,
    departure_airport STRING,
    arrival_airport STRING,
    scheduled_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,
    flight_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/flights'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Reservations table
CREATE EXTERNAL TABLE reservations (
    reservation_id STRING,
    ticket_number STRING,
    passenger_id STRING,
    channel_id INT,
    promotion_id STRING,
    fare_basis_id STRING,
    flight_id STRING,
    booking_date TIMESTAMP,
    departure_date TIMESTAMP,
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
    cancellation_date TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/source_staging.db/reservations'
TBLPROPERTIES ("skip.header.line.count"="1");