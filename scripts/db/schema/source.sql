CREATE TABLE reservations (
    reservation_id VARCHAR(20) PRIMARY KEY,
    ticket_number VARCHAR(20) NOT NULL,
    passenger_id VARCHAR(20) NOT NULL,
    channel_id INT NOT NULL,
    promotion_id VARCHAR(20),
    fare_basis_id VARCHAR(10) NOT NULL,
    flight_id VARCHAR(20) NOT NULL,
    booking_date TIMESTAMP NOT NULL,
    departure_date TIMESTAMP NOT NULL,
    booking_class VARCHAR(2) NOT NULL,
    seat_number VARCHAR(10),
    promotion_amount NUMERIC(10,2) DEFAULT 0.00,
    tax_amount NUMERIC(10,2) NOT NULL,
    operational_fees NUMERIC(10,2) NOT NULL,
    cancellation_fees NUMERIC(10,2) DEFAULT 0.00,
    fare_price NUMERIC(10,2) NOT NULL,
    final_price NUMERIC(10,2) NOT NULL,
    is_cancelled BOOLEAN DEFAULT FALSE,
    cancellation_reason VARCHAR(100),
    cancellation_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (passenger_id) REFERENCES passengers(passenger_id),
    FOREIGN KEY (channel_id) REFERENCES sales_channels(channel_id),
    FOREIGN KEY (promotion_id) REFERENCES promotions(promotion_id),
    FOREIGN KEY (fare_basis_id) REFERENCES fare_basis_codes(fare_basis_id),
    FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
);

CREATE TABLE flights (
    flight_id VARCHAR(20) PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    aircraft_id VARCHAR(20) NOT NULL,
    departure_airport VARCHAR(5) NOT NULL,
    arrival_airport VARCHAR(5) NOT NULL,
    scheduled_departure TIMESTAMP NOT NULL,
    scheduled_arrival TIMESTAMP NOT NULL,
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,
    flight_status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(aircraft_id),
    FOREIGN KEY (departure_airport) REFERENCES airports(airport_code),
    FOREIGN KEY (arrival_airport) REFERENCES airports(airport_code)
);

CREATE TABLE passengers (
    passenger_id VARCHAR(20) NOT NULL,
    national_id VARCHAR(30) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    nationality VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    phone_number VARCHAR(20),
    gender CHAR(1),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    frequent_flyer_number VARCHAR(20),
    frequent_flyer_tier VARCHAR(20),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (passenger_id)
);

CREATE TABLE sales_channels (
    channel_id INT PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL,
    channel_type VARCHAR(30) NOT NULL,
    category VARCHAR(20) NOT NULL CHECK (category IN ('ONLINE', 'OFFLINE')),
    commission_rate NUMERIC(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE promotions (
    promotion_id VARCHAR(20) NOT NULL,
    promotion_name VARCHAR(100) NOT NULL,
    promotion_type VARCHAR(30) NOT NULL,
    target_segment VARCHAR(30),
    channel VARCHAR(30),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    discount_value NUMERIC(10,2) NOT NULL,
    discount_type VARCHAR(10) NOT NULL CHECK (discount_type IN ('PERCENTAGE', 'FIXED')),
    max_discount_amount NUMERIC(10,2),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (promotion_id)
);

CREATE TABLE airports (
    airport_code VARCHAR(5) PRIMARY KEY,
    airport_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    timezone VARCHAR(50),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    runway_count INT,
    size_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fare_basis_codes (
    fare_basis_id VARCHAR(10) PRIMARY KEY,
    fare_basis_code VARCHAR(10) NOT NULL UNIQUE,
    fare_class VARCHAR(2) NOT NULL,
    is_refundable BOOLEAN NOT NULL,
    is_changeable BOOLEAN NOT NULL,
    description VARCHAR(200),
    baggage_allowance VARCHAR(100),
    meal_included BOOLEAN,
    upgrade_eligible BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE aircraft (
    aircraft_id VARCHAR(20) PRIMARY KEY,
    model VARCHAR(50) NOT NULL,
    manufacturer VARCHAR(50) NOT NULL,
    total_capacity INT NOT NULL,
    economy_seats INT NOT NULL,
    business_seats INT NOT NULL,
    first_class_seats INT NOT NULL,
    manufacture_year INT,
    fuel_efficiency NUMERIC(5,2),
    maintenance_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);