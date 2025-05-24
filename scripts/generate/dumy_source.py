import os
import random
# import csv
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from collections import defaultdict

# Initialize Faker and seeds
fake = Faker()
Faker.seed(0)
random.seed(0)

# 🔥 CONFIGURATION
NUM_ROWS = 50000
OUTPUT_DIR = 'data/OLTP'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Data storage for referential integrity
data_store = defaultdict(list)

# ---------------------- Helper Functions ----------------------
def random_date(start, end):
    """Generate random date between start and end"""
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def random_datetime(start, end):
    """Generate random datetime between start and end"""
    return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------- aircraft.csv ----------------------
def generate_aircraft():
    """Generate aircraft data"""
    print("🔥 Generating aircraft data...")
    aircraft_data = []
    models = ['B737', 'A320', 'B787', 'A350', 'B777', 'A380']
    
    for i in range(50):
        aircraft_id = f"AC{str(i+1).zfill(4)}"
        model = random.choice(models)
        manufacturer = 'Boeing' if model.startswith('B') else 'Airbus'
        total_capacity = random.randint(100, 500)
        economy = int(total_capacity * 0.8)
        business = int(total_capacity * 0.15)
        first_class = total_capacity - economy - business
        
        aircraft_data.append({
            "aircraft_id": aircraft_id,
            "model": model,
            "manufacturer": manufacturer,
            "total_capacity": total_capacity,
            "economy_seats": economy,
            "business_seats": business,
            "first_class_seats": first_class,
            "manufacture_year": random.randint(1990, 2023),
            "fuel_efficiency": round(random.uniform(2.5, 5.5), 2),
            "maintenance_status": random.choice(['Good', 'Maintenance Required', 'Excellent']),
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        })
        data_store['aircraft'].append(aircraft_id)
    
    pd.DataFrame(aircraft_data).to_csv(f'{OUTPUT_DIR}/aircraft.csv', index=False)

# ---------------------- airports.csv ----------------------
def generate_airports():
    """Generate airports data"""
    print("🔥 Generating airports data...")
    airports_data = []
    used_codes = set()
    
    for i in range(50):
        while True:
            code = fake.unique.bothify(text='???').upper()
            if code not in used_codes:
                used_codes.add(code)
                break
        
        airports_data.append({
            "airport_code": code,
            "airport_name": f"{fake.city()} International Airport",
            "city": fake.city(),
            "country": fake.country()[:50],
            "region": fake.state(),
            "timezone": fake.timezone(),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6),
            "runway_count": random.randint(1, 4),
            "size_category": random.choice(['Small', 'Medium', 'Large']),
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        })
        data_store['airports'].append(code)
    
    pd.DataFrame(airports_data).to_csv(f'{OUTPUT_DIR}/airports.csv', index=False)

# ---------------------- sales_channels.csv ----------------------
def generate_sales_channels():
    """Generate sales channels data"""
    print("🔥 Generating sales channels data...")
    channels_data = [
        {
            "channel_id": 1,
            "channel_name": "Online Website",
            "channel_type": "Website",
            "category": "ONLINE",
            "commission_rate": round(random.uniform(5.0, 15.0), 2),
            "is_active": True,
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        },
        {
            "channel_id": 2,
            "channel_name": "Airport Counter",
            "channel_type": "Counter",
            "category": "OFFLINE",
            "commission_rate": round(random.uniform(3.0, 10.0), 2),
            "is_active": True,
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        }
    ]
    
    for channel in channels_data:
        data_store['sales_channels'].append(channel["channel_id"])
    
    pd.DataFrame(channels_data).to_csv(f'{OUTPUT_DIR}/sales_channels.csv', index=False)

# ---------------------- promotions.csv ----------------------
def generate_promotions():
    """Generate promotions data"""
    print("🔥 Generating promotions data...")
    promotions_data = []
    
    for i in range(20):
        promo_id = f"PR{str(i+1).zfill(4)}"
        discount_type = random.choice(['PERCENTAGE', 'FIXED'])
        start_date = random_date(datetime(2020,1,1), datetime(2023,1,1))
        end_date = random_date(datetime.strptime(start_date, "%Y-%m-%d"), datetime(2023,12,31))
        
        promotions_data.append({
            "promotion_id": promo_id,
            "promotion_name": f"{random.choice(['Summer', 'Winter', 'Spring', 'Fall'])} Sale",
            "promotion_type": random.choice(['Seasonal', 'Flash', 'Loyalty']),
            "target_segment": random.choice(['All', 'Business', 'Leisure'])[:30],
            "channel": random.choice(['ONLINE', 'OFFLINE', 'BOTH']),
            "start_date": start_date,
            "end_date": end_date,
            "discount_value": round(random.uniform(5, 30), 2) if discount_type == 'PERCENTAGE' else round(random.uniform(20, 200), 2),
            "discount_type": discount_type,
            "max_discount_amount": round(random.uniform(50, 300), 2) if discount_type == 'PERCENTAGE' else None,
            "effective_date": start_date,
            "expiry_date": end_date,
            "is_current": True,
            "created_at": random_date(datetime(2020,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        })
        data_store['promotions'].append(promo_id)
    
    pd.DataFrame(promotions_data).to_csv(f'{OUTPUT_DIR}/promotions.csv', index=False)

# ---------------------- passengers.csv ----------------------
def generate_passengers():
    """Generate passengers data"""
    print("🔥 Generating passengers data...")
    passengers_data = []
    
    for i in range(min(NUM_ROWS, 10000)):
        passenger_id = f"PA{str(i+1).zfill(6)}"
        has_ff = random.random() > 0.3
        phone = f"+{random.randint(1,99)} {random.randint(100,999)} {random.randint(1000,9999)}"
        
        passengers_data.append({
            "passenger_id": passenger_id,
            "national_id": fake.unique.ssn(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
            "nationality": fake.country()[:50],
            "email": fake.email(),
            "phone_number": phone[:20],
            "gender": random.choice(['M', 'F']),
            "status": 'ACTIVE',
            "frequent_flyer_number": f"FF{random.randint(100000, 999999)}" if has_ff else '',
            "frequent_flyer_tier": random.choice(['Silver', 'Gold', 'Platinum']) if has_ff else '',
            "effective_date": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "expiry_date": random_date(datetime(2020,1,1), datetime(2023,1,1)),
            "is_current": True,
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        })
        data_store['passengers'].append(passenger_id)
    
    pd.DataFrame(passengers_data).to_csv(f'{OUTPUT_DIR}/passengers.csv', index=False)

# ---------------------- fare_basis_codes.csv ----------------------
def generate_fare_basis_codes():
    """Generate fare basis codes data"""
    print("🔥 Generating fare basis codes data...")
    fare_basis_data = []
    
    for i in range(50):
        fare_id = f"FB{str(i+1).zfill(4)}"
        fare_class = random.choice(['Y', 'B', 'F'])
        is_refundable = random.random() > 0.3
        
        fare_basis_data.append({
            "fare_basis_id": fare_id,
            "fare_basis_code": f"{fare_class}{random.choice(['', 'E', 'F'])}{random.randint(100, 999)}",
            "fare_class": fare_class,
            "is_refundable": is_refundable,
            "is_changeable": random.random() > 0.4,
            "description": f"{fare_class} class {'refundable' if is_refundable else 'non-refundable'} fare",
            "baggage_allowance": f"{random.randint(1, 3)} checked bags",
            "meal_included": True if fare_class in ['B', 'F'] else random.random() > 0.5,
            "upgrade_eligible": fare_class != 'F',
            "created_at": random_date(datetime(2010,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2020,1,1), datetime(2023,1,1))
        })
        data_store['fare_basis_codes'].append(fare_id)
    
    pd.DataFrame(fare_basis_data).to_csv(f'{OUTPUT_DIR}/fare_basis_codes.csv', index=False)

# ---------------------- flights.csv ----------------------
def generate_flights():
    """Generate flights data"""
    print("🔥 Generating flights data...")
    if not data_store['aircraft'] or not data_store['airports']:
        raise ValueError("Required parent data (aircraft/airports) not generated yet")
    
    flights_data = []
    airlines = ['AA', 'DL', 'UA', 'BA', 'LH']
    
    for i in range(1000):
        flight_id = f"FL{str(i+1).zfill(5)}"
        aircraft_id = random.choice(data_store['aircraft'])
        dep_airport, arr_airport = random.sample(data_store['airports'], 2)
        
        dep_date = random_date(datetime(2023,1,1), datetime(2023,12,31))
        dep_time = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:00"
        flight_duration = timedelta(hours=random.randint(1, 12))
        
        scheduled_departure = f"{dep_date} {dep_time}"
        scheduled_arrival = (datetime.strptime(scheduled_departure, "%Y-%m-%d %H:%M:%S") + flight_duration).strftime("%Y-%m-%d %H:%M:%S")
        
        if random.random() > 0.1:
            actual_departure = scheduled_departure
            actual_arrival = scheduled_arrival
            status = random.choice(['On Time', 'Delayed'])
        else:
            actual_departure = None
            actual_arrival = None
            status = 'Cancelled'
        
        flights_data.append({
            "flight_id": flight_id,
            "flight_number": f"{random.choice(airlines)}{random.randint(100, 9999)}",
            "aircraft_id": aircraft_id,
            "departure_airport": dep_airport,
            "arrival_airport": arr_airport,
            "scheduled_departure": scheduled_departure,
            "scheduled_arrival": scheduled_arrival,
            "actual_departure": actual_departure,
            "actual_arrival": actual_arrival,
            "flight_status": status,
            "created_at": random_date(datetime(2020,1,1), datetime(2023,1,1)),
            "updated_at": random_date(datetime(2022,1,1), datetime(2023,1,1))
        })
        data_store['flights'].append(flight_id)
    
    pd.DataFrame(flights_data).to_csv(f'{OUTPUT_DIR}/flights.csv', index=False)

# ---------------------- reservations.csv ----------------------
def generate_reservations():
    """Generate reservations data"""
    print("🔥 Generating reservations data...")
    if not all(k in data_store for k in ['passengers', 'flights', 'fare_basis_codes', 'sales_channels', 'promotions']):
        raise ValueError("Required parent data not generated yet")
    
    reservations_data = []
    fare_classes = {
        'Y': round(random.uniform(100, 500), 2),
        'B': round(random.uniform(500, 1500), 2),
        'F': round(random.uniform(1500, 3000), 2)
    }
    
    for i in range(min(NUM_ROWS, 20000)):
        reservation_id = f"RS{str(i+1).zfill(6)}"
        passenger_id = random.choice(data_store['passengers'])
        flight_id = random.choice(data_store['flights'])
        fare_basis_id = random.choice(data_store['fare_basis_codes'])
        
        fare_class = fare_basis_id[2] if len(fare_basis_id) > 2 and fare_basis_id[2] in fare_classes else 'Y'
        base_price = fare_classes[fare_class]
        
        has_promo = random.random() > 0.7
        promo_amount = round(random.uniform(10, 100), 2) if has_promo else 0
        taxes = round(base_price * 0.1, 2)
        fees = round(random.uniform(10, 50), 2)
        is_cancelled = random.random() > 0.9
        
        reservations_data.append({
            "reservation_id": reservation_id,
            "ticket_number": f"TK{random.randint(100000000, 999999999)}",
            "passenger_id": passenger_id,
            "channel_id": random.choice(data_store['sales_channels']),
            "promotion_id": random.choice(data_store['promotions']) if has_promo else None,
            "fare_basis_id": fare_basis_id,
            "flight_id": flight_id,
            "booking_date": random_datetime(datetime(2022,1,1), datetime(2023,1,1)),
            "departure_date": random_datetime(datetime(2023,1,1), datetime(2023,12,31)),
            "booking_class": fare_class,
            "seat_number": f"{random.randint(1, 50)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}",
            "promotion_amount": promo_amount,
            "tax_amount": taxes,
            "operational_fees": fees,
            "cancellation_fees": round(base_price * 0.2, 2) if is_cancelled else 0,
            "fare_price": base_price,
            "final_price": base_price + taxes + fees - promo_amount,
            "is_cancelled": is_cancelled,
            "cancellation_reason": random.choice(['Personal reasons', 'Schedule change', 'Found better price']) if is_cancelled else None,
            "cancellation_date": random_datetime(datetime(2023,1,1), datetime(2023,12,31)) if is_cancelled else None,
            "created_at": random_datetime(datetime(2022,1,1), datetime(2023,1,1)),
            "updated_at": random_datetime(datetime(2022,1,1), datetime(2023,1,1))
        })
        data_store['reservations'].append(reservation_id)
    
    pd.DataFrame(reservations_data).to_csv(f'{OUTPUT_DIR}/reservations.csv', index=False)

# ---------------------- Main Execution ----------------------
def main():
    """Main execution function"""
    # Generate in proper order
    generate_aircraft()
    generate_airports()
    generate_sales_channels()
    generate_promotions()
    generate_passengers()
    generate_fare_basis_codes()
    generate_flights()
    generate_reservations()
    
    print(f"✅ All CSV files generated successfully in '{OUTPUT_DIR}' directory!")

if __name__ == '__main__':
    main()