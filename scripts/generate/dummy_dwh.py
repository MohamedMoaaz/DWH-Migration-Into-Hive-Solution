import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker and seeds
fake = Faker()
Faker.seed(0)
random.seed(0)

# Output directory
OUTPUT_DIR = "data/OLAP"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 🔥 MAKE DATA BIGGER
NUM_PASSENGERS = 10000         # Increased
NUM_AIRPORTS = 200             # Increased
NUM_CHANNELS = 50              # Increased
NUM_PROMOTIONS = 500           # Increased
NUM_AIRCRAFT = 100             # Increased
NUM_RESERVATIONS = 50000       # Increased
NUM_DATES = 365                # 3 years

# ---------------------- dim_passenger ----------------------
passengers = []
for i in range(NUM_PASSENGERS):
    passengers.append({
        "passenger_key": i + 1,
        "passenger_id": fake.uuid4(),
        "passenger_national_id": fake.ssn(),
        "passenger_firstname": fake.first_name(),
        "passenger_lastname": fake.last_name(),
        "passenger_dob": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "passenger_nationality": fake.country(),
        "passenger_email": fake.email(),
        "passenger_phoneno": fake.phone_number(),
        "passenger_gender": random.choice(["Male", "Female"]),
        "passenger_status": random.choice(["active", "inactive"]),
        "frequent_flyer_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
        "effective_date": datetime.today().date(),
        "expiry_date": (datetime.today() + timedelta(days=365)).date(),
        "is_current": True
    })
pd.DataFrame(passengers).to_csv(f"{OUTPUT_DIR}/dim_passengers.csv", index=False)

# ---------------------- dim_airport ----------------------
airports = []
for i in range(NUM_AIRPORTS):
    airports.append({
        "airport_key": i + 1,
        "airport_id": f"APT{i+100}",
        "airport_name": fake.city() + " Airport",
        "airport_code": fake.lexify(text="???").upper(),
        "airport_city": fake.city(),
        "airport_country": fake.country(),
        "airport_region": fake.state(),
        "airport_timezone": f"UTC+{random.randint(0, 12)}",
        "airport_latitude": fake.latitude(),
        "airport_longitude": fake.longitude(),
        "airport_no_of_runways": random.randint(1, 6),
        "airport_size_category": random.choice(["small", "medium", "large", "hub"])
    })
pd.DataFrame(airports).to_csv(f"{OUTPUT_DIR}/dim_airports.csv", index=False)

# ---------------------- dim_channel ----------------------
channels = []
for i in range(NUM_CHANNELS):
    channels.append({
        "channel_key": i + 1,
        "channel_name": fake.company(),
        "channel_type": random.choice(["Website", "Agency", "Mobile App", "Call Center"]),
        "channel_category": random.choice(["online", "offline"]),
        "commission_rate": round(random.uniform(0, 15), 2),
        "is_active": random.choice([True, False])
    })
pd.DataFrame(channels).to_csv(f"{OUTPUT_DIR}/dim_sales_channels.csv", index=False)

# ---------------------- dim_promotions ----------------------
promotions = []
for i in range(NUM_PROMOTIONS):
    start = fake.date_between(start_date='-2y', end_date='today')
    end = start + timedelta(days=random.randint(10, 90))
    promotions.append({
        "promotion_key": i + 1,
        "promotion_id": fake.uuid4(),
        "promotion_name": fake.bs().title(),
        "promotion_type": random.choice(["Seasonal", "Flash Sale", "Loyalty", "Anniversary"]),
        "promotion_target_segment": random.choice(["Students", "Business", "Families", "All"]),
        "promotion_channel": random.choice([c["channel_name"] for c in channels]),
        "promotion_start_date": start,
        "promotion_end_date": end,
        "discount_value": round(random.uniform(5, 70), 2),
        "discount_type": random.choice(["percentage", "fixed"]),
        "max_discount_amount": round(random.uniform(100, 2000), 2),
        "effective_date": start,
        "expiry_date": end,
        "is_current": random.choice([True, False]),
        "promotion_year": start.year
    })
pd.DataFrame(promotions).to_csv(f"{OUTPUT_DIR}/dim_promotions.csv", index=False)

# ---------------------- dim_aircraft ----------------------
aircrafts = []
for i in range(NUM_AIRCRAFT):
    aircrafts.append({
        "aircraft_key": i + 1,
        "aircraft_model": fake.lexify(text="Model ???"),
        "aircraft_manufacturer": fake.company(),
        "aircraft_capacity": random.randint(100, 400),
        "economy_seats": random.randint(100, 300),
        "business_seats": random.randint(20, 70),
        "firstclass_seats": random.randint(0, 20),
        "aircraft_age": random.randint(1, 30),
        "fuel_efficiency": round(random.uniform(0.5, 6.0), 2),
        "maintenance_status": random.choice(["Good", "Needs Inspection", "Under Repair", "Grounded"])
    })
pd.DataFrame(aircrafts).to_csv(f"{OUTPUT_DIR}/dim_aircraft.csv", index=False)

# ---------------------- dim_date ----------------------
dates = []
base_date = datetime.today() - timedelta(days=NUM_DATES//2)
for i in range(NUM_DATES):
    date = base_date + timedelta(days=i)
    dates.append({
        "date_key": int(date.strftime("%Y%m%d")),
        "full_date": date.date(),
        "day_number": date.day,
        "day_name": date.strftime("%A"),
        "month_name": date.strftime("%B"),
        "year_no": date.year,
        "quarter": (date.month - 1) // 3 + 1,
        "week_of_year": date.isocalendar()[1],
        "is_weekend": date.weekday() >= 5,
        "is_holiday": random.choice([False, False, True]),  # make some random holidays
        "holiday_name": fake.word() if random.random() < 0.05 else None
    })
pd.DataFrame(dates).to_csv(f"{OUTPUT_DIR}/dim_date.csv", index=False)

# ---------------------- fare_basis ----------------------
fare_bases = []
for i in range(100):  # More variety of fare bases
    fare_bases.append({
        "fare_basis_key": i + 1,
        "fare_basis_code": fake.lexify(text="FBC??"),
        "fare_class": random.choice(["Economy", "Business", "First"]),
        "refundable": random.choice([True, False]),
        "changeable": random.choice([True, False]),
        "fare_description": fake.text(max_nb_chars=50),
        "baggage_allowance": f"{random.randint(20, 40)}kg",
        "meal_included": random.choice([True, False]),
        "upgrade_eligible": random.choice([True, False])
    })
pd.DataFrame(fare_bases).to_csv(f"{OUTPUT_DIR}/dim_fare_basis_codes.csv", index=False)

# ---------------------- fact_reservation ----------------------
reservations = []
for i in range(NUM_RESERVATIONS):
    passenger = random.choice(passengers)
    reservation_date = fake.date_between(start_date='-2y', end_date='today')
    departure_date = reservation_date + timedelta(days=random.randint(1, 60))
    fare = random.uniform(100, 2000)
    promo = random.uniform(0, 300)
    tax = fare * 0.15
    final = fare + tax - promo

    reservations.append({
        "ticket_id": fake.uuid4(),
        "channel_key": random.randint(1, NUM_CHANNELS),
        "promotion_key": random.randint(1, NUM_PROMOTIONS),
        "passenger_key": passenger["passenger_key"],
        "fare_basis_key": random.randint(1, 100),
        "aircraft_key": random.randint(1, NUM_AIRCRAFT),
        "source_airport": random.randint(1, NUM_AIRPORTS),
        "destination_airport": random.randint(1, NUM_AIRPORTS),
        "reservation_date_key": int(reservation_date.strftime("%Y%m%d")),
        "departure_date_key": int(departure_date.strftime("%Y%m%d")),
        "booking_class": random.choice(["Economy", "Business", "First"]),
        "seat_number": f"{random.randint(1, 60)}{random.choice(['A', 'B', 'C', 'D', 'E'])}",
        "promotion_amount": round(promo, 2),
        "tax_amount": round(tax, 2),
        "operational_fees": round(random.uniform(10, 50), 2),
        "cancelation_fees": 0.0,
        "fare_price": round(fare, 2),
        "final_price": round(final, 2),
        "is_cancelled": random.choice([False, False, False, False, True]),  # Mostly not canceled
        "cancellation_reason": None,
        "reservation_year": reservation_date.year,
        "reservation_month": reservation_date.month
    })
pd.DataFrame(reservations).to_csv(f"{OUTPUT_DIR}/fact_reservations.csv", index=False)

print(f"✅ BIG CSVs generated in '{OUTPUT_DIR}/' directory!")