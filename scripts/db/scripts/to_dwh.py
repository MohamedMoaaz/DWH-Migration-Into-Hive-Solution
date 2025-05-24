from load import DatabaseLoader

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'dwh',
    'user': 'user',
    'password': 'password'
}

# Tables in correct load order with expected column counts
TABLES = [
    ('dim_aircraft', 10),
    ('dim_airports', 12),
    ('dim_sales_channels', 6),
    ('dim_promotions', 15),
    ('dim_passengers', 15),
    ('dim_fare_basis_codes', 9),
    ('dim_date', 11),
    ('fact_reservations', 22),
]
EXTRACTION_DIR = 'data/OLAP'
if __name__ == '__main__':
    loader = DatabaseLoader(DB_CONFIG, TABLES, EXTRACTION_DIR)
    loader.load_data()