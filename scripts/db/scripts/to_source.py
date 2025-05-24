from load import DatabaseLoader

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'source',
    'user': 'user',
    'password': 'password'
}

# Tables in correct load order with expected column counts
TABLES = [
    ('aircraft', 12),
    ('airports', 12),
    ('sales_channels', 8),
    ('promotions', 15),
    ('passengers', 17),
    ('fare_basis_codes', 11),
    ('flights', 12),
    ('reservations', 22),
]
EXTRACTION_DIR = 'data/OLTP'
if __name__ == '__main__':
    loader = DatabaseLoader(DB_CONFIG, TABLES, EXTRACTION_DIR)
    loader.load_data()