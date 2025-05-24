from EL_dwh import PostgresToHdfsExporter

# Configuration
PG_CONFIG = {
    'host' : 'localhost',
    'port' : 5432,
    'dbname' : 'dwh',
    'user' : 'user',
    'password' : 'password'
}
TABLES = [
    'dim_aircraft',
    'dim_airports',
    'dim_sales_channels',
    'dim_promotions',
    'dim_passengers',
    'dim_fare_basis_codes',
    'dim_date',
    'fact_reservations'
]
HDFS_PATH = '/user/hive/warehouse/dwh_staging.db'
HDFS_CONTAINER = 'master1'
CONTAINER_TEMP_DIR = '/tmp/csv_staging'

# Create exporter instance
exporter = PostgresToHdfsExporter(
    pg_config=PG_CONFIG,
    hdfs_path=HDFS_PATH,
    tables=TABLES,
    hdfs_container= HDFS_CONTAINER,
    container_temp_dir = CONTAINER_TEMP_DIR
)

# Export tables
results = exporter.export_tables()

# Print results
for table, status in results.items():
    print(f"{table}: {status}")