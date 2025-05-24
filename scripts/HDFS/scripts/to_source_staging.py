from EL_to_hdfs import PostgresToHdfsExporter

# Configuration
PG_CONFIG = {
    'host' : 'localhost',
    'port' : 5432,
    'dbname' : 'source',
    'user' : 'user',
    'password' : 'password'
}
TABLES = [
    'aircraft',
    'airports',
    'fare_basis_codes',
    'flights',
    'passengers',
    'promotions',
    'reservations',
    'sales_channels'
]
HDFS_PATH = '/user/hive/warehouse/source_staging.db'
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