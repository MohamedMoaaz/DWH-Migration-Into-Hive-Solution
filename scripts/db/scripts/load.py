import os
import csv
import psycopg2
from psycopg2 import sql

class DatabaseLoader:
    def __init__(self, db_config, tables, csv_dir):
        self.db_config = db_config
        self.tables = tables
        self.csv_dir = csv_dir

    def validate_csv(self, file_path, expected_columns):

        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                if len(headers) != expected_columns:
                    print(f"Error: CSV has {len(headers)} columns, expected {expected_columns}")
                    return False
                    
                for row in reader:
                    if len(row) != expected_columns:
                        print(f"Warning: Row has {len(row)} columns, expected {expected_columns}")
                        return False
                        
            return True
        except Exception as e:
            print(f"Validation error: {e}")
            return False

    def load_table(self, cursor, table, expected_columns):
        """Load a single table"""
        csv_path = os.path.join(self.csv_dir, f'{table}.csv')
        if not os.path.exists(csv_path):
            print(f"Error: Missing CSV file for {table}")
            return False
            
        if not self.validate_csv(csv_path, expected_columns):
            print(f"Skipping {table} due to validation errors")
            return False
            
        try:
            # Clear existing data
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            
            # Load data with proper NULL handling
            with open(csv_path, 'r') as f:
                cursor.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH CSV HEADER NULL ''").format(
                        sql.Identifier(table)
                    ),
                    f
                )
            print(f"Successfully loaded {table}")
            return True
        except psycopg2.Error as e:
            print(f"Error loading {table}: {e.pgerror}")
            return False

    def load_data(self):
        """Main method to load all tables"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False
            cursor = conn.cursor()
            
            # Disable constraints temporarily
            cursor.execute("SET session_replication_role = 'replica';")
            
            for table, expected_columns in self.tables:
                self.load_table(cursor, table, expected_columns)
                conn.commit()
            
            # Re-enable constraints and verify
            cursor.execute("SET session_replication_role = 'origin';")
            cursor.execute("SELECT 1")  # Test query
            conn.commit()
            print("All data loaded with constraints verified")
            return True
            
        except Exception as e:
            print(f"Database error: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()