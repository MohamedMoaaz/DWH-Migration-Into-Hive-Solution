import os
import psycopg2
import pandas as pd
import subprocess
from datetime import datetime
from contextlib import contextmanager

class PostgresToHdfsExporter:
    def __init__(self, pg_config, hdfs_path, tables, hdfs_container, container_temp_dir):
        self.pg_config = pg_config
        self.hdfs_path = hdfs_path.rstrip('/')
        self.tables = tables
        self.hdfs_container = hdfs_container
        self.container_temp_dir = container_temp_dir
        self.last_extract_dir = "/Users/mohamedmoaaz/Desktop/hive/last_extracts"
        os.makedirs(self.last_extract_dir, exist_ok=True)

    @contextmanager
    def _db_connection(self):
        conn = psycopg2.connect(**self.pg_config)
        try:
            yield conn
        finally:
            conn.close()

    def _run_docker_cmd(self, cmd):
        try:
            subprocess.run(
                f"docker exec {self.hdfs_container} {cmd}",
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {cmd}\nError: {e.stderr}")

    def _get_last_extract(self, table):
        time_file = os.path.join(self.last_extract_dir, f"{table}_last_extract.txt")
        if os.path.exists(time_file):
            with open(time_file, 'r') as f:
                return f.read().strip()
        return '1970-01-01 00:00:00'

    def _update_last_extract(self, table):
        with open(os.path.join(self.last_extract_dir, f"{table}_last_extract.txt"), 'w') as f:
            f.write(datetime.utcnow().isoformat())

    def _get_incremental_query(self, table, conn):
        last_time = self._get_last_extract(table)
        
        print(f"\nDEBUG for {table}:")
        print(f"Last extraction time: {last_time}")
        
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(updated_at) FROM {table}")
            max_db_time = cur.fetchone()[0]
            print(f"Max updated_at in DB: {max_db_time}")
        
        return f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at"

    def _process_table(self, table, conn, incremental):
        local_csv = f"{table}.csv"
        container_csv = f"{self.container_temp_dir}/{table}.csv"
        
        try:
            # Build and execute query
            if incremental:
                query = self._get_incremental_query(table, conn)
                last_time = self._get_last_extract(table)
                print(f"Using incremental query for {table}")
                
                # Use cursor directly to avoid pandas warning
                with conn.cursor() as cur:
                    cur.execute(query, (last_time,))
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
            else:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table}")
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=colnames)
            
            if incremental:
                if df.empty:
                    print(f"No new records found in {table} (last extract: {self._get_last_extract(table)})")
                    return f"No new records in {table}"
                print(f"Found {len(df)} new records in {table}")

            # Export to CSV - ensure no header rows in data
            df = df[~df.apply(lambda row: row.astype(str).str.strip().isin(df.columns).all(), axis=1)]
            df.to_csv(local_csv, index=False)
            
            # Transfer to HDFS
            hdfs_file = f"{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            hdfs_path = f"{self.hdfs_path}/{table}/{hdfs_file}"
            
            subprocess.run(f"docker cp {local_csv} {self.hdfs_container}:{container_csv}", shell=True, check=True)
            self._run_docker_cmd(f"hdfs dfs -mkdir -p {self.hdfs_path}/{table}")
            self._run_docker_cmd(f"hdfs dfs -put -f {container_csv} {hdfs_path}")
            
            if incremental and not df.empty:
                self._update_last_extract(table)
                
            return f"Exported {len(df)} records to {hdfs_path}"
            
        finally:
            if os.path.exists(local_csv):
                os.remove(local_csv)
            self._run_docker_cmd(f"rm -f {container_csv}")

    def export_tables(self, incremental=True):
        results = {}
        self._run_docker_cmd(f"mkdir -p {self.container_temp_dir}")
        
        with self._db_connection() as conn:
            for table in self.tables:
                print(f"\nProcessing {table}...")
                try:
                    results[table] = self._process_table(table, conn, incremental)
                    print(f"✓ {results[table]}")
                except Exception as e:
                    results[table] = f"Failed: {str(e)}"
                    print(f"✗ {results[table]}")
        
        return results