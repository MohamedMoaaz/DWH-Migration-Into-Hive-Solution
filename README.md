# Apache Hive Project Documentation

## Overview

This project demonstrates the setup, configuration, and operation of Apache Hive running atop a Hadoop cluster in a containerized environment. The system is optimized for distributed processing, automated data extraction, incremental loading, and periodic data transformations.

## Project Components

### Infrastructure Setup

* **Apache Hive on Hadoop Cluster:** Established a fully operational Apache Hive environment leveraging Hadoop for data storage and distributed computation.
* **Multi-stage Dockerfile:** Created Dockerfiles for streamlined deployment of both Hadoop and Hive services.
* **Service Orchestration:** Utilized `docker-compose.yml` for orchestrating Namenodes, Datanodes, Hive Server2, Hive Metastore, and PostgreSQL.

### Configuration

* **Execution Engine:** Set Apache Hive's execution engine to `Tez` for optimized performance.
* **Hive Metastore:** Configured remote metastore using PostgreSQL for better scalability and metadata management.
* **Environment and Networking:** Defined proper environment variables, volume mappings, Docker networks, port mappings, and dependencies.
* **Hive and Tez Configuration:** Managed configurations in `hive-site.xml` and `tez-site.xml` to optimize Hive execution and resource allocation.

### Data Handling

* **Data Migration:** Migrated historical DWH data to HDFS as CSV files and loaded transactional PostgreSQL data periodically.
* **Incremental Loading:** Implemented a Python-based ELT script to incrementally load transactional data into Hive.
* **Staging Area:** Loaded data initially to a staging schema on HDFS, then transformed it into Hive optimized schemas.

### ETL and ELT Automation

* **Python ELT Script:** Developed and deployed Python scripts to automate data extraction from PostgreSQL and incremental loading into Hive.
* **Scheduled Automation:** Utilized `crontab` to schedule:

  * SQL transformation scripts on Hive server.
  * Python incremental load scripts locally.

### Schema and Table Optimization

* **Hive Schema:** Designed a scalable schema optimized for distributed querying and analytics.

* **File Format (ORC):** Chose ORC (Optimized Row Columnar) files because they are highly compatible with Hive. ORC format provides efficient storage, compression, and improved query performance due to built-in indexing and statistics.

* **ACID Compliance:** Implemented Slowly Changing Dimensions (SCD) as ACID-compliant tables in Hive, supporting transactional operations such as row-level updates, deletes, and inserts, essential for managing slowly changing dimension data.

* **Non-ACID Tables:** Managed larger, denormalized Hive tables optimized for query performance and minimal complexity. Non-ACID tables are suitable for data that doesn't require frequent updates or transactional consistency.

* **Partitioning Strategy:**

  * **Big Tables:** Partitioned by year and month to enhance query performance by enabling efficient pruning, minimizing data scanning, and optimizing resource utilization.
  * **Passenger Dimension (ACID Table):** Partitioned by passenger frequent flyer tier, ensuring fast lookups and updates based on customer loyalty categories.
  * **Promotions (ACID Table):** Partitioned by promotion type, allowing efficient management and queries related to promotional analytics.

* **Bucketing Strategy:** Chose bucketing on passenger ID for both the big table and passenger dimension to evenly distribute data across buckets, facilitating efficient joins, aggregations, and data sampling.

* **Compression (Snappy):** Selected Snappy compression for compaction because it provides a balance between compression ratio and processing speed, reducing storage costs and enhancing query performance without significant CPU overhead.

### Data Validation and Consistency

* Performed rigorous data validation checks to ensure consistency and correctness post-transformation.

## Scheduled Task Setup

### Hive Server (Transformations)

```bash
sudo service cron start

echo export PATH=/usr/local/hive/bin:/usr/local/hadoop/bin:/usr/bin:/bin >> /app/hive_cron.sh
echo "beeline -u jdbc:hive2://localhost:10000 -n hive -p hive -f /app/test.sql >> /app/hive_cron.log 2>&1" >> /app/hive_cron.sh

echo "* 2 * * * /app/hive_cron.sh" | crontab -
```

### Local Host (Python ELT)

```bash
* 2 * * * export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin && cd /Users/mohamedmoaaz/Desktop/hive/hadoop/scripts && /usr/local/bin/python3 to_source_staging.py >> /Users/mohamedmoaaz/Desktop/output.log 2>&1
```

## Conclusion

This comprehensive project setup provides an efficient, reliable, and scalable solution for data warehousing using Apache Hive, facilitating both batch and incremental data processing workflows.
