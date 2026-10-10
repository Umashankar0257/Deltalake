

# Delta Lake Fundamentals and Hands-On Demos

## Project Overview

This repository contains hands-on demonstrations of **Delta Lake concepts using Apache Spark, PySpark, SQL, and Databricks**.

The objective is to understand how Delta Lake improves data reliability, supports incremental data processing, manages schema changes, optimizes query performance, and maintains historical versions of data.

These concepts are important for building reliable data engineering pipelines using Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), and Azure Databricks.

---

## Table of Contents

1. [Introduction to Delta Lake](#1-introduction-to-delta-lake)
2. [Delta Lake vs Parquet vs Data Lake](#2-delta-lake-vs-parquet-vs-data-lake)
3. [Demo 1: Delta Lake Fundamentals](#3-demo-1-delta-lake-fundamentals)
   - Create and read Delta tables
   - Schema evolution
   - Time travel
   - Restore previous versions
   - MERGE operations
4. [Demo 2: Optimization and Maintenance](#4-demo-2-optimization-and-maintenance)
   - OPTIMIZE
   - Z-ORDER
   - DESCRIBE DETAIL
   - VACUUM
   - Retention properties
5. [Demo 3: Shallow Clone vs Deep Clone](#5-demo-3-shallow-clone-vs-deep-clone)
6. [Demo 4: Partitioning vs Liquid Clustering](#6-demo-4-partitioning-vs-liquid-clustering)
7. [Demo 5: Change Data Feed (CDF)](#7-demo-5-change-data-feed-cdf)
8. [Real-World Azure Data Engineering Architecture](#8-real-world-azure-data-engineering-architecture)
9. [Best Practices](#9-best-practices)
10. [Interview Questions and Answers](#10-interview-questions-and-answers)
11. [Conclusion](#11-conclusion)

---

## 1. Introduction to Delta Lake

Delta Lake is an open-source storage layer that adds reliability and data management capabilities to data lakes.

It stores table data in Parquet files and maintains a transaction log in the `_delta_log` directory.

The transaction log records committed table operations and helps Delta Lake provide features such as:

- ACID transactions
- Schema enforcement
- Schema evolution
- INSERT, UPDATE, DELETE, and MERGE operations
- Time travel
- Table history and restore
- File compaction and data layout optimization
- Change Data Feed (CDF)

### Key Components

| Component | Purpose |
|---|---|
| Data Lake | Storage location for large amounts of data |
| Parquet | Column-oriented file format |
| Delta Lake | Transactional storage layer built on Parquet |
| `_delta_log` | Transaction log that records table changes |
| Spark / Databricks | Processing engine used to read and transform data |

---

## 2. Delta Lake vs Parquet vs Data Lake

These terms have different meanings.

| Feature | Data Lake | Parquet | Delta Lake |
|---|---|---|---|
| Meaning | Storage environment | File format | Transactional storage layer |
| Stores data | Yes | Yes | Yes, using Parquet files |
| ACID transactions | Depends on the storage and processing system | Not provided by the file format alone | Supported |
| UPDATE and DELETE | Depends on the processing system | Not provided by the format alone | Supported |
| MERGE / upsert | Requires processing logic | Requires processing logic | Supported |
| Schema evolution | Depends on tools used | Limited to file-level schema capabilities | Supported with configuration |
| Time travel | Not inherently provided | Not inherently provided | Supported through table versions and retained files |
| Change Data Feed | Not inherently provided | Not inherently provided | Supported when enabled |

### Why use Delta Lake instead of plain Parquet?

Consider an employee table containing 10,000 records.

If an employee's department changes, a plain Parquet-based workflow may require custom logic to identify and rewrite affected data safely.

With Delta Lake, a MERGE operation can update the matching employee record transactionally. The transaction log tracks the committed change.

Delta Lake also provides schema management, version history, and other features that reduce the amount of custom data management code required.

**Important:** Delta Lake does not replace a Data Lake. Delta tables can be stored inside a Data Lake, such as ADLS Gen2.

---

## 3. Demo 1: Delta Lake Fundamentals

### 3.1 Create a Delta Table

```sql
CREATE TABLE training.delta_demo.people_delta_demo (
    id INT,
    name STRING,
    age INT
)
USING DELTA;
```

`USING DELTA` specifies that the table uses Delta Lake.

### 3.2 Insert Sample Data Using PySpark

```python
from pyspark.sql import Row

data = [
    Row(id=1, name="Alice", age=25),
    Row(id=2, name="Bob", age=30),
    Row(id=3, name="Charlie", age=35)
]

df = spark.createDataFrame(data)

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("training.delta_demo.people_delta_demo")
```

Read the table:

```sql
SELECT * FROM training.delta_demo.people_delta_demo;
```

Writing with `format("delta")` explicitly selects the Delta format.

### 3.3 Write Delta Data to a Storage Path

```python
path = "/Volumes/training/delta_demo/my_volume/mytable"

df.write.format("delta") \
    .mode("overwrite") \
    .save(path)

read_df = spark.read.format("delta").load(path)

display(read_df)
```

This demonstrates writing and reading Delta data using a storage path rather than a registered table name.

**Note:** `overwrite` replaces existing data at the specified path. Use it carefully.

### 3.4 Schema Evolution

Schema evolution allows a Delta table to accommodate supported schema changes, such as adding a new column.

Example: add a `country` column.

```python
from pyspark.sql.functions import lit

new_df = spark.createDataFrame([
    (7, "Tom", 28),
    (8, "Jerry", 37),
    (9, "Herry", 27)
], ["id", "name", "age"])

new_df = new_df.withColumn("country", lit("USA"))

new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("training.delta_demo.people_delta_demo")
```

Inspect the result:

```sql
SELECT * FROM training.delta_demo.people_delta_demo;
```

The table now includes the `country` column.

- Older rows receive `NULL` for the new column if no value was provided.
- New rows contain `USA`.
- `mergeSchema = true` allows the incoming schema to add compatible columns to the target schema.

Schema evolution should be controlled carefully in production to avoid unexpected changes.

### 3.5 Time Travel

Time travel allows us to query a previous committed version of a Delta table.

View table history:

```sql
DESCRIBE HISTORY training.delta_demo.people_delta_demo;
```

Query a historical version:

```sql
SELECT *
FROM training.delta_demo.people_delta_demo
VERSION AS OF 2;
```

Query the latest version:

```sql
SELECT *
FROM training.delta_demo.people_delta_demo;
```

**Use case:** Investigate a previous data state, compare historical results, or troubleshoot an incorrect data update.

The requested version must exist, and the required data files must still be available.

### 3.6 Restore a Previous Version

```sql
RESTORE TABLE training.delta_demo.people_delta_demo
TO VERSION AS OF 2;
```

`RESTORE` creates a new committed version that returns the table to the state of the selected historical version.

It does not simply erase all later transaction history.

**Important:** Use the actual version shown by `DESCRIBE HISTORY`. The example version `2` may not exist in a newly created table.

### 3.7 MERGE: Insert or Update Records

MERGE is used to insert new records and update existing records based on a matching condition.

Example source data:

```python
source_df = spark.createDataFrame([
    (1, "Tom", 28),
    (7, "Herry", 27),
    (8, "Jerry", 37)
], ["id", "name", "age"])

source_df.createOrReplaceTempView("source_people")
```

Apply changes:

```sql
MERGE INTO training.delta_demo.people_delta_demo AS target
USING source_people AS source
ON target.id = source.id

WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.age = source.age

WHEN NOT MATCHED THEN
    INSERT (id, name, age)
    VALUES (source.id, source.name, source.age);
```

Verify the results:

```sql
SELECT id, name, age
FROM training.delta_demo.people_delta_demo;
```

How it works:

- If the source `id` exists in the target, update the matching row.
- If the source `id` does not exist, insert a new row.
- If the source has no matching row, the target row remains unchanged unless another MERGE clause specifies otherwise.

MERGE is commonly used for incremental loading and upsert logic.

---

## 4. Demo 2: Optimization and Maintenance

As data grows, a Delta table may contain many small files. Too many small files can increase file-management overhead and slow down queries.

Delta Lake provides maintenance commands to improve data layout and manage obsolete files.

### 4.1 OPTIMIZE

```sql
OPTIMIZE training.delta_demo.people_delta_demo;
```

OPTIMIZE compacts small data files into fewer, larger files where appropriate.

Benefits:

- Reduces small-file overhead.
- Improves read efficiency for many workloads.
- Helps maintain a more efficient table layout.

OPTIMIZE is a maintenance operation, not a replacement for good query design.

### 4.2 Z-ORDER

```sql
OPTIMIZE training.delta_demo.people_delta_demo
ZORDER BY (id);
```

Z-ORDER attempts to colocate related values in the data layout to improve data skipping for suitable filter queries.

Example:

```sql
SELECT *
FROM training.delta_demo.people_delta_demo
WHERE id = 7;
```

Z-ORDER can help when queries frequently filter on the selected columns and the table is large enough for data skipping to matter.

**Remember:** Z-ORDER is not the same as SQL `ORDER BY`. It does not guarantee that query results are returned in sorted order.

Check platform compatibility before using Z-ORDER, especially with tables configured for liquid clustering.

### 4.3 DESCRIBE DETAIL

```sql
DESCRIBE DETAIL training.delta_demo.people_delta_demo;
```

This command provides table metadata, including information such as:

- Table format
- Table location
- Number of files
- Total table size
- Partition columns
- Table properties

It is useful for inspecting the physical characteristics of a Delta table.

### 4.4 VACUUM

VACUUM removes eligible obsolete data files that are no longer needed by the current table state and are older than the configured retention threshold.

Preview candidate files:

```sql
VACUUM training.delta_demo.people_delta_demo DRY RUN;
```

Perform cleanup:

```sql
VACUUM training.delta_demo.people_delta_demo;
```

`DRY RUN` previews candidate files; it does not perform the deletion.

### 4.5 VACUUM Retention and Safety

Example of a dangerous operation:

```sql
VACUUM training.delta_demo.people_delta_demo
RETAIN 0 HOURS;
```

Delta Lake normally enforces a minimum retention safety check. Disabling that check can allow deletion of files that active jobs or historical reads still need.

The following setting disables the safety check in supported environments:

```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
```

**Do not disable this safety check or run zero-hour VACUUM in production without a carefully reviewed recovery and concurrency plan.**

Deleting obsolete data files can make historical versions unavailable even if transaction-log history remains.

### 4.6 Delta Retention Properties

The two properties serve different purposes:

| Property | Purpose |
|---|---|
| `delta.logRetentionDuration` | Controls how long transaction-log history is retained |
| `delta.deletedFileRetentionDuration` | Sets the minimum age for obsolete data files before VACUUM can make them eligible for removal |

Example table configuration:

```sql
ALTER TABLE training.delta_demo.people_delta_demo
SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
```

These values are illustrative, not universal recommendations.

Retaining transaction-log history for 30 days does not guarantee 30 days of time travel if the required data files have already been removed.

---

## 5. Demo 3: Shallow Clone vs Deep Clone

Cloning creates another Delta table based on an existing Delta table.

Example source table:

```sql
CREATE TABLE training.delta_demo.source_table (
    id INT,
    name STRING,
    age INT
)
USING DELTA;
```

### 5.1 Shallow Clone

```sql
CREATE OR REPLACE TABLE training.delta_demo.target_shallow
SHALLOW CLONE training.delta_demo.source_table;
```

A shallow clone creates a separate table definition and transaction history while referencing the source table's existing data files.

Characteristics:

- Usually fast and storage-efficient.
- Does not copy all source data files.
- Depends on the availability of referenced source files.
- Source file cleanup can affect the shallow clone's ability to read shared files.

Use case: Create a quick development or testing table when sharing underlying data files is acceptable.

### 5.2 Deep Clone

```sql
CREATE OR REPLACE TABLE training.delta_demo.target_deep
DEEP CLONE training.delta_demo.source_table;
```

A deep clone copies the source table's data files and table state into the destination.

Characteristics:

- Requires additional storage and copying work.
- The cloned data is independent of the source data files after a successful clone.
- Subsequent source changes do not automatically update the clone.

Use case: Create an independent copy for testing, analysis, or certain backup workflows.

A deep clone is not a complete backup of an entire Databricks workspace or all related resources.

### 5.3 Comparison

| Feature | Shallow Clone | Deep Clone |
|---|---|---|
| Copies source data files | No, initially references existing files | Yes |
| Initial storage overhead | Lower | Higher |
| Clone creation | Usually faster | May take longer |
| Dependency on source files | Yes, for shared files | No, after successful copying |
| Separate table state | Yes | Yes |
| Suitable for independent data copy | Limited | Better suited |

Both clone types create a separate table state. Changes to one table do not automatically change the logical contents of the other.

---

## 6. Demo 4: Partitioning vs Liquid Clustering

Data layout can improve query performance by reducing the amount of data that needs to be scanned.

This demo compares traditional partitioning with liquid clustering.

### 6.1 Create a Partitioned Delta Table

```sql
CREATE OR REPLACE TABLE training.delta_demo.sales_partitioned (
    order_id STRING,
    order_date DATE,
    country STRING,
    category STRING,
    amount DOUBLE
)
USING DELTA
PARTITIONED BY (country, category);
```

Insert sample records:

```sql
INSERT INTO training.delta_demo.sales_partitioned VALUES
('1', DATE'2024-01-01', 'USA', 'Electronics', 1200.50),
('2', DATE'2024-01-02', 'INDIA', 'Clothing', 450.00),
('3', DATE'2024-01-03', 'USA', 'Clothing', 180.75),
('4', DATE'2024-01-01', 'UK', 'Furniture', 899.00);
```

The physical layout separates data into partitions based on `country` and `category`.

Example conceptual layout:

```text
sales_partitioned/
    country=USA/
        category=Electronics/
        category=Clothing/
    country=INDIA/
        category=Clothing/
    country=UK/
        category=Furniture/
```

This is an illustrative layout; the actual physical files depend on the write and storage process.

### 6.2 Partition Pruning

Example query:

```sql
SELECT *
FROM training.delta_demo.sales_partitioned
WHERE country = 'USA';
```

Partition pruning allows the engine to skip partitions that cannot satisfy the filter.

Partitioning is most useful when the table is large and partition values produce a manageable number of reasonably sized partitions.

Avoid excessive partitioning, particularly on high-cardinality columns, because it can create many small partitions.

### 6.3 Write a Partitioned Delta Path with PySpark

```python
df = spark.read.table(
    "training.delta_demo.sales_partitioned"
)

df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("country", "category") \
    .save("/Volumes/training/delta_demo/my_volume/sales_partitioned")
```

Explicitly specifying `format("delta")` ensures that the output is written as Delta rather than relying on Spark's default data source format.

### 6.4 Create a Liquid-Clustered Table

```sql
CREATE OR REPLACE TABLE training.delta_demo.sales_clustered (
    order_id STRING,
    order_date DATE,
    country STRING,
    category STRING,
    amount DOUBLE
)
USING DELTA
CLUSTER BY (country, category, order_date);
```

Insert the sample data and optimize the table:

```sql
INSERT INTO training.delta_demo.sales_clustered VALUES
('1', DATE'2024-01-01', 'USA', 'Electronics', 1200.50),
('2', DATE'2024-01-02', 'INDIA', 'Clothing', 450.00),
('3', DATE'2024-01-03', 'USA', 'Clothing', 180.75),
('4', DATE'2024-01-01', 'UK', 'Furniture', 899.00);

OPTIMIZE training.delta_demo.sales_clustered;
```

Liquid clustering organizes data around the configured clustering columns without creating a directory for every combination of their values.

Benefits:

- Helps data skipping for suitable queries.
- Offers flexibility as data and query patterns change.
- Avoids relying on a large fixed set of partition directories.

Liquid clustering requires a compatible Databricks environment and table configuration.

### 6.5 Comparison

| Feature | Partitioning | Liquid Clustering |
|---|---|---|
| Physical layout | Separate partition directories | Organizes data without per-value partition directories |
| Data skipping | Partition pruning | Data skipping using the clustered layout |
| Best fit | Suitable large tables with manageable partition values | Flexible layouts for evolving query patterns |
| High-cardinality columns | Can create excessive partitions | Avoids creating a directory for every distinct value |
| Maintenance | Depends on file and partition layout | Uses OPTIMIZE to maintain clustering |
| Compatibility | Widely supported | Requires compatible platform and table features |

Four sample records demonstrate syntax, not a measurable performance improvement. Benchmark realistic data volumes and query patterns before choosing a layout.

---

## 7. Demo 5: Change Data Feed (CDF)

Change Data Feed allows users to read row-level changes made to a Delta table.

It can expose inserts, deletes, and update before-and-after images for supported operations.

### 7.1 Create a Table with CDF Enabled

```sql
DROP TABLE IF EXISTS training.delta_demo.people;

CREATE TABLE training.delta_demo.people (
    first_name STRING,
    age LONG
)
USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

CDF is enabled before the demo's insert, delete, and update operations.

### 7.2 Insert Records

```python
df = spark.createDataFrame([
    ("Bob", 23),
    ("Sue", 25),
    ("Jim", 27)
]).toDF("first_name", "age")

df.write.mode("append") \
    .format("delta") \
    .saveAsTable("training.delta_demo.people")
```

### 7.3 Delete and Update Records

```sql
DELETE FROM training.delta_demo.people
WHERE first_name = 'Bob';

UPDATE training.delta_demo.people
SET age = 30
WHERE first_name = 'Jim';
```

The current table now contains Sue (25) and Jim (30). Bob has been deleted.

### 7.4 Read Change Data Feed

```python
df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("training.delta_demo.people")

display(df)
```

Important options:

- `readChangeFeed = true`: requests change records.
- `startingVersion = 0`: begins reading at version 0, provided that version and the necessary change history are available.
- `table(...)`: identifies the Delta table.

CDF returns additional metadata columns:

| Column | Meaning |
|---|---|
| `_change_type` | Type of row change |
| `_commit_version` | Delta commit version |
| `_commit_timestamp` | Commit timestamp |

Common change types:

| Change type | Meaning |
|---|---|
| `insert` | Row inserted |
| `delete` | Row deleted |
| `update_preimage` | Row before an update |
| `update_postimage` | Row after an update |

For the sample operations, the changes can conceptually include:

| first_name | age | Change type |
|---|---:|---|
| Bob | 23 | insert |
| Sue | 25 | insert |
| Jim | 27 | insert |
| Bob | 23 | delete |
| Jim | 27 | update_preimage |
| Jim | 30 | update_postimage |

Exact commit versions and timestamps depend on the table's transaction history.

### 7.5 Enable CDF on an Existing Table

```sql
ALTER TABLE training.delta_demo.people
SET TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

This setting is redundant if CDF was already enabled during table creation.

Enabling CDF does not retroactively generate change records for earlier operations. The requested versions and required change history must also remain available.

### 7.6 CDF vs Watermark-Based Incremental Loading

| Feature | Change Data Feed | Watermark |
|---|---|---|
| Tracks | Row-level changes in a Delta table | Progress based on a selected column or value |
| Can expose deletes | Yes, for supported captured changes | Not reliably, unless deletion tracking is implemented separately |
| Provides update before/after images | Yes | Usually not by itself |
| Requires source column such as `updated_at` | No | Commonly used |
| Recovery/progress tracking | Track processed Delta versions | Track the last successfully processed watermark |

CDF is useful for incremental downstream processing. A production pipeline should persist its last processed version, make processing restart-safe, and account for retention limitations.

---

## 8. Real-World Azure Data Engineering Architecture

A typical Azure data engineering workflow can use these components:

```text
Source Systems
     |
     v
Azure Data Factory
     |
     v
Azure Data Lake Storage Gen2
     |
     v
Bronze Layer
(Raw / Landed Data)
     |
     v
Azure Databricks + PySpark
     |
     v
Silver Delta Tables
(Cleansing, Validation, MERGE)
     |
     v
Gold Delta Tables
(Aggregations and Business Metrics)
     |
     v
Power BI / Reporting / Downstream Systems
```

### Where the Delta Lake Features Fit

| Requirement | Delta Lake capability |
|---|---|
| Add a compatible new column | Schema evolution |
| Correct or upsert records | MERGE |
| Investigate previous data | Time travel and history |
| Return a table to an earlier state | RESTORE |
| Reduce small-file overhead | OPTIMIZE |
| Improve data skipping | Z-ORDER or liquid clustering, where appropriate |
| Remove obsolete files | VACUUM |
| Create a test copy | Shallow or deep clone |
| Process changes incrementally | Change Data Feed |

### Example Business Scenario

Imagine a payroll data pipeline.

1. ADF ingests employee and payroll files into ADLS Gen2.
2. Databricks processes raw data into Bronze Delta tables.
3. PySpark validates records and transforms them into Silver tables.
4. MERGE applies new and changed employee records.
5. Gold tables calculate department-level payroll metrics.
6. CDF can capture eligible changes from a Delta table for downstream processing.
7. OPTIMIZE and an appropriate data layout help maintain performance as data grows.
8. Time travel and table history support investigation of data issues, subject to retention.

---

## 9. Best Practices

- Use `format("delta")` explicitly when writing Delta data to paths.
- Use schema evolution intentionally; validate incoming schemas before appending data.
- Use MERGE for appropriate upsert scenarios and ensure source keys are handled correctly.
- Check `DESCRIBE HISTORY` before choosing a historical version for time travel or RESTORE.
- Remember that time travel requires both historical metadata and the necessary data files.
- Use OPTIMIZE based on table size, write patterns, and query performance.
- Choose Z-ORDER or liquid clustering based on supported features and real query patterns.
- Avoid excessive partitioning and high-cardinality partition columns.
- Run VACUUM with a retention period appropriate for concurrent jobs, recovery needs, and historical queries.
- Never disable retention safety checks casually in production.
- Choose shallow cloning when sharing underlying data files is acceptable; choose deep cloning when an independent copy is required.
- Enable CDF before the changes you need to capture.
- Track the last processed CDF version and make downstream processing restart-safe.
- Validate performance and recovery requirements in a test environment before applying maintenance operations to production tables.

---

## 10. Interview Questions and Answers

### Q1. What is Delta Lake?

Delta Lake is a storage layer built on Parquet files and a transaction log. It provides ACID transactions, schema management, MERGE, time travel, and other data reliability features.

### Q2. What is the difference between Parquet and Delta Lake?

Parquet is a column-oriented file format. Delta Lake uses Parquet data files together with a transaction log to provide transactional table management and additional capabilities.

### Q3. What is schema evolution?

Schema evolution allows a table's schema to change in supported ways, such as adding a column. In PySpark, `mergeSchema` can be used for compatible schema changes during a write.

### Q4. What is time travel?

Time travel lets us query a previous committed version of a Delta table using a version number or timestamp, provided the required history and data files are available.

### Q5. What is RESTORE?

RESTORE creates a new table version that returns the table to a selected previous state.

### Q6. What is MERGE?

MERGE compares source and target records using a condition. It can update matching rows, insert nonmatching rows, and handle other cases using additional clauses.

### Q7. What is OPTIMIZE?

OPTIMIZE reorganizes Delta data files, including compacting small files, to improve read efficiency for suitable workloads.

### Q8. What is Z-ORDER?

Z-ORDER colocates related values in the data layout to help data skipping. It is not equivalent to sorting query results.

### Q9. What is VACUUM?

VACUUM removes eligible obsolete data files after the retention threshold. It can affect time travel if historical data files are deleted.

### Q10. What is the difference between shallow and deep clone?

Shallow clone initially references existing source data files, making it faster and more storage-efficient but dependent on shared files. Deep clone copies data files, requiring more storage and work but providing an independent data copy.

### Q11. What is the difference between partitioning and liquid clustering?

Partitioning separates data into physical partition directories. Liquid clustering organizes data around selected columns without creating a separate directory for every partition value.

### Q12. What is Change Data Feed?

CDF exposes row-level changes from a Delta table, including inserts, deletes, and update before-and-after images for supported operations.

### Q13. How can Delta Lake support incremental data pipelines?

Delta Lake can use MERGE to apply incremental upserts and CDF to expose changes for downstream processing. The pipeline must track its progress and handle retries and retention appropriately.

### Q14. What is the purpose of `_delta_log`?

The transaction log records committed table operations and supports consistent reads, table versioning, and Delta Lake transaction management.

### Q15. Does enabling CDF capture all previous table changes?

No. CDF must be enabled before the changes you want to capture, and the required versions and change history must remain available.

---

## 11. Conclusion

This project demonstrates essential Delta Lake capabilities used in modern data engineering.

The five hands-on demos cover:

1. **Fundamentals:** Delta tables, schema evolution, time travel, RESTORE, and MERGE.
2. **Optimization:** OPTIMIZE, Z-ORDER, DESCRIBE DETAIL, VACUUM, and retention properties.
3. **Cloning:** Shallow clone and deep clone.
4. **Data layout:** Partitioning and liquid clustering.
5. **Change tracking:** Change Data Feed and incremental processing.

Together, these concepts provide a foundation for building reliable, maintainable, and scalable data pipelines using Apache Spark, PySpark, SQL, Delta Lake, and Azure Databricks.

---

## Technologies Covered

- Apache Spark
- PySpark
- SQL
- Delta Lake
- Databricks
- Delta transaction log
- Schema evolution
- MERGE / upsert
- Time travel and RESTORE
- OPTIMIZE and Z-ORDER
- VACUUM and retention management
- Shallow and deep cloning
- Partitioning and liquid clustering
- Change Data Feed (CDF)
- Azure Data Engineering concepts: ADF, ADLS Gen2, Bronze, Silver, and Gold layers
