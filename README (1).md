# Gold Layer – Medallion Architecture

## Overview

This repository contains the implementation of the **Gold Layer** in the Medallion Architecture.

The Gold layer contains:
- Business-ready aggregated tables
- Reporting datasets
- Analytics-ready curated outputs

All Gold transformations are:
- SQL-based
- Metadata-driven
- Executed via a parallel driver
- Controlled through a registry table
- Stored and managed in GitHub
- Automatically converted to Databricks notebooks

---

# 1. Control Layer Schema (Gold Registry)

Create the Gold control registry table in the `control` schema:

```sql
CREATE TABLE control.gold_table_registry (
    gold_table STRING,
    driving_silver_confirmed_table STRING,
    notebook_path STRING,
    dependency_level INT,
    is_active BOOLEAN,
    created_on TIMESTAMP,
    updated_on TIMESTAMP,
    batch_time TIMESTAMP
) USING ICEBERG;
```

This table controls execution of all Gold layer transformations.

---

# 2. Repository Structure

The repository structure must follow this format:

```
gold-layer/
│
├── transformations/
│   ├── <gold_table_name>.sql
│   ├── <gold_table_name>.sql
│   └── ...
│
├── parallel_driver.py
└── README.md
```

---

# 3. Transformation File Standards

- Each Gold table must have its own SQL file.
- The file name must exactly match the Gold table name.
- File format must be:

```
<gold_table_name>.sql
```

Example:

```
fact_incident_summary.sql
dim_agency_performance.sql
```

---

# 4. SQL Rules (Mandatory)

All SQL files must follow these standards:

1. All SQL statements must end with a semicolon `;`
2. Do not leave any statement without a closing `;`
3. Keep complete transformation logic inside the `.sql` file
4. Do not split logic across multiple files
5. No inline execution logic inside driver

Example:

```sql
CREATE OR REPLACE TABLE gold.fact_incident_summary
USING ICEBERG
AS
SELECT ...
FROM silver_confirmed.dim_incident
JOIN silver_confirmed.fact_response;
```

---

# 5. Adding Gold Tables to Registry

Every Gold table must be inserted into:

```
control.gold_table_registry
```

Example:

```sql
INSERT INTO control.gold_table_registry
VALUES (
    'fact_incident_summary',
    'fact_response',
    '/Repos/<repo_path>/parallel_driver',
    1,
    true,
    current_timestamp(),
    current_timestamp(),
    current_timestamp()
);
```

---

# 6. Execution Model

Execution is metadata-driven.

The `parallel_driver.py` notebook:

- Reads `gold_table_registry`
- Filters `is_active = true`
- Executes based on `dependency_level`
- Runs SQL files in parallel

---

# 7. Running for Testing

For testing:

1. Open `parallel_driver.py` in Databricks
2. Run it as a notebook
3. It will invoke single execution mode

Currently:

```
max_workers = 3
```

This is intentionally limited for testing.

During production load testing, this will be increased.

---

# 8. Dependency Handling

- `dependency_level` controls execution order
- Lower number runs first
- Same level runs in parallel
- Higher level waits for lower level completion

Example:

| Table | dependency_level |
|-------|------------------|
| dim_* | 1 |
| fact_* | 2 |
| agg_* | 3 |

---

# 9. Important Notes

- All Gold logic must be deterministic
- No manual execution of SQL outside driver
- No hardcoding table names inside driver
- Registry table controls everything
- Always update `updated_on` when modifying entries
- Ensure Git sync before execution

---

# 10. Production Scaling

Current configuration:

```
max_workers = 3
```

For production:
- Increase workers based on cluster capacity
- Perform load testing before scaling
- Monitor cluster utilization
- Validate Iceberg write performance

---

# 11. Summary

Gold Layer is:

- Fully metadata-driven
- Git-controlled
- Parallel-executable
- Scalable
- Deterministic
- Production-ready

All transformations must strictly follow this README standard.