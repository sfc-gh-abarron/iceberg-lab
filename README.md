original author: Gilberto Hernandez, Scott Teal
Modified by: Alex Barron

Initial guide is available at: https://www.snowflake.com/en/developers/guides/get-started-snowflake-managed-iceberg-tables/

# Get Started with Iceberg Tables on Snowflake-Managed Storage
<!-- ------------------------ -->
## Overview

Apache Iceberg tables in Snowflake give you the benefits of an open table format — interoperability with engines like Spark, DuckDB, and Flink — while Snowflake handles storage, optimization, and governance for you. With Snowflake-managed Iceberg storage, you don't need to configure a cloud bucket or external volume. Snowflake stores and manages all Iceberg data and metadata files internally, so you can focus on building pipelines and analytics rather than infrastructure.

> **Note**: Snowflake-managed Iceberg tables can be stored in your own cloud storage bucket or in Snowflake-managed storage. For simplicity, this guide uses Snowflake-managed storage so you can get started without any external cloud configuration.

In this lab, you'll build a fleet analytics pipeline using Snowflake-managed Iceberg tables for a fictitious fleet management company that collects vehicle telemetry, sensor readings, and maintenance logs.

The lab has two parts:

1. **In Snowflake** — create a Git Workspace from this repository and run SQL scripts **01–05** to build the database, Iceberg tables, Dynamic Iceberg Tables, load data, and migrate a standard table to Iceberg. Then put a Cortex Agent on top of it.
2. **On your laptop** — clone the repository and run the interoperability pieces locally: a Python script that streams telemetry into Snowflake, then DuckDB and Apache Spark reading the same Iceberg tables through Horizon Catalog — without moving a single byte of data.

### What You'll Learn

- How to create Snowflake-managed Iceberg tables with `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`
- How to batch-load JSON files into Iceberg tables with `COPY INTO`
- How to migrate a standard Snowflake table to Iceberg with a single CTAS
- How to build declarative transformation pipelines with Dynamic Iceberg Tables
- How to stream semi-structured data into Iceberg tables from a local Python script
- How to query Iceberg data with natural language using Snowflake Intelligence
- How to read Snowflake-managed Iceberg tables from DuckDB and Apache Spark via Horizon Catalog

### What You'll Need

- A Snowflake account ([trial](https://signup.snowflake.com/developers), or otherwise) on **AWS** or **Azure** (Snowflake-managed Iceberg storage is not available on Google Cloud as of 26/10/26)
- **ACCOUNTADMIN** access
- For the local interoperability section:
  - `git`
  - [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation) with a [configured connection](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/specify-credentials)
  - Python 3.9+
  - (Optional, for Spark) Conda and **Java 17 or 21**

### What You'll Build

- Snowflake-managed Iceberg tables for streaming telemetry, sensor readings, vehicle data, and maintenance logs
- A Dynamic Iceberg Table pipeline for automated incremental transformations
- A Cortex Agent for natural language querying of fleet data via Snowflake Intelligence
- DuckDB and Spark clients demonstrating cross-engine interoperability on the same Iceberg tables

<!-- ------------------------ -->
## Part 1: Build the Pipeline in Snowflake

### Create a Git Workspace

- Log into your Snowflake account
- Navigate to **Projects → Workspaces**
- At the top, next to Workspaces / Databases, select the **+** icon and create a new **Git Workspace**
- Add `https://github.com/sfc-gh-abarron/iceberg-lab` as the link to the Git repository
- Click **Create API Integration** and name it `GITHUB`
  - Add `https://github.com/sfc-gh-abarron/` for allowed prefixes
  - Allow all authentication secrets
  - Click **Create**
- Select **Public Repository** on the Create Workspace page
- Click **Create**

You now have all the lab materials in a workspace. The SQL scripts are in the **assets** folder. Run them in order.

### 01_create_database.sql

Creates the `FLEET_WH` adaptive warehouse and the `FLEET_DB` database with `RAW` and `ANALYTICS` schemas. It sets the database's default external volume to `SNOWFLAKE_MANAGED`, so Iceberg tables use Snowflake-managed storage, and sets the default Iceberg version to v3, which VARIANT columns require. Finally, it creates the `LOGS_STAGE` internal stage for the JSON maintenance logs.

Run the whole script.

### Upload the Maintenance Log Files

Script 04 loads JSON files from `LOGS_STAGE`, so upload them now:

1. Download `maintenance_log_001.json`, `maintenance_log_002.json` and `maintenance_log_003.json` from the [assets folder](https://github.com/sfc-gh-abarron/iceberg-lab/tree/main/assets) (or use them from your local clone — see Part 2)
2. In Snowsight, go to **Catalog → Database Explorer → FLEET_DB → RAW → Stages → LOGS_STAGE**
3. Click **+ Files**, add the three JSON files, and click **Upload**

### 02_create_iceberg_tables.sql

Creates the raw-layer tables in `RAW`. Three are Snowflake-managed Iceberg tables:

- `VEHICLE_TELEMETRY_STREAM` — streaming telemetry, stored as VARIANT (filled later by the local streaming script)
- `VEHICLE_REGISTRY` — vehicle and driver master data
- `MAINTENANCE_LOGS` — JSON diagnostic logs, stored as VARIANT

It also creates `SENSOR_READINGS_TEMP`, a **standard** Snowflake table that script 05 migrates to Iceberg.

Two parameters make these tables Iceberg:

- **`CATALOG = 'SNOWFLAKE'`** — Snowflake manages the Iceberg metadata (catalog entries, snapshots, manifest files)
- **`EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`** — Snowflake stores the Iceberg data files internally, just like standard Snowflake tables

```sql
CREATE OR REPLACE ICEBERG TABLE VEHICLE_TELEMETRY_STREAM (
    VEHICLE_ID STRING NOT NULL,
    EVENT_TIMESTAMP TIMESTAMP_NTZ(6) NOT NULL,
    TELEMETRY_DATA VARIANT NOT NULL,
    INGESTED_AT TIMESTAMP_LTZ(6)
)
    EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
    CATALOG = 'SNOWFLAKE';
```

The `VARIANT` column lets us land semi-structured JSON without defining a rigid schema upfront, while still supporting efficient querying with path notation (e.g., `TELEMETRY_DATA:speed_mph`).

### 03_create_dynamic_tables.sql

Builds a two-tier declarative pipeline of Dynamic Iceberg Tables in `ANALYTICS`:

- **`TELEMETRY_ENRICHED`** (`TARGET_LAG = '10 minutes'`) — extracts speed, engine and diagnostic fields from the VARIANT telemetry, classifies engine health as NORMAL / WARNING / CRITICAL, and joins vehicle and driver details from the registry
- **`DAILY_FLEET_SUMMARY`** (`TARGET_LAG = 'DOWNSTREAM'`) — aggregates the enriched data into daily metrics per fleet region

You write a `SELECT` that describes the output; Snowflake handles incremental refresh, dependency management and scheduling. These tables stay empty until telemetry is streamed in Part 2.

### 04_load_sample_data.sql

Generates synthetic data — 100 vehicles in `VEHICLE_REGISTRY` and 10,000 sensor readings in `SENSOR_READINGS_TEMP` — using `INSERT ... SELECT` with `GENERATOR`. It then loads the JSON files you uploaded into `MAINTENANCE_LOGS` with `COPY INTO`:

```sql
COPY INTO MAINTENANCE_LOGS (LOG_ID, VEHICLE_ID, LOG_TIMESTAMP, LOG_DATA, SOURCE_FILE)
FROM (
    SELECT
        $1:log_id::VARCHAR,
        $1:vehicle_id::VARCHAR,
        $1:log_timestamp::TIMESTAMP_NTZ,
        $1,
        METADATA$FILENAME
    FROM @LOGS_STAGE
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
PATTERN = '.*maintenance_log.*\.json';
```

- Typed fields are extracted from the JSON (`$1` is the whole JSON object)
- The full payload is stored in the `LOG_DATA` VARIANT column
- The source file name is captured in `SOURCE_FILE` for lineage

The script finishes with row counts per table. If `MAINTENANCE_LOGS` shows 0, check that the files were uploaded to `LOGS_STAGE` (`LIST @FLEET_DB.RAW.LOGS_STAGE;`) and re-run the `COPY INTO`.

Query the VARIANT data:

```sql
SELECT
    LOG_ID,
    VEHICLE_ID,
    LOG_DATA:event_type::STRING AS event_type,
    LOG_DATA:severity::STRING AS severity,
    LOG_DATA:description::STRING AS description,
    LOG_DATA:total_cost::FLOAT AS total_cost
FROM FLEET_DB.RAW.MAINTENANCE_LOGS
ORDER BY LOG_TIMESTAMP DESC
LIMIT 10;
```

### 05_migrate_table.sql

Migrates the standard `SENSOR_READINGS_TEMP` table to a Snowflake-managed Iceberg table with a single CTAS:

```sql
CREATE OR REPLACE ICEBERG TABLE SENSOR_READINGS
    EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
    CATALOG = 'SNOWFLAKE'
AS
SELECT * FROM SENSOR_READINGS_TEMP;
```

It then validates the migration by comparing row counts and running `MINUS` in both directions (both `only_in_` columns should be 0), drops the original standard table, and finishes by dropping and undropping the Iceberg table to show that Time Travel works on Iceberg tables too.

### Verify

```sql
SHOW ICEBERG TABLES IN SCHEMA FLEET_DB.RAW;
SHOW DYNAMIC TABLES IN SCHEMA FLEET_DB.ANALYTICS;
```

You should see four Iceberg tables in **RAW** (`VEHICLE_TELEMETRY_STREAM`, `VEHICLE_REGISTRY`, `MAINTENANCE_LOGS`, `SENSOR_READINGS`) and two Dynamic Tables in **ANALYTICS**.

> `06_cleanup.sql` is for the end of the lab — don't run it yet.

<!-- ------------------------ -->
## Part 2: Interoperability from Your Laptop

Everything in this part runs locally. The streaming script writes into Snowflake; DuckDB and Spark read the Iceberg tables directly through Horizon Catalog's Iceberg REST endpoint.

### Clone the Repository

```bash
git clone https://github.com/sfc-gh-abarron/iceberg-lab.git
cd iceberg-lab/assets
```

All remaining commands are run from the `assets` directory.

### Create a Programmatic Access Token (PAT)

DuckDB and Spark authenticate to Horizon Catalog with a PAT. In Snowsight, go to **User Menu → Settings → Authentication → Programmatic access tokens**, generate a token with role **ACCOUNTADMIN**, and copy it — it's only shown once.

### Create Your config.env

`config.env` is listed in `.gitignore`, so it isn't in the repository — create your own from the template:

```bash
cp config.env.template config.env
```

Then edit **config.env**:

```bash
# Snowflake CLI connection name used by the streaming script (run `snow connection list`)
SNOWFLAKE_CONNECTION="default"

# Account identifier (orgname-accountname), used to build the Horizon Catalog URL
# Find yours: SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
SNOWFLAKE_ACCOUNT="your_account_identifier"
SNOWFLAKE_USER="your_username"

# The PAT you just created
SNOWFLAKE_PAT=""
```

- The **streaming script** uses `SNOWFLAKE_CONNECTION` to connect through your existing Snowflake CLI configuration in `~/.snowflake/config.toml`. If you don't have one yet:
  ```bash
  snow connection add --connection-name default --account <your-account> --user <your-user>
  ```
- **DuckDB and Spark** use `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PAT` to reach Horizon Catalog at `https://<account>.snowflakecomputing.com/polaris/api/catalog`. Underscores in the account name are converted to hyphens automatically, since underscores aren't valid in TLS hostnames.

### Run the Local Setup Script

```bash
chmod +x setup.sh
./setup.sh
```

The setup script only prepares your local machine:

- Tests your Snowflake CLI connection
- Creates the `fleet_venv` Python virtual environment for the streaming and DuckDB scripts
- Creates the `fleet-spark` Conda environment with PySpark 4.0 and Jupyter (if Conda is installed)

### Stream Telemetry into Iceberg

```bash
source fleet_venv/bin/activate

# Runs for 5 minutes by default (STREAMING_DURATION in config.env)
python stream_telemetry.py
```

The script simulates a fleet of 50 vehicles and inserts JSON telemetry events into the `TELEMETRY_DATA` VARIANT column of **VEHICLE_TELEMETRY_STREAM**. Each event looks like this:

```json
{
    "location": {"lat": 37.7749, "lon": -122.4194},
    "speed_mph": 65.3,
    "heading": 285,
    "engine": {
        "rpm": 3200,
        "temperature_f": 195,
        "oil_pressure_psi": 42,
        "fuel_level_pct": 67.5
    },
    "diagnostics": {
        "check_engine": false,
        "tire_pressure_warning": false,
        "codes": []
    },
    "driver_behavior": {
        "hard_acceleration_count": 2,
        "hard_brake_count": 1,
        "sharp_turn_count": 0
    }
}
```

While it runs, watch the data arrive in a Snowsight worksheet:

```sql
SELECT COUNT(*) AS event_count FROM FLEET_DB.RAW.VEHICLE_TELEMETRY_STREAM;

SELECT
    VEHICLE_ID,
    EVENT_TIMESTAMP,
    TELEMETRY_DATA:location:lat::FLOAT AS latitude,
    TELEMETRY_DATA:location:lon::FLOAT AS longitude,
    TELEMETRY_DATA:speed_mph::FLOAT AS speed_mph,
    TELEMETRY_DATA:engine:temperature_f::INT AS engine_temp,
    TELEMETRY_DATA:engine:fuel_level_pct::FLOAT AS fuel_level
FROM FLEET_DB.RAW.VEHICLE_TELEMETRY_STREAM
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 10;
```

> **Tip**: Stop the script at any time with **Ctrl+C**. Data already inserted stays in the table.

### Refresh the Dynamic Iceberg Tables

Now that telemetry exists, refresh the pipeline instead of waiting for the target lag:

```sql
ALTER DYNAMIC TABLE FLEET_DB.ANALYTICS.TELEMETRY_ENRICHED REFRESH;
ALTER DYNAMIC TABLE FLEET_DB.ANALYTICS.DAILY_FLEET_SUMMARY REFRESH;

SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(FLEET_DB.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name IN ('TELEMETRY_ENRICHED', 'DAILY_FLEET_SUMMARY')
ORDER BY refresh_start_time DESC
LIMIT 10;
```

After the initial refresh, run the streaming script again and refresh once more — `refresh_action` should show **INCREMENTAL**, meaning only new rows were processed.

```sql
SELECT VEHICLE_ID, MAKE, MODEL, DRIVER_NAME, FLEET_REGION,
       speed_mph, engine_temp_f, engine_health_status, EVENT_TIMESTAMP
FROM FLEET_DB.ANALYTICS.TELEMETRY_ENRICHED
WHERE engine_health_status != 'NORMAL'
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 10;

SELECT * FROM FLEET_DB.ANALYTICS.DAILY_FLEET_SUMMARY
ORDER BY summary_date DESC, FLEET_REGION
LIMIT 20;
```

### Query from DuckDB

DuckDB supports the Iceberg REST Catalog protocol, so it can attach to Horizon Catalog and read the Snowflake-managed Iceberg tables directly — no Snowflake warehouse involved.

```bash
source fleet_venv/bin/activate   # if not already active
pip install duckdb requests
python duckdb_interop.py
```

The script reads `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PAT` from **config.env**, then:

- Installs and loads the DuckDB Iceberg extension
- Creates a secret from your PAT
- Attaches `FLEET_DB` through the Horizon Iceberg REST endpoint
- Lists tables in `RAW`, queries `VEHICLE_REGISTRY`, and aggregates `SENSOR_READINGS` (the table you migrated in script 05)

```python
conn.execute("SHOW TABLES IN horizon.RAW").show()

conn.execute("""
    SELECT VEHICLE_ID,
           COUNT(*) AS reading_count,
           ROUND(AVG(ENGINE_TEMP_F), 1) AS avg_engine_temp,
           ROUND(AVG(FUEL_CONSUMPTION_GPH), 2) AS avg_fuel_gph
    FROM horizon.RAW.SENSOR_READINGS
    GROUP BY VEHICLE_ID
    ORDER BY avg_fuel_gph DESC
    LIMIT 10
""").show()
```

> **Tip**: DuckDB reads VARIANT columns as strings. Use `json_extract` to parse nested fields.

### Query from Apache Spark

Spark uses the same Horizon Catalog endpoint through its Iceberg REST catalog integration. Spark 4.0+ supports the Iceberg v3 VARIANT type natively.

```bash
deactivate                      # leave fleet_venv if active
conda activate fleet-spark
jupyter notebook spark_iceberg_interop.ipynb
```

> **Note**: PySpark 4.0 requires **Java 17 or 21** (check with `java -version`). Launch Jupyter from the `assets` directory — the notebook loads `config.env` from its working directory.

The first cell loads your settings from **config.env**; the second creates a Spark session with a `horizon` catalog:

```python
spark = SparkSession.builder \
    .appName('Fleet Analytics - Iceberg Interop') \
    .master('local[*]') \
    .config('spark.jars.packages',
            f'org.apache.iceberg:iceberg-spark-runtime-4.0_{SCALA_VERSION}:{ICEBERG_VERSION},'
            f'org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}') \
    .config('spark.sql.extensions',
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config('spark.sql.catalog.horizon', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.horizon.type', 'rest') \
    .config('spark.sql.catalog.horizon.uri',
            f'https://{SNOWFLAKE_ACCOUNT_URL}.snowflakecomputing.com/polaris/api/catalog') \
    .config('spark.sql.catalog.horizon.oauth2-server-uri',
            f'https://{SNOWFLAKE_ACCOUNT_URL}.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens') \
    .config('spark.sql.catalog.horizon.credential', SNOWFLAKE_PAT) \
    .config('spark.sql.catalog.horizon.warehouse', SNOWFLAKE_DATABASE) \
    .config('spark.sql.catalog.horizon.scope', f'session:role:{SNOWFLAKE_ROLE}') \
    .config('spark.sql.catalog.horizon.header.X-Iceberg-Access-Delegation',
            'vended-credentials') \
    .getOrCreate()
```

- The `horizon` catalog points at Snowflake's Iceberg REST endpoint
- Vended credentials let Spark read the Iceberg files without any cloud storage permissions of its own
- `scope` makes Spark operate under your Snowflake role, so Snowflake access control applies

Run the remaining cells to list tables, query the registry, and read VARIANT telemetry with `variant_get()`:

```python
spark.sql("""
    SELECT
        VEHICLE_ID,
        EVENT_TIMESTAMP,
        variant_get(TELEMETRY_DATA, '$.speed_mph', 'float') AS speed_mph,
        variant_get(TELEMETRY_DATA, '$.engine.temperature_f', 'int') AS engine_temp,
        variant_get(TELEMETRY_DATA, '$.location.lat', 'float') AS latitude,
        variant_get(TELEMETRY_DATA, '$.location.lon', 'float') AS longitude
    FROM horizon.RAW.VEHICLE_TELEMETRY_STREAM
    WHERE variant_get(TELEMETRY_DATA, '$.speed_mph', 'float') > 60
    LIMIT 10
""").show()
```

Dynamic Iceberg Tables appear as regular Iceberg tables to external engines too:

```python
spark.sql("""
    SELECT * FROM horizon.ANALYTICS.DAILY_FLEET_SUMMARY
    ORDER BY summary_date DESC
    LIMIT 10
""").show()
```

Snowflake runs the transformation pipeline; any Iceberg-compatible engine reads the results. No data movement, no ETL, no duplication.

<!-- ------------------------ -->
## Query with Snowflake Intelligence

With telemetry flowing and the pipeline refreshed, make the data available to business users through a Cortex Agent.

### Create a Semantic View

1. In Snowsight, navigate to **AI & ML → Analyst**
2. Click **Create New: Semantic View**
3. **Name**: `fleet_semantic_view`; **Location**: **FLEET_DB.ANALYTICS**
4. Click **Next** and skip the "Provide context" step
5. Select these tables:
   - `ANALYTICS.TELEMETRY_ENRICHED`
   - `ANALYTICS.DAILY_FLEET_SUMMARY`
   - `RAW.VEHICLE_REGISTRY`
   - `RAW.SENSOR_READINGS`
6. Click **Next**, select all columns, and click **Create and Save**

Cortex Analyst generates descriptions, logical names and relationships automatically. (`fleet_semantic_model.yaml` in the assets folder is a reference model over the same four tables.)

### Create a Cortex Agent

1. Navigate to **AI & ML → Agents** and click **Create agent**
2. **Database**: **FLEET_DB**; **Schema**: **ANALYTICS**; **Agent object name**: `fleet_agent`; **Display name**: `Fleet Analytics Agent`
3. Click **Create**
4. On the **Tools** tab, click **Add+** next to **Cortex Analyst** and select **FLEET_DB.ANALYTICS.FLEET_SEMANTIC_VIEW**
5. Name the tool, click **Generate with Cortex** for the description, then **Add** and **Save**

### Ask Questions

1. In **AI & ML → Agents**, open the **Snowflake Intelligence** tab, click **Add existing agent**, and select **fleet_agent**
2. Open **Snowflake Intelligence** from the left navigation and select **Fleet Analytics Agent**
3. Try:
   - "What are the top 5 fleet regions by average speed?"
   - "Show me vehicles with critical engine health events"
   - "How many check engine events occurred per region today?"
   - "Which drivers have the most telemetry events?"

> **Note**: If you see a region availability error, enable cross-region inference:
> ```sql
> USE ROLE ACCOUNTADMIN;
> ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
> ```

<!-- ------------------------ -->
## Clean Up

Back in the Workspace, run **06_cleanup.sql**. It drops `FLEET_DB` (including all tables, dynamic tables, stages and the agent) and the `FLEET_WH` warehouse.

Locally, you can remove the environments with:

```bash
rm -rf fleet_venv
conda env remove -n fleet-spark
```

<!-- ------------------------ -->
## Conclusion and Resources

You built a fleet analytics pipeline on Snowflake-managed Iceberg tables and proved that the same data is accessible from DuckDB and Apache Spark — without configuring cloud storage, without moving data, and without giving up Snowflake's governance and performance.

### What You Learned

- Creating Snowflake-managed Iceberg tables with `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'` — no cloud bucket required
- Batch-loading JSON files into Iceberg tables with `COPY INTO`
- Migrating a standard table to Iceberg with CTAS, and using Time Travel on Iceberg tables
- Building incremental transformation pipelines with Dynamic Iceberg Tables
- Streaming semi-structured JSON into VARIANT columns in Iceberg tables
- Querying fleet data with natural language through a Cortex Agent in Snowflake Intelligence
- Reading Snowflake-managed Iceberg tables from DuckDB and Apache Spark via Horizon Catalog, including VARIANT data with `variant_get()`

### Related Resources

- [Snowflake Storage for Apache Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg-internal-storage)
- [Query Iceberg Tables with an External Engine through Horizon Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon)
- [Dynamic Iceberg Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-create-iceberg)
- [Snowflake Intelligence](https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence)
