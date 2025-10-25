# AOD–AERONET ETL & Dashboard  
**Airflow + Polars + MySQL (+ Open‑Meteo, GeoPandas, Streamlit)**

> Reproducible pipeline to transform and load daily **AERONET AOD** averages into a **MySQL Data Warehouse**, enrich them with historical weather (Open‑Meteo), validate **data quality (DQ)**, and explore results in a **Streamlit dashboard**.

---

## Table of contents
- [Repository structure](#repository-structure)
- [Requirements](#requirements)
- [Configuration (.env)](#configuration-env)
- [How to run the project](#how-to-run-the-project)
- [Expected dataset](#expected-dataset)
- [Airflow DAG: design & parameters](#airflow-dag-design--parameters)
- [ETL steps (Extract → Transform → Enrich → DQ → Load)](#etl-steps-extract--transform--enrich--dq--load)
- [MySQL schema](#mysql-schema)
- [Data Quality (DQ) & gate](#data-quality-dq--gate)
- [Dashboard (Streamlit)](#dashboard-streamlit)
- [Assumptions & constraints](#assumptions--constraints)
- [Troubleshooting](#troubleshooting)
- [Credits & sources](#credits--sources)
- [License](#license)

---

## Repository structure

```
.
├── dags/
│   ├── etl.py                    # Airflow DAG (extract→transform→enrich→dq→load)
│   └── libs/
│       └── lib.py                # AerosolETL class (full ETL + DQ)
├── dashboard/
│   ├── data/
│   │   └── aod_1000_enriched_half.csv
│   ├── main.py                   # Streamlit app
│   ├── visuals.py                # Plotly chart helpers
│   └── requirements.txt          # dashboard-specific deps
├── data/
│   └── All_Sites_Times_Daily_Averages_AOD20.csv   # AERONET CSV (input)
├── api_meteo.ipynb               # helper notebook (optional)
├── EDA_Aerosoles.ipynb           # exploratory notebook (optional)
├── docker-compose.yml
├── requirements.txt              # project-wide deps
├── .env
├── .gitignore
├── .gitattributes
└── README.md
```

> The paths `./dags`, `./data`, `./plugins`, `./logs` are mounted under `/opt/airflow/...` inside the Airflow container.

---


## Requirements

- Docker and Docker Compose
- Internet access (Natural Earth download; Open‑Meteo API calls)
- **MySQL** reachable from the container (use an existing local/remote instance)
- Python 3.10+ to run the dashboard locally

---

## Configuration (.env)

Create a `.env` file at the repo root. Minimal example:

```ini
# Paths & files
DATA_DIR=/opt/airflow/data
CSV_FILENAME=All_Sites_Times_Daily_Averages_AOD20.csv
STAGING_DIR=/opt/airflow/data/staging
SAMPLE_CSV_NAME=aod_sample_500.csv
SAMPLE_TARGET=50
SAMPLE_SEED=42

# MySQL (Data Warehouse)
MYSQL_HOST=host.docker.internal   # or the IP/host of your MySQL
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_DB=aerosol_dw
MYSQL_FASTLOAD=1                  # enable LOAD DATA LOCAL INFILE
BATCH_SIZE=20000
MYSQL_SESSION_TWEAKS=1

# Data Quality
DQ_MIN_SCORE_PCT=90.0

# Weather (extra knobs in docker-compose)
WEATHER_MAX_REQUESTS=5000
```

> If you enable `MYSQL_FASTLOAD=1`, turn on `local_infile=1` on the MySQL server and make sure the user has permission.

---

## How to run the project

1. Copy the AERONET CSV to `./data/All_Sites_Times_Daily_Averages_AOD20.csv`.
2. Adjust `.env` with your MySQL credentials and paths.
3. Start services:
   ```bash
   docker compose up -d
   ```
4. Open **Airflow UI** at `http://localhost:8080` (user/password: `airflow / airflow`).
5. The `etl_aeronet_aod_polars` DAG comes **unpaused**; you can trigger it manually if desired.

---

##  **Project Objective**

The main goal of this project is to build a complete **ETL pipeline** to process and analyze atmospheric aerosol data (AOD) from NASA’s **AERONET** network, enriched with meteorological variables from the **Open-Meteo Historical API**.

The pipeline extracts, cleans, transforms, validates, and loads the data into a **MySQL Data Warehouse** using a star schema.
This allows faster queries and visual analysis of how aerosols interact with climate conditions such as temperature, humidity, radiation, and wind.

---

##  **Project Context**

Atmospheric aerosols are tiny particles suspended in the air.
Although invisible, they have a major impact on **air quality, human health, and climate** — they can absorb or scatter sunlight, affect cloud formation, and even change rainfall patterns.

NASA created **AERONET**, a global observation network that measures **Aerosol Optical Depth (AOD)**, which tells us how much sunlight is blocked by particles in the atmosphere.

However, AOD alone does not explain *why* the aerosol concentration changes.
To understand the causes, we integrated AERONET data with **meteorological variables** from the Open-Meteo API, such as temperature, humidity, radiation, wind speed, and evapotranspiration.
This combination allows us to study not only *how many* aerosols are present, but also *why* their levels increase or decrease under different weather conditions.

---

##  **General Pipeline Flow**

The following flow summarizes the full **AOD ETL pipeline**:

<img width="1056" height="451" alt="image" src="https://github.com/user-attachments/assets/234ba5c0-e924-4b0e-a654-855aa13c7c21" />

The workflow shown in the diagram automates every step — from data collection to visualization — ensuring clean, validated, and enriched data ready for analysis.

1. **Data Extraction**

   * Source: **AERONET CSV dataset (NASA)** and **Open-Meteo API** for historical weather data.
   * We use **Polars** (a fast DataFrame engine) to efficiently load and clean millions of records, replacing invalid values like `-999` with `NULL`.

2. **Transformation**

   * The dataset is normalized, cleaned, and converted from *wide* to *long* format.
   * New analytical columns are created such as `Spectral_Band`, `Sensitive_Aerosol`, and `Particle_Type`.
   * Results are saved in **Parquet** format — optimized for columnar storage and fast processing.

3. **Enrichment & Merge**

   * Using **Open-Meteo API**, we enrich each AOD record with daily climate variables (temperature, humidity, radiation, wind, etc.).
   * The merge key `(Date, Latitude, Longitude)` ensures accurate matching between AERONET and weather data.

4. **Data Quality Validation**

   * A full **Data Quality (DQ)** stage checks **Completeness, Consistency, Uniqueness, and Validity**.
   * If the overall quality KPI is below **90%**, the pipeline automatically stops — preventing bad data from being loaded.

5. **Load to Data Warehouse**

   * The validated data is loaded into a **MySQL Data Warehouse**, structured in a **Star Schema** (Fact + Dimensions).
   * This design supports efficient analytical queries and dashboards.

6. **Visualization & Analysis**

   * Exploratory Data Analysis (**EDA**) is performed in **Jupyter Notebooks**.
   * A **Streamlit dashboard** provides an interactive interface to explore AOD and climate insights.
   * The entire workflow, including code, configuration, and artifacts, is **version-controlled on GitHub** for transparency and reproducibility.

---

##  **Airflow DAG Overview**

The entire ETL process is orchestrated in **Apache Airflow** through a DAG named `etl_aeronet_aod_polars`.
This DAG automatically controls dependencies and execution order across the pipeline.

<img width="1690" height="191" alt="image" src="https://github.com/user-attachments/assets/732b6b9f-1e28-473c-9b67-417f74ef0236" />

**Tasks Overview:**

1. **extract** → Reads and cleans the original AERONET CSV file.
2. **transform** → Applies transformations with Polars, creates dimensions and fact tables, and writes outputs in Parquet format.
3. **weather_is_enriched** → Calls the Open-Meteo API and stores daily climate data as a Parquet file.
4. **dq_quality** → Performs full data quality validation (completeness, consistency, uniqueness, and validity).

   * If the global KPI is **below 90%**, the DAG stops automatically.
5. **load** → Loads validated data into the MySQL Data Warehouse.
---

## ETL steps (Extract → Transform → Enrich → DQ → Load)

### 1) Extract (`AerosolETL.extract`)
- **Polars** read with explicit dtypes, sanitize `-999→NULL`.
- Date parsing: `Date(dd:mm:yyyy)` → `Date`.

### 2) Transform (`transform_streaming` / `transform`)
- **Wide → Long**: `AOD_*nm` → `AOD_Value` @ `Wavelength_nm`.
- Derived fields: `Spectral_Band` (UV/VIS/NIR), `Sensitive_Aerosol`, `Particle_type` from `Angstrom_Exponent` (`fine/mixed/coarse`).
- `AOD_Value` **clipped to ≥ 0**.
- **Dimensions**: `dim_wavelength`, `dim_date`, `dim_site` (enriched with **Natural Earth** via GeoPandas).
- **Facts**: `fact_aod` written as **part‑files** Parquet.
- **Sample** (`SAMPLE_CSV_NAME`) to bootstrap weather enrichment.

### 3) Enrich (`t_enrich_weather`)
- Uses the **sample** (Date/Lat/Lon) to query **Open‑Meteo** and produce a Parquet with:
  `temperature_mean`, `radiation_sum`, `humidity_mean`, `wind_speed_max`,
  `wind_direction_dominant`, `evapotranspiration`, `sunshine_duration_sec`.

### 4) DQ (`AerosolETL.dq_validate_transformed_paths`) with **gate**
- KPIs for **completeness, consistency, uniqueness, validity**; reports at `data/reports/*.json|.txt`.
- Configurable gate: `DQ_MIN_SCORE_PCT` (90% by default).

### 5) Load (`AerosolETL.load_to_db_from_paths`)
- Creates/validates `dim_*` and `fact_aod` in MySQL.
- Loads `dim_weather` and **enriches** facts with `id_weather` / `is_enriched` when there is a match (join on `Date, Latitude, Longitude` rounded to 6 decimals).
- Inserts:
  - **Fastload** (`LOAD DATA LOCAL INFILE`) if `MYSQL_FASTLOAD=1`,
  - or **batched INSERT** (`BATCH_SIZE`).

---

## MySQL Star Schema

<img width="623" height="750" alt="image" src="https://github.com/user-attachments/assets/e4f8aa5b-cb3e-4aab-aa9a-ba9dfcf4ac02" />


- **dim_wavelength**: `id_wavelength (PK)`, `Wavelength_nm`, `Spectral_Band`, `Sensitive_Aerosol`
- **dim_date**: `id_date (PK)`, `Date`, `Year`, `Month`, `Day`, `Day_of_Year`
- **dim_site**: `id_site (PK)`, `AERONET_Site`, `Latitude`, `Longitude`, `Elevation`, `Country`, `Continent`
- **dim_weather**: `id_weather (PK)`, `temperature_mean`, `radiation_sum`, `humidity_mean`, `wind_speed_max`, `wind_direction_dominant`, `evapotranspiration`, `sunshine_duration_sec`
- **fact_aod**: `Fact_ID (PK)`, `id_date (FK)`, `id_wavelength (FK)`, `id_site (FK)`, `id_weather (FK, NULL)`, `Particle_type`, `AOD_Value`, `Precipitable_Water`, `Angstrom_Exponent`, `is_enriched (0/1)`

---

## Data Quality (DQ) & gate

- **Completeness**: % non‑nulls in key columns.
- **Consistency**: resolvable FKs; `Day_of_Year` vs `Date`; `Particle_type`–`Angstrom_Exponent` and `Spectral_Band`–`Wavelength_nm` coherence; weather coverage.
- **Uniqueness**: duplicates in `Fact_ID` and in `(id_date,id_wavelength,id_site)`.
- **Validity**: physical ranges (`AOD≥0`, extremes>3), `PW≥0`, `-1≤α≤5`, valid lat/lon.

---

## Dashboard (Streamlit)

**Local requirements:**
```bash
python -m pip install -r dashboard/requirements.txt
# or use the root requirements.txt if you consolidate deps
```

**Run:**
```bash
cd dashboard
streamlit run main.py
```
- By default, the script looks for `dashboard/data/aod_1000_enriched_half.csv` (you can upload another CSV from the UI).
- Charts: map with **layers** (AOD/Temperature/Humidity), counts by continent and particle type, hist/line plots, correlation heatmap, regressions, and a scatter‑matrix.

---

## Assumptions & constraints

1. **Sentinel `-999` → NULL** (missing values).  
2. **Non‑negative AOD** (clipped to `≥ 0`).  
3. **Classification by α**: `fine (≥1.5)`, `coarse (≤1.0)`, `mixed` (between both).  
4. **Weather join** on `(Date, Latitude, Longitude)` rounded to 6 decimals.  
5. **Deterministic IDs** via `row_count` with offset=1.  
6. **Streaming transform** over **chunks** of AOD columns (bounded memory).  
7. **GeoPandas/rtree** required for `sjoin` (Natural Earth is downloaded into `data/`).  
8. **External MySQL**: not started by `docker-compose.yml`; provide host/port/credentials.  
9. **Optional fastload** (`MYSQL_FASTLOAD=1` + `local_infile=1`).  
10. **DQ gate** blocks the load if the overall KPI falls below the threshold.  

---

## Troubleshooting

- **`LOAD DATA LOCAL INFILE`**: enable `local_infile=1` in MySQL and allow `allow_local_infile`.  
- **`sjoin` failures**: install `rtree`/`pygeos` and verify the Natural Earth shapefile fully downloaded.  
- **Open‑Meteo rate limiting**: the task uses caching and small sleeps (`sleep(1)`); reduce `SAMPLE_TARGET` if needed.  
- **CSV not found**: confirm the name/path in `.env` and that `./data/<CSV>` exists.  
- **DQ gate**: check `data/reports/*.txt` for causes (FKs, ranges, uniqueness, etc.).

---

## Credits & sources

- **AERONET** (Aerosol Optical Depth, AOD).  
- **Open‑Meteo** (ERA5‑based historical weather).  
- **Natural Earth** (countries/continents).
