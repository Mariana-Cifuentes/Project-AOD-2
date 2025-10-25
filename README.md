# 🛰️ AOD–AERONET ETL & Dashboard  
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

## Expected dataset

- CSV including (among others):
  - `AERONET_Site`, `Date(dd:mm:yyyy)` or `Date`, `Day_of_Year`
  - `AOD_340nm`, `AOD_380nm`, …, `AOD_1640nm`
  - `440-870_Angstrom_Exponent`, `Precipitable_Water(cm)`
  - `Site_Latitude(Degrees)`, `Site_Longitude(Degrees)`, `Site_Elevation(m)`
- The sentinel **`-999`** is converted to **NULL** during extract.

---

## Airflow DAG: design & parameters

- **File**: `dags/etl.py`
- **ID**: `etl_aeronet_aod_polars`
- **Schedule**: `@daily` (`catchup=False`, `max_active_runs=1`, `concurrency=1`)
- **Tasks** (dependencies):  
  `extract` → `transform` → `weather_is_enriched` → `dq_quality` (gate) → `load`
- **Staging artifacts** in `STAGING_DIR` (dimension Parquets and **fact split into part‑files**).

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

## MySQL schema

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
