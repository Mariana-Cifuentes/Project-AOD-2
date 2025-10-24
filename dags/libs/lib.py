import math
import os
import re
import glob
from typing import Tuple, List, Dict, Optional

import requests
from dotenv import load_dotenv

import polars as pl
import numpy as np

import mysql.connector as mysql
from mysql.connector import errorcode

#Geo 
try:
    import geopandas as gpd
    from shapely.geometry import Point  # noqa: F401
    _HAS_GPD = True
except Exception as _e:
    _HAS_GPD = False
    _GPD_IMPORT_ERR = _e


__all__ = ["AerosolETL"]


class AerosolETL:
    # Natural Earth
    
    NE_URL = "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_0_countries.zip"
    NE_BASENAME = "ne_50m_admin_0_countries"
    NE_SIDEKICKS = [".shp", ".dbf", ".shx", ".prj"]

    # Ångström rules
    AE_FINE_TH = 1.5
    AE_COARSE_TH = 1.0

    def __init__(self) -> None:
        load_dotenv()
        # Rutas fuente
        self.DATA_DIR = os.getenv("DATA_DIR", "data")
        os.makedirs(self.DATA_DIR, exist_ok=True)
        self.CSV_FILENAME = os.getenv("CSV_FILENAME", "All_Sites_Times_Daily_Averages_AOD20.csv")
        self.CSV_PATH = os.path.join(self.DATA_DIR, self.CSV_FILENAME)

        # STAGING 
        self.STAGING_DIR = os.getenv("STAGING_DIR", "/opt/airflow/data/staging")
        os.makedirs(self.STAGING_DIR, exist_ok=True)
        
        self.WEATHER_GLOB = os.getenv("WEATHER_GLOB", "weather_daily_*.parquet")

        # Natural Earth
        self.NE_ZIP_PATH = os.path.join(self.DATA_DIR, f"{self.NE_BASENAME}.zip")
        self.NE_SHP_PATH = os.path.join(self.DATA_DIR, f"{self.NE_BASENAME}.shp")

        # MySQL
        self.MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
        self.MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
        self.MYSQL_USER = os.getenv("MYSQL_USER", "root")
        self.MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
        self.MYSQL_DB = os.getenv("MYSQL_DB")

        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))
        self.MYSQL_FASTLOAD = os.getenv("MYSQL_FASTLOAD")  # cualquier valor activa
        self.MYSQL_SESSION_TWEAKS = os.getenv("MYSQL_SESSION_TWEAKS", "1") == "1"

        # Sampling output config (por defecto 10; configurable por env vars)
        self.SAMPLE_CSV_NAME = os.getenv("SAMPLE_CSV_NAME", "aod_sample_500.csv")
        self.SAMPLE_SEED = int(os.getenv("SAMPLE_SEED", "42"))
        self.SAMPLE_TARGET = int(os.getenv("SAMPLE_TARGET", "50"))

    
    # EXTRACT
    

    def _detect_columns(self) -> Tuple[List[str], List[str]]:
        import csv
        with open(self.CSV_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
        cols = header
        aod_cols = [c for c in cols if re.fullmatch(r"AOD_\d+nm", c)]
        base_cols = [
            "AERONET_Site",
            "Date(dd:mm:yyyy)", "Date", "Day_of_Year",
            "Precipitable_Water(cm)", "440-870_Angstrom_Exponent",
            "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)",
        ]
        usecols = [c for c in base_cols if c in cols] + aod_cols
        if not aod_cols:
            raise ValueError("No se encontraron columnas AOD_*nm en el CSV.")
        return usecols, aod_cols

    def _dtype_map(self, usecols: List[str]) -> dict:
        dtypes: dict = {}
        for c in usecols:
            if re.fullmatch(r"AOD_\d+nm", c):
                dtypes[c] = pl.Float32
        for c in ["Precipitable_Water(cm)", "440-870_Angstrom_Exponent",
                  "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"]:
            if c in usecols:
                dtypes[c] = pl.Float32
        if "Day_of_Year" in usecols:
            dtypes["Day_of_Year"] = pl.Float32
        if "AERONET_Site" in usecols:
            dtypes["AERONET_Site"] = pl.Utf8
        if "Date(dd:mm:yyyy)" in usecols:
            dtypes["Date(dd:mm:yyyy)"] = pl.Utf8
        if "Date" in usecols:
            dtypes["Date"] = pl.Utf8
        return dtypes

    def extract(self) -> pl.DataFrame:
        usecols, _ = self._detect_columns()
        dtypes = self._dtype_map(usecols)
        df = pl.read_csv(self.CSV_PATH, columns=usecols, dtypes=dtypes, ignore_errors=True)

        numeric_like = [c for c in usecols if c not in ("AERONET_Site", "Date(dd:mm:yyyy)", "Date")]
        repl_exprs = [pl.when(pl.col(c).is_in([-999.0, -999])).then(None).otherwise(pl.col(c)).alias(c)
                      for c in numeric_like if c in df.columns]
        if repl_exprs:
            df = df.with_columns(repl_exprs)

        if "Date(dd:mm:yyyy)" in df.columns:
            df = df.with_columns(
                pl.col("Date(dd:mm:yyyy)").str.strptime(pl.Date, format="%d:%m:%Y", strict=False).alias("Date")
            ).drop("Date(dd:mm:yyyy)")
        elif "Date" in df.columns and df.schema["Date"] == pl.Utf8:
            df = df.with_columns(pl.col("Date").str.strptime(pl.Date, strict=False))

        return df

    # TRANSFORM 

    @staticmethod
    def _spectral_band_expr():
        return (
            pl.when(pl.col("Wavelength_nm") < 400).then(pl.lit("UV"))
            .when(pl.col("Wavelength_nm") <= 700).then(pl.lit("VIS"))
            .otherwise(pl.lit("NIR"))
        )

    @staticmethod
    def _sensitive_aerosol_expr():
        return (
            pl.when(pl.col("Wavelength_nm") <= 500).then(pl.lit("fine-sensitive"))
            .when(pl.col("Wavelength_nm") >= 800).then(pl.lit("coarse-sensitive"))
            .otherwise(pl.lit("balanced"))
        )

    def _build_dim_site_with_country_continent_from_sites(self, df_sites: pl.DataFrame) -> pl.DataFrame:
        if not _HAS_GPD:
            raise RuntimeError(
                f"GeoPandas es requerido para el enriquecimiento geográfico y no está disponible. Detalle: {_GPD_IMPORT_ERR}"
            )

        site_cols = ["AERONET_Site", "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"]
        available_cols = [c for c in site_cols if c in df_sites.columns]

        dim_site = (
            df_sites.select(available_cols)
            .unique()
            .rename({
                "Site_Latitude(Degrees)": "Latitude",
                "Site_Longitude(Degrees)": "Longitude",
                "Site_Elevation(m)": "Elevation",
            })
        )

        dim_site = dim_site.with_columns([
            pl.col("Latitude").cast(pl.Float64, strict=False),
            pl.col("Longitude").cast(pl.Float64, strict=False),
        ])

        swap_mask = (pl.col("Latitude").abs() > 90) & (pl.col("Longitude").abs() <= 90)
        dim_site = dim_site.with_columns([
            pl.when(swap_mask).then(pl.col("Longitude")).otherwise(pl.col("Latitude")).alias("Latitude"),
            pl.when(swap_mask).then(pl.col("Latitude")).otherwise(pl.col("Longitude")).alias("Longitude"),
        ])

        dim_site = dim_site.filter(
            (pl.col("Latitude").is_between(-90, 90, closed="both")) &
            (pl.col("Longitude").is_between(-180, 180, closed="both"))
        ).unique()

        dim_site = dim_site.with_row_count(name="id_site", offset=1)

        self._ensure_ne_countries()
        dim_site_pd = dim_site.to_pandas()

        g_sites = gpd.GeoDataFrame(
            dim_site_pd,
            geometry=gpd.points_from_xy(dim_site_pd["Longitude"], dim_site_pd["Latitude"]),
            crs="EPSG:4326",
        )
        world = gpd.read_file(self.NE_SHP_PATH).to_crs("EPSG:4326")

        if "ADMIN" in world.columns:
            world = world.rename(columns={"ADMIN": "Country"})
        if "CONTINENT" in world.columns:
            world = world.rename(columns={"CONTINENT": "Continent"})
        cols_keep = [c for c in ["Country", "Continent", "geometry"] if c in world.columns]
        if "Country" not in world.columns or "Continent" not in world.columns:
            raise RuntimeError("El shapefile no trae columnas 'Country'/'Continent'.")

        try:
            joined = gpd.sjoin(g_sites, world[cols_keep], predicate="within", how="left")
        except Exception as e:
            raise RuntimeError("Falló el sjoin (¿falta índice espacial? Instala rtree o pygeos).") from e

        na_mask = joined["Country"].isna()
        if na_mask.any():
            joined2 = gpd.sjoin(g_sites.loc[na_mask], world[cols_keep], predicate="intersects", how="left")
            joined.loc[na_mask, ["Country", "Continent"]] = joined2[["Country", "Continent"]].values

        joined = joined.drop(columns=[c for c in ["geometry", "index_right"] if c in joined.columns])
        dim_site = pl.from_pandas(joined)
        cols_order = ["id_site", "AERONET_Site", "Latitude", "Longitude", "Elevation", "Country", "Continent"]
        dim_site = dim_site.select([c for c in cols_order if c in dim_site.columns])
        return dim_site

    # Natural Earth
    def _ne_shapefile_present(self) -> bool:
        return all(os.path.exists(os.path.join(self.DATA_DIR, self.NE_BASENAME + ext)) for ext in self.NE_SIDEKICKS)

    def _ne_download_zip(self) -> None:
        headers = {"User-Agent": "Mozilla/5.0"}
        with requests.get(self.NE_URL, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(self.NE_ZIP_PATH, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)

    def _ensure_ne_countries(self) -> None:
        if self._ne_shapefile_present():
            return
        if not os.path.exists(self.NE_ZIP_PATH):
            print("[Geo] Descargando Natural Earth...")
            self._ne_download_zip()
        print("[Geo] Extrayendo shapefile...")
        with zipfile.ZipFile(self.NE_ZIP_PATH, "r") as z:
            z.extractall(self.DATA_DIR)

    # Transform 
    def transform(self, df: pl.DataFrame):
        ae_col = "440-870_Angstrom_Exponent"
        maybe_numeric = [
            "AOD_340nm", "AOD_380nm", "AOD_400nm", "AOD_440nm", "AOD_443nm", "AOD_490nm", "AOD_500nm",
            "AOD_510nm", "AOD_532nm", "AOD_551nm", "AOD_555nm", "AOD_560nm", "AOD_620nm", "AOD_667nm",
            "AOD_675nm", "AOD_681nm", "AOD_709nm", "AOD_779nm", "AOD_865nm", "AOD_870nm", "AOD_1020nm",
            "AOD_1640nm", "Precipitable_Water(cm)", ae_col,
            "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)",
        ]
        cast_exprs = [pl.col(col).cast(pl.Float32, strict=False).alias(col) for col in maybe_numeric if col in df.columns]
        if cast_exprs:
            df = df.with_columns(cast_exprs)

        if ae_col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(ae_col).is_null()).then(None)
                .when(pl.col(ae_col) >= self.AE_FINE_TH).then(pl.lit("fine"))
                .when(pl.col(ae_col) <= self.AE_COARSE_TH).then(pl.lit("coarse"))
                .otherwise(pl.lit("mixed"))
                .alias("Particle_type")
            )
        else:
            df = df.with_columns(pl.lit(None).alias("Particle_type"))

        aod_cols = [c for c in df.columns if re.fullmatch(r"AOD_\d+nm", c)]
        if not aod_cols:
            raise ValueError("No AOD_*nm columns found en el DataFrame.")

        id_vars = [c for c in [
            "AERONET_Site", "Date", "Day_of_Year", "Precipitable_Water(cm)", ae_col, "Particle_type",
            "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)",
        ] if c in df.columns]

        df_long = df.melt(id_vars=id_vars, value_vars=aod_cols, variable_name="AOD_band", value_name="AOD_Value")

        df_long = df_long.with_columns([
            pl.col("AOD_band").str.extract(r"AOD_(\d+)nm", group_index=1).cast(pl.Float64).alias("Wavelength_nm"),
        ])
        df_long = df_long.with_columns([
            self._spectral_band_expr().alias("Spectral_Band"),
            self._sensitive_aerosol_expr().alias("Sensitive_Aerosol"),
        ])
        df_long = df_long.with_columns(pl.col("AOD_Value").cast(pl.Float64, strict=False).clip(lower_bound=0).alias("AOD_Value"))
        df_long = df_long.drop_nulls(["AOD_Value", "Wavelength_nm"])

        dim_wavelength = (
            df_long.select(["Wavelength_nm", "Spectral_Band", "Sensitive_Aerosol"])
            .unique().sort("Wavelength_nm")
            .with_row_count(name="id_wavelength", offset=1)
        )
        dim_date = (
            df_long.select(["Date"]).unique().sort("Date")
            .with_row_count(name="id_date", offset=1)
            .with_columns([
                pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
                pl.col("Date").dt.month().cast(pl.Int32).alias("Month"),
                pl.col("Date").dt.day().cast(pl.Int32).alias("Day"),
                pl.col("Date").dt.ordinal_day().cast(pl.Int32).alias("Day_of_Year"),
            ])
        )
        dim_site_input = df.select(["AERONET_Site", "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"])
        dim_site = self._build_dim_site_with_country_continent_from_sites(dim_site_input)

        fact_df = (
            df_long
            .join(dim_date.select(["id_date", "Date"]), on="Date", how="left")
            .join(dim_wavelength.select(["id_wavelength", "Wavelength_nm"]), on="Wavelength_nm", how="left")
            .join(dim_site.select(["id_site", "AERONET_Site"]), on="AERONET_Site", how="left")
            .rename({"Precipitable_Water(cm)": "Precipitable_Water", ae_col: "Angstrom_Exponent"})
        )
        fact_df = (
            fact_df.with_row_count(name="Fact_ID", offset=1)
            .select([
                "Fact_ID", "id_date", "id_wavelength", "id_site",
                "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
            ])
        )
        return fact_df, dim_wavelength, dim_date, dim_site

    # Transform streaming por lotes 
    def transform_streaming(self, raw_parquet_path: str, out_dir: str, aod_chunk_size: int = 6) -> Dict[str, str]:
        os.makedirs(out_dir, exist_ok=True)
        fact_dir = os.path.join(out_dir, "fact_parts")
        os.makedirs(fact_dir, exist_ok=True)

        lazy = pl.scan_parquet(raw_parquet_path)
        cols = list(lazy.schema.keys())

        ae_col = "440-870_Angstrom_Exponent"
        aod_cols = [c for c in cols if re.fullmatch(r"AOD_\d+nm", c)]
        if not aod_cols:
            raise ValueError("No AOD_*nm columns en el Parquet de entrada.")

        dim_wavelength = pl.DataFrame({
            "Wavelength_nm": [float(re.findall(r"\d+", c)[0]) for c in aod_cols],
        }).with_columns([
            self._spectral_band_expr().alias("Spectral_Band"),
            self._sensitive_aerosol_expr().alias("Sensitive_Aerosol"),
        ]).sort("Wavelength_nm").with_row_count(name="id_wavelength", offset=1)

        dim_date = (
            lazy.select(pl.col("Date")).unique().collect(streaming=True)
            .sort("Date")
            .with_row_count(name="id_date", offset=1)
            .with_columns([
                pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
                pl.col("Date").dt.month().cast(pl.Int32).alias("Month"),
                pl.col("Date").dt.day().cast(pl.Int32).alias("Day"),
                pl.col("Date").dt.ordinal_day().cast(pl.Int32).alias("Day_of_Year"),
            ])
        )

        site_needed = [c for c in ["AERONET_Site", "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)"] if c in cols]
        dim_site_input = lazy.select([pl.col(c) for c in site_needed]).unique().collect(streaming=True)
        dim_site = self._build_dim_site_with_country_continent_from_sites(dim_site_input)

        out_w = os.path.join(out_dir, "dim_wavelength.parquet")
        out_d = os.path.join(out_dir, "dim_date.parquet")
        out_s = os.path.join(out_dir, "dim_site.parquet")
        dim_wavelength.write_parquet(out_w)
        dim_date.write_parquet(out_d)
        dim_site.write_parquet(out_s)

        map_date = dim_date.select(["id_date", "Date"])
        map_wav = dim_wavelength.select(["id_wavelength", "Wavelength_nm"])
        map_site = dim_site.select(["id_site", "AERONET_Site"])

        id_vars = [c for c in [
            "AERONET_Site", "Date", "Day_of_Year",
            "Precipitable_Water(cm)", ae_col,
            "Site_Latitude(Degrees)", "Site_Longitude(Degrees)", "Site_Elevation(m)",
        ] if c in cols]

        next_fact_id = 1
        part_idx = 1

        for i in range(0, len(aod_cols), aod_chunk_size):
            subset = aod_cols[i:i + aod_chunk_size]
            df_chunk = (
                pl.scan_parquet(raw_parquet_path)
                .select([pl.col(c) for c in (id_vars + subset)])
                .collect()
            )

            if ae_col in df_chunk.columns:
                df_chunk = df_chunk.with_columns(
                    pl.when(pl.col(ae_col).is_null()).then(None)
                    .when(pl.col(ae_col) >= self.AE_FINE_TH).then(pl.lit("fine"))
                    .when(pl.col(ae_col) <= self.AE_COARSE_TH).then(pl.lit("coarse"))
                    .otherwise(pl.lit("mixed"))
                    .alias("Particle_type")
                )
            else:
                df_chunk = df_chunk.with_columns(pl.lit(None).alias("Particle_type"))

            df_long = df_chunk.melt(
                id_vars=[c for c in id_vars if c in df_chunk.columns] + ["Particle_type"],
                value_vars=subset,
                variable_name="AOD_band",
                value_name="AOD_Value",
            )

            df_long = df_long.with_columns([
                pl.col("AOD_band").str.extract(r"AOD_(\d+)nm", group_index=1).cast(pl.Float64).alias("Wavelength_nm"),
                pl.col("AOD_Value").cast(pl.Float64, strict=False).clip(lower_bound=0).alias("AOD_Value"),
            ])
            df_long = df_long.with_columns([
                self._spectral_band_expr().alias("Spectral_Band"),
                self._sensitive_aerosol_expr().alias("Sensitive_Aerosol"),
            ])
            df_long = df_long.drop_nulls(["AOD_Value", "Wavelength_nm"])

            fact_part = (
                df_long
                .join(map_date, on="Date", how="left")
                .join(map_wav, on="Wavelength_nm", how="left")
                .join(map_site, on="AERONET_Site", how="left")
                .rename({"Precipitable_Water(cm)": "Precipitable_Water", ae_col: "Angstrom_Exponent"})
                .select([
                    "id_date", "id_wavelength", "id_site",
                    "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
                ])
            )

            nrows = fact_part.height
            if nrows == 0:
                continue
            fact_part = fact_part.with_row_count(name="Fact_ID", offset=next_fact_id).select([
                "Fact_ID", "id_date", "id_wavelength", "id_site",
                "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
            ])
            next_fact_id += nrows

            out_part = os.path.join(fact_dir, f"fact_aod_part_{part_idx:03d}.parquet")
            fact_part.write_parquet(out_part)
            part_idx += 1

        paths_dict = {"fact_dir": fact_dir, "wavelength": out_w, "date": out_d, "site": out_s}
        try:
            self._write_sample_geo_csv_from_paths(paths_dict)
        except Exception as e:
            print(f"[Sample] Advertencia: no se pudo escribir el CSV de muestra (transform_streaming): {e}")

        return paths_dict

    #LOAD 
    def _connect_db(self, for_fastload: bool = False):
        try:
            conn = mysql.connect(
                host=self.MYSQL_HOST,
                port=self.MYSQL_PORT,
                user=self.MYSQL_USER,
                password=self.MYSQL_PASSWORD,
                database=self.MYSQL_DB,
                autocommit=False,
                allow_local_infile=True if (self.MYSQL_FASTLOAD or for_fastload) else False,
            )
            return conn
        except mysql.Error as e:
            if e.errno == errorcode.ER_BAD_DB_ERROR:
                raise SystemExit(f"Schema '{self.MYSQL_DB}' does not exist. Create it first.")
            else:
                raise SystemExit(f"Error connecting to MySQL: {e}")

    @staticmethod
    def _run(cursor, sql: str) -> None:
        cursor.execute(sql)

    def _bulk_session_on(self, cur) -> None:
        if not self.MYSQL_SESSION_TWEAKS:
            return
        for sql in ("SET FOREIGN_KEY_CHECKS=0;", "SET UNIQUE_CHECKS=0;", "SET autocommit=0;"):
            try:
                self._run(cur, sql)
            except Exception:
                pass

    def _bulk_session_off(self, cur) -> None:
        if not self.MYSQL_SESSION_TWEAKS:
            return
        for sql in ("SET UNIQUE_CHECKS=1;", "SET FOREIGN_KEY_CHECKS=1;"):
            try:
                self._run(cur, sql)
            except Exception:
                pass

    # Dim_Weather
    def _create_schema_objects(self) -> None:
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            
            self._run(cur, """
            CREATE TABLE IF NOT EXISTS dim_wavelength (
                id_wavelength INT PRIMARY KEY,
                Wavelength_nm DOUBLE NOT NULL,
                Spectral_Band VARCHAR(10),
                Sensitive_Aerosol VARCHAR(20),
                UNIQUE KEY uq_wavelength (Wavelength_nm)
            ) ENGINE=InnoDB;""")
            self._run(cur, """
            CREATE TABLE IF NOT EXISTS dim_date (
                id_date INT PRIMARY KEY,
                Date DATE NOT NULL,
                Year SMALLINT NOT NULL,
                Month TINYINT NOT NULL,
                Day TINYINT NOT NULL,
                Day_of_Year SMALLINT NOT NULL,
                UNIQUE KEY uq_date (Date)
            ) ENGINE=InnoDB;""")
            self._run(cur, """
            CREATE TABLE IF NOT EXISTS dim_site (
                id_site INT PRIMARY KEY,
                AERONET_Site VARCHAR(150),
                Latitude DECIMAL(9,6),
                Longitude DECIMAL(9,6),
                Elevation DOUBLE,
                Country VARCHAR(120),
                Continent VARCHAR(60),
                KEY ix_site_name (AERONET_Site),
                KEY ix_site_latlon (Latitude, Longitude)
            ) ENGINE=InnoDB;""")

            # Dim Weather
            self._run(cur, """
            CREATE TABLE IF NOT EXISTS dim_weather (
                id_weather INT PRIMARY KEY,
                temperature_mean DOUBLE NULL,
                radiation_sum DOUBLE NULL,
                humidity_mean DOUBLE NULL,
                wind_speed_max DOUBLE NULL,
                wind_direction_dominant DOUBLE NULL,
                evapotranspiration DOUBLE NULL,
                sunshine_duration_sec DOUBLE NULL
            ) ENGINE=InnoDB;""")

            # Fact 
            self._run(cur, """
            CREATE TABLE IF NOT EXISTS fact_aod (
                Fact_ID BIGINT PRIMARY KEY,
                id_date INT NOT NULL,
                id_wavelength INT NOT NULL,
                id_site INT NOT NULL,
                id_weather INT NULL,
                Particle_type VARCHAR(10),
                AOD_Value DOUBLE NOT NULL,
                Precipitable_Water DOUBLE NULL,
                Angstrom_Exponent DOUBLE NULL,
                is_enriched TINYINT(1) NOT NULL DEFAULT 0,
                KEY ix_date (id_date),
                KEY ix_wavelength (id_wavelength),
                KEY ix_site (id_site),
                KEY ix_weather (id_weather),
                CONSTRAINT fk_fact_date FOREIGN KEY (id_date) REFERENCES dim_date(id_date)
                    ON UPDATE CASCADE ON DELETE RESTRICT,
                CONSTRAINT fk_fact_wavelength FOREIGN KEY (id_wavelength) REFERENCES dim_wavelength(id_wavelength)
                    ON UPDATE CASCADE ON DELETE RESTRICT,
                CONSTRAINT fk_fact_site FOREIGN KEY (id_site) REFERENCES dim_site(id_site)
                    ON UPDATE CASCADE ON DELETE RESTRICT,
                CONSTRAINT fk_fact_weather FOREIGN KEY (id_weather) REFERENCES dim_weather(id_weather)
                    ON UPDATE CASCADE ON DELETE SET NULL
            ) ENGINE=InnoDB;""")

            # ALTERs
            try:
                self._run(cur, "ALTER TABLE fact_aod ADD COLUMN IF NOT EXISTS id_weather INT NULL;")
            except Exception:
                pass
            try:
                self._run(cur, "ALTER TABLE fact_aod ADD COLUMN IF NOT EXISTS is_enriched TINYINT(1) NOT NULL DEFAULT 0;")
            except Exception:
                pass
            try:
                self._run(cur, "ALTER TABLE fact_aod ADD KEY IF NOT EXISTS ix_weather (id_weather);")
            except Exception:
                pass
            try:
                self._run(cur, "ALTER TABLE fact_aod ADD CONSTRAINT fk_fact_weather FOREIGN KEY (id_weather) REFERENCES dim_weather(id_weather) ON UPDATE CASCADE ON DELETE SET NULL;")
            except Exception:
                pass

            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _truncate_tables(self) -> None:
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            self._run(cur, "SET FOREIGN_KEY_CHECKS=0;")
            for tbl in ("fact_aod", "dim_weather", "dim_wavelength", "dim_date", "dim_site"):
                self._run(cur, f"TRUNCATE TABLE {tbl};")
            self._run(cur, "SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _nan_to_none(val):
        try:
            if val is None:
                return None
            if isinstance(val, float) and (math.isnan(val) or val in (float("inf"), float("-inf"))):
                return None
        except Exception:
            pass
        return None if (val is None or (isinstance(val, float) and math.isnan(val))) else val

    def _ensure_pandas(self, df):
        try:
            import pandas as pd  # noqa: F401
        except Exception as e:
            raise RuntimeError("Pandas es requerido para inserción batched a MySQL.") from e
        if hasattr(df, "to_pandas"):
            return df.to_pandas()
        return df

    # Inserts de dimensiones
    def _insert_dim_wavelength(self, df) -> None:
        df = self._ensure_pandas(df)
        if df is None or df.empty:
            return
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            sql = "INSERT INTO dim_wavelength (id_wavelength, Wavelength_nm, Spectral_Band, Sensitive_Aerosol) VALUES (%s, %s, %s, %s)"
            rows = [(int(r["id_wavelength"]), float(r["Wavelength_nm"]),
                     self._nan_to_none(r.get("Spectral_Band")), self._nan_to_none(r.get("Sensitive_Aerosol")))
                    for _, r in df.iterrows()]
            cur.executemany(sql, rows)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _insert_dim_date(self, df) -> None:
        import pandas as pd
        df = self._ensure_pandas(df)
        if df is None or df.empty:
            return

        conn = self._connect_db()
        cur = conn.cursor()
        try:
            sql = (
                "INSERT INTO dim_date (id_date, Date, Year, Month, Day, Day_of_Year) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            )

            def to_date(v):
                if pd.isna(v):
                    return None
                try:
                    return pd.to_datetime(v).date()
                except Exception:
                    return None

            rows = [
                (
                    int(id_date),
                    to_date(date),
                    int(year), int(month), int(day), int(doy),
                )
                for (id_date, date, year, month, day, doy) in
                df[["id_date", "Date", "Year", "Month", "Day", "Day_of_Year"]].itertuples(index=False, name=None)
            ]

            cur.executemany(sql, rows)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _insert_dim_site(self, df) -> None:
        df = self._ensure_pandas(df)
        if df is None or df.empty:
            return
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            sql = ("INSERT INTO dim_site (id_site, AERONET_Site, Latitude, Longitude, Elevation, Country, Continent) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s)")
            rows = [(int(r["id_site"]), self._nan_to_none(r.get("AERONET_Site")),
                     self._nan_to_none(r.get("Latitude")), self._nan_to_none(r.get("Longitude")),
                     self._nan_to_none(r.get("Elevation")), self._nan_to_none(r.get("Country")),
                     self._nan_to_none(r.get("Continent")))
                    for _, r in df.iterrows()]
            cur.executemany(sql, rows)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _insert_dim_weather(self, df_weather: pl.DataFrame) -> None:
        
        if df_weather is None or df_weather.height == 0:
            return
        dfp = df_weather.select([
            "id_weather", "temperature_mean", "radiation_sum", "humidity_mean",
            "wind_speed_max", "wind_direction_dominant", "evapotranspiration", "sunshine_duration_sec"
        ]).to_pandas()
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            sql = ("INSERT INTO dim_weather (id_weather, temperature_mean, radiation_sum, humidity_mean, "
                   "wind_speed_max, wind_direction_dominant, evapotranspiration, sunshine_duration_sec) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
            rows = [(
                int(r["id_weather"]),
                self._nan_to_none(r.get("temperature_mean")),
                self._nan_to_none(r.get("radiation_sum")),
                self._nan_to_none(r.get("humidity_mean")),
                self._nan_to_none(r.get("wind_speed_max")),
                self._nan_to_none(r.get("wind_direction_dominant")),
                self._nan_to_none(r.get("evapotranspiration")),
                self._nan_to_none(r.get("sunshine_duration_sec")),
            ) for _, r in dfp.iterrows()]
            cur.executemany(sql, rows)
            conn.commit()
        finally:
            cur.close()
            conn.close()

    # Fact loaders 
    def _insert_fact_fastload(self, fact_df_pl: pl.DataFrame) -> None:
        temp_csv = os.path.join(self.DATA_DIR, "_fact_aod_tmp.csv")
        fact_df_pl.write_csv(temp_csv, include_header=True, null_value="\\N")

        conn = self._connect_db(for_fastload=True)
        cur = conn.cursor()
        try:
            if self.MYSQL_SESSION_TWEAKS:
                try:
                    self._run(cur, "SET SESSION local_infile = 1;")
                except Exception:
                    pass
            self._bulk_session_on(cur)
            load_sql = f"""
            LOAD DATA LOCAL INFILE '{temp_csv.replace("\\\\", "/")}'
            INTO TABLE fact_aod
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (Fact_ID, id_date, id_wavelength, id_site, id_weather, Particle_type, AOD_Value, Precipitable_Water, Angstrom_Exponent, is_enriched);
            """
            self._run(cur, load_sql)
            self._bulk_session_off(cur)
            conn.commit()
        finally:
            try:
                cur.close()
            finally:
                conn.close()
            try:
                os.remove(temp_csv)
            except Exception:
                pass

    def _insert_fact_batched(self, df_pl: pl.DataFrame) -> None:
        total = df_pl.height
        if total == 0:
            return
        conn = self._connect_db()
        cur = conn.cursor()
        try:
            self._bulk_session_on(cur)
            sql = ("INSERT INTO fact_aod (Fact_ID, id_date, id_wavelength, id_site, id_weather, "
                   "Particle_type, AOD_Value, Precipitable_Water, Angstrom_Exponent, is_enriched) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            for start in range(0, total, self.BATCH_SIZE):
                end = min(start + self.BATCH_SIZE, total)
                chunk_pd = df_pl.slice(start, end - start).to_pandas()
                rows = [
                    (
                        int(r["Fact_ID"]), int(r["id_date"]), int(r["id_wavelength"]), int(r["id_site"]),
                        self._nan_to_none(r.get("id_weather")),
                        self._nan_to_none(r.get("Particle_type")),
                        float(r["AOD_Value"]),
                        self._nan_to_none(r.get("Precipitable_Water")),
                        self._nan_to_none(r.get("Angstrom_Exponent")),
                        int(r.get("is_enriched", 0)),
                    )
                    for _, r in chunk_pd.iterrows()
                ]
                cur.executemany(sql, rows)
            self._bulk_session_off(cur)
            conn.commit()
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    # WEATHER
    def _find_latest_weather_parquet(self) -> Optional[str]:
        pattern = os.path.join(self.STAGING_DIR, self.WEATHER_GLOB)
        files = glob.glob(pattern)
        if not files:
            return None
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[0]

    def _build_dim_weather_from_parquet(self, weather_path: str) -> Tuple[pl.DataFrame, pl.DataFrame]:
    
        dfw = pl.read_parquet(weather_path)

        # Normalizar tipos/columnas
        rename_map = {
            "temperature_2m_mean": "temperature_mean",
            "shortwave_radiation_sum": "radiation_sum",
            "relative_humidity_2m_mean": "humidity_mean",
            "wind_speed_10m_max": "wind_speed_max",
            "wind_direction_10m_dominant": "wind_direction_dominant",
            "et0_fao_evapotranspiration": "evapotranspiration",
            "sunshine_duration": "sunshine_duration_sec",
        }
        for src, dst in rename_map.items():
            if src in dfw.columns:
                dfw = dfw.rename({src: dst})

        # Tipos
        if "Date" in dfw.columns and dfw.schema["Date"] != pl.Date:
            dfw = dfw.with_columns(pl.col("Date").str.strptime(pl.Date, strict=False))
        dfw = dfw.with_columns([
            pl.col("Latitude").cast(pl.Float64, strict=False),
            pl.col("Longitude").cast(pl.Float64, strict=False),
        ])

        # Redondeo suave
        dfw = dfw.with_columns([
            pl.col("Latitude").round(6).alias("Latitude"),
            pl.col("Longitude").round(6).alias("Longitude"),
        ])

        # Unique por (Date, Lat, Lon)
        base_keys = ["Date", "Latitude", "Longitude"]
        dfw_unique = dfw.select(base_keys + list(rename_map.values())).unique()

        # Asignar id_weather
        dfw_unique = dfw_unique.with_row_count(name="id_weather", offset=1)

        dim_weather_df = dfw_unique.select([
            "id_weather", "temperature_mean", "radiation_sum", "humidity_mean",
            "wind_speed_max", "wind_direction_dominant", "evapotranspiration", "sunshine_duration_sec"
        ])
        weather_key_map = dfw_unique.select(base_keys + ["id_weather"])
        return dim_weather_df, weather_key_map

    # Muestra (CSV)
    def _write_sample_geo_csv(
        self,
        fact_df: pl.DataFrame,
        dim_date: pl.DataFrame,
        dim_site: pl.DataFrame,
    ) -> None:
        try:
            n_total = fact_df.height
            if n_total == 0:
                print("[Sample] No hay filas en fact_df; no se genera muestra.")
                return

            n = min(int(self.SAMPLE_TARGET), n_total)
            try:
                sample = fact_df.sample(n=n, with_replacement=False, shuffle=True, seed=self.SAMPLE_SEED)
            except Exception:
                sample = fact_df.head(n)

            map_date = dim_date.select(["id_date", "Date"])
            site_cols = [c for c in ["id_site", "AERONET_Site", "Country", "Continent", "Latitude", "Longitude"] if c in dim_site.columns]
            map_site = dim_site.select(site_cols)

            out = (
                sample
                .join(map_date, on="id_date", how="left")
                .join(map_site, on="id_site", how="left")
                .with_columns([
                    (pl.col("Latitude").round(6).alias("Latitude")) if ("Latitude" in map_site.columns) else pl.lit(None).alias("Latitude"),
                    (pl.col("Longitude").round(6).alias("Longitude")) if ("Longitude" in map_site.columns) else pl.lit(None).alias("Longitude"),
                ])
            )

            preferred_cols = [
                "Fact_ID", "Date",
                "AERONET_Site", "Country", "Continent", "Latitude", "Longitude",
                "id_wavelength", "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
            ]
            cols = [c for c in preferred_cols if c in out.columns]
            out = out.select(cols)

            out_path = os.path.join(self.DATA_DIR, self.SAMPLE_CSV_NAME)
            out.write_csv(out_path)
            print(f"[Sample] CSV de muestra escrito en {out_path} con {n} filas.")
        except Exception as e:
            print(f"[Sample] Advertencia: no se pudo escribir el CSV de muestra: {e}")

    def _write_sample_geo_csv_from_paths(self, paths: Dict[str, str]) -> None:
        try:
            dim_d = pl.read_parquet(paths["date"])
            dim_s = pl.read_parquet(paths["site"])

            dim_w = None
            try:
                dim_w = pl.read_parquet(paths["wavelength"])
            except Exception:
                pass

            parts = sorted(glob.glob(os.path.join(paths["fact_dir"], "fact_aod_part_*.parquet")))
            if not parts:
                print("[Sample] No hay part-files; no se genera muestra.")
                return

            remaining = int(self.SAMPLE_TARGET)
            samples: List[pl.DataFrame] = []

            for p in parts:
                if remaining <= 0:
                    break
                dfp = pl.read_parquet(p)
                if dfp.height == 0:
                    continue
                k = min(remaining, dfp.height)
                try:
                    take = dfp.sample(n=k, with_replacement=False, shuffle=True, seed=self.SAMPLE_SEED)
                except Exception:
                    take = dfp.head(k)
                samples.append(take)
                remaining -= k

            if not samples:
                print("[Sample] No se recolectó muestra de los part-files.")
                return

            sample = pl.concat(samples)

            out = (
                sample
                .join(dim_d.select(["id_date", "Date"]), on="id_date", how="left")
                .join(
                    dim_s.select([c for c in ["id_site", "AERONET_Site", "Country", "Continent", "Latitude", "Longitude"] if c in dim_s.columns]),
                    on="id_site", how="left"
                )
                .with_columns([
                    (pl.col("Latitude").round(6).alias("Latitude")) if ("Latitude" in dim_s.columns) else pl.lit(None).alias("Latitude"),
                    (pl.col("Longitude").round(6).alias("Longitude")) if ("Longitude" in dim_s.columns) else pl.lit(None).alias("Longitude"),
                ])
            )

            if dim_w is not None:
                out = out.join(
                    dim_w.select([c for c in ["id_wavelength", "Wavelength_nm", "Spectral_Band"] if c in dim_w.columns]),
                    on="id_wavelength", how="left"
                )

            preferred_cols = [
                "Fact_ID", "Date",
                "AERONET_Site", "Country", "Continent", "Latitude", "Longitude",
                "id_wavelength", "Wavelength_nm", "Spectral_Band",
                "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent",
            ]
            cols = [c for c in preferred_cols if c in out.columns]
            out = out.select(cols)

            out_path = os.path.join(self.DATA_DIR, self.SAMPLE_CSV_NAME)
            out.write_csv(out_path)
            print(f"[Sample] CSV de muestra escrito en {out_path} con {out.height} filas.")
        except Exception as e:
            print(f"[Sample] Advertencia: no se pudo escribir el CSV de muestra (transform_streaming): {e}")

    # Load
    def load_to_db(
        self,
        fact_df: pl.DataFrame,
        dim_wavelength: pl.DataFrame,
        dim_date: pl.DataFrame,
        dim_site: pl.DataFrame,
    ) -> None:
        # CSV de muestra
        self._write_sample_geo_csv(fact_df, dim_date, dim_site)

        print("[Load] Creando/validando esquema...")
        self._create_schema_objects()
        print("[Load] Limpiando destino...")
        self._truncate_tables()
        print("[Load] Insertando dimensiones básicas...")
        self._insert_dim_wavelength(dim_wavelength)
        self._insert_dim_date(dim_date)
        self._insert_dim_site(dim_site)

        # Intentar integrar Weather si existe el parquet
        weather_path = self._find_latest_weather_parquet()
        dim_weather_df, weather_key_map = (None, None)
        if weather_path:
            try:
                dim_weather_df, weather_key_map = self._build_dim_weather_from_parquet(weather_path)
                self._insert_dim_weather(dim_weather_df)
                print(f"[Load] Dim_weather insertada desde: {weather_path}")
            except Exception as e:
                print(f"[Load] Advertencia: no se pudo procesar Weather ({e}). Se cargará fact sin enriquecer.")

        print("[Load] Preparando hechos...")
        fact_enriched = fact_df

        # Si hay weather, enriquecer con id_weather e is_enriched
        if weather_key_map is not None:
            map_date = dim_date.select(["id_date", "Date"])
            map_site = dim_site.select(["id_site", "Latitude", "Longitude"]).with_columns([
                pl.col("Latitude").round(6).alias("Latitude"),
                pl.col("Longitude").round(6).alias("Longitude"),
            ])

            fact_enriched = (
                fact_df
                .join(map_date, on="id_date", how="left")
                .join(map_site, on="id_site", how="left")
                .join(weather_key_map, on=["Date", "Latitude", "Longitude"], how="left")
                .with_columns([
                    pl.when(pl.col("id_weather").is_null()).then(pl.lit(0)).otherwise(pl.lit(1)).alias("is_enriched")
                ])
                .select([
                    "Fact_ID", "id_date", "id_wavelength", "id_site", "id_weather",
                    "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent", "is_enriched"
                ])
            )
        else:
            fact_enriched = fact_df.with_columns([
                pl.lit(None).alias("id_weather"),
                pl.lit(0).alias("is_enriched"),
            ]).select([
                "Fact_ID", "id_date", "id_wavelength", "id_site", "id_weather",
                "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent", "is_enriched"
            ])

        print("[Load] Insertando hechos...")
        if self.MYSQL_FASTLOAD:
            self._insert_fact_fastload(fact_enriched)
        else:
            self._insert_fact_batched(fact_enriched)
        print("========== Carga completada ==========")

    def load_to_db_from_paths(self, paths: Dict[str, str]) -> None:
        
        # CSV de muestra
        self._write_sample_geo_csv_from_paths(paths)

        print("[Load] Creando/validando esquema...")
        self._create_schema_objects()
        print("[Load] Limpiando destino...")
        self._truncate_tables()

        print("[Load] Leyendo/parsing dimensiones...")
        dim_w = pl.read_parquet(paths["wavelength"])
        dim_d = pl.read_parquet(paths["date"])
        dim_s = pl.read_parquet(paths["site"])

        print("[Load] Insertando dimensiones básicas...")
        self._insert_dim_wavelength(dim_w)
        self._insert_dim_date(dim_d)
        self._insert_dim_site(dim_s)

        weather_path = self._find_latest_weather_parquet()
        dim_weather_df, weather_key_map = (None, None)
        if weather_path:
            try:
                dim_weather_df, weather_key_map = self._build_dim_weather_from_parquet(weather_path)
                self._insert_dim_weather(dim_weather_df)
                print(f"[Load] Dim_weather insertada desde: {weather_path}")
            except Exception as e:
                print(f"[Load] Advertencia: no se pudo procesar Weather ({e}). Se cargará fact sin enriquecer.")
        else:
            print("[Load] Parquet de clima no encontrado. Se cargará fact sin enriquecer.")

        # Mapas mínimos para enriquecer facts
        map_date = dim_d.select(["id_date", "Date"])
        map_site = dim_s.select(["id_site", "Latitude", "Longitude"]).with_columns([
            pl.col("Latitude").round(6).alias("Latitude"),
            pl.col("Longitude").round(6).alias("Longitude"),
        ])

        print("[Load] Insertando hechos (por partes)...")
        part_glob = os.path.join(paths["fact_dir"], "fact_aod_part_*.parquet")
        parts = sorted(glob.glob(part_glob))
        if not parts:
            raise RuntimeError(f"No se encontraron part-files en {paths['fact_dir']}")

        for p in parts:
            fact_part = pl.read_parquet(p)

            if weather_key_map is not None:
                fact_enriched = (
                    fact_part
                    .join(map_date, on="id_date", how="left")
                    .join(map_site, on="id_site", how="left")
                    .join(weather_key_map, on=["Date", "Latitude", "Longitude"], how="left")
                    .with_columns([
                        pl.when(pl.col("id_weather").is_null()).then(pl.lit(0)).otherwise(pl.lit(1)).alias("is_enriched")
                    ])
                    .select([
                        "Fact_ID", "id_date", "id_wavelength", "id_site", "id_weather",
                        "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent", "is_enriched"
                    ])
                )
            else:
                fact_enriched = fact_part.with_columns([
                    pl.lit(None).alias("id_weather"),
                    pl.lit(0).alias("is_enriched"),
                ]).select([
                    "Fact_ID", "id_date", "id_wavelength", "id_site", "id_weather",
                    "Particle_type", "AOD_Value", "Precipitable_Water", "Angstrom_Exponent", "is_enriched"
                ])

            if self.MYSQL_FASTLOAD:
                self._insert_fact_fastload(fact_enriched)
            else:
                self._insert_fact_batched(fact_enriched)

        print("[Load] Carga completada por partes.")

    # DQ sobre datos TRANSFORMADOS
    def dq_validate_transformed_paths(
        self,
        paths: Dict[str, str],
        dq_min_score_pct: float = 90.0,
        weather_parquet: str = ""
    ) -> dict:
    
        import json as _json
        from datetime import datetime as _dt

        #Cargar dimensiones
        dim_w = pl.read_parquet(paths["wavelength"])
        dim_d = pl.read_parquet(paths["date"])
        dim_s = pl.read_parquet(paths["site"])

        w_map = dim_w.select(["id_wavelength", "Wavelength_nm", "Spectral_Band", "Sensitive_Aerosol"])
        d_map = dim_d.select(["id_date", "Date", "Year", "Month", "Day", "Day_of_Year"])
        s_map = dim_s.select(["id_site", "Latitude", "Longitude"]).with_columns([
            pl.col("Latitude").round(6).alias("Latitude"),
            pl.col("Longitude").round(6).alias("Longitude"),
        ])

        # Weather opcional
        has_weather = False
        weather_key_map = None
        if weather_parquet and os.path.exists(weather_parquet):
            try:
                _, weather_key_map = self._build_dim_weather_from_parquet(weather_parquet)
                has_weather = weather_key_map is not None
            except Exception as _e:
                print(f"[DQ] Advertencia: no se pudo procesar weather para validación: {_e}")

        # Acumuladores
        n_parts = 0
        n_fact = 0

        # Completitud
        compl_nulls = {
            "id_date": 0, "id_wavelength": 0, "id_site": 0,
            "AOD_Value": 0, "Precipitable_Water": 0, "Angstrom_Exponent": 0,
        }

        # Unicidad
        fact_id_dups = 0
        combo_dups = 0
        seen_combos = set()
        seen_fact_ids = set()

        # Consistencia
        fk_miss_date = 0
        fk_miss_wav  = 0
        fk_miss_site = 0
        doy_mismatch = 0
        particle_mismatch = 0
        spectral_band_mismatch = 0

        # Cobertura del enriquecimiento
        enrich_fk_miss = 0
        enrich_checked = 0

        # Validez física
        aod_neg = 0
        aod_ext_gt3 = 0
        pw_neg = 0
        ae_out = 0
        lat_bad = 0
        lon_bad = 0

        AE_FINE_TH = self.AE_FINE_TH  # 1.5
        AE_COARSE_TH = self.AE_COARSE_TH  # 1.0

        def _expected_particle(ae):
            if ae is None or (isinstance(ae, float) and math.isnan(ae)):
                return None
            if ae >= AE_FINE_TH:
                return "fine"
            if ae <= AE_COARSE_TH:
                return "coarse"
            return "mixed"

        part_glob = os.path.join(paths["fact_dir"], "fact_aod_part_*.parquet")
        for p in sorted(glob.glob(part_glob)):
            n_parts += 1
            fact = pl.read_parquet(p)
            n = fact.height
            n_fact += n
            if n == 0:
                continue

            # Completitud
            for c in list(compl_nulls.keys()):
                if c in fact.columns:
                    compl_nulls[c] += int(fact.select(pl.col(c).is_null().sum()).item())

            # Unicidad
            if "Fact_ID" in fact.columns:
                fids = fact.select("Fact_ID").to_series().to_list()
                for fid in fids:
                    if fid in seen_fact_ids:
                        fact_id_dups += 1
                    else:
                        seen_fact_ids.add(fid)

            keys_ok = all(c in fact.columns for c in ["id_date", "id_wavelength", "id_site"])
            if keys_ok:
                combos = fact.select(["id_date", "id_wavelength", "id_site"]).to_numpy()
                for (i_d, i_w, i_s) in combos:
                    key = (int(i_d), int(i_w), int(i_s))
                    if key in seen_combos:
                        combo_dups += 1
                    else:
                        seen_combos.add(key)

            # Consistencia: FKs (joins)
            f = (
                fact
                .join(d_map, on="id_date", how="left")
                .join(w_map, on="id_wavelength", how="left")
                .join(s_map, on="id_site", how="left")
            )

            fk_miss_date += int(f.select(pl.col("Date").is_null().sum()).item())
            fk_miss_wav  += int(f.select(pl.col("Wavelength_nm").is_null().sum()).item())
            fk_miss_site += int(f.select(pl.col("Latitude").is_null().sum()).item())

            # DOY vs Date 
            if "Day_of_Year" in f.columns:
                good = f.filter(pl.col("Date").is_not_null())
                if good.height:
                    doy_mismatch += int((good["Day_of_Year"] != good["Date"].dt.ordinal_day()).sum())

            # Particle_type vs Angstrom_Exponent
            if "Particle_type" in f.columns and "Angstrom_Exponent" in f.columns:
                for pt_i, ae_i in zip(f["Particle_type"].to_list(), f["Angstrom_Exponent"].to_list()):
                    if ae_i is None or (isinstance(ae_i, float) and math.isnan(ae_i)):
                        continue
                    exp = _expected_particle(ae_i)
                    if exp is not None and pt_i is not None and pt_i != exp:
                        particle_mismatch += 1

            # Spectral_Band coherencia (opcional)
            if "Wavelength_nm" in f.columns and "Spectral_Band" in f.columns:
                for wv, sband in zip(f["Wavelength_nm"].to_list(), f["Spectral_Band"].to_list()):
                    if wv is None:
                        continue
                    if (wv < 400 and sband != "UV") or (400 <= wv <= 700 and sband != "VIS") or (wv > 700 and sband != "NIR"):
                        spectral_band_mismatch += 1

            # Validez física
            if "AOD_Value" in f.columns:
                s = f["AOD_Value"]
                aod_neg += int((s < 0).sum())
                aod_ext_gt3 += int((s > 3.0).sum())

            if "Precipitable_Water" in f.columns:
                s = f["Precipitable_Water"]
                pw_neg += int((s < 0).sum())

            if "Angstrom_Exponent" in f.columns:
                s = f["Angstrom_Exponent"]
                ae_out += int(((s < -1.0) | (s > 5.0)).sum())

            if "Latitude" in f.columns:
                lat_bad += int((~f["Latitude"].is_between(-90, 90, closed="both")).sum())
            if "Longitude" in f.columns:
                lon_bad += int((~f["Longitude"].is_between(-180, 180, closed="both")).sum())

    
        if has_weather and weather_key_map is not None and fact.height:
            # Trae Date/Lat/Lon desde las dims
            f_keys = (
                fact
                .join(d_map.select(["id_date", "Date"]), on="id_date", how="left")
                .join(s_map.select(["id_site", "Latitude", "Longitude"]), on="id_site", how="left")
                .with_columns([
                    pl.col("Latitude").round(6).alias("Latitude"),
                    pl.col("Longitude").round(6).alias("Longitude"),
                ])
                .select(["Date", "Latitude", "Longitude"])
            )

            # Filtra filas elegibles
            weather_keys = weather_key_map.select(["Date", "Latitude", "Longitude"]).unique()
            eligible = f_keys.join(weather_keys, on=["Date", "Latitude", "Longitude"], how="inner")

            # Si no hay elegibles, no penalizamos
            eligible_n = eligible.height
            if eligible_n > 0:
                
                f_enrich = (
                    eligible
                    .join(weather_key_map, on=["Date", "Latitude", "Longitude"], how="left")
                    .select(["Date", "Latitude", "Longitude", "id_weather"])
                )
                misses = int(f_enrich.select(pl.col("id_weather").is_null().sum()).item())
                enrich_fk_miss += misses
                enrich_checked  += eligible_n


        # KPIs por dimensión 
        compl_pct_by_col = {}
        for c, nulls in compl_nulls.items():
            pct = 100.0 if n_fact == 0 else 100.0 * (1.0 - (nulls / max(1, n_fact)))
            compl_pct_by_col[c] = round(pct, 4)
        compl_kpi = float(sum(compl_pct_by_col.values()) / len(compl_pct_by_col)) if compl_pct_by_col else float("nan")

        parts_cons = []
        if n_fact > 0:
            parts_cons.append(100.0 * (1.0 - fk_miss_date / n_fact))
            parts_cons.append(100.0 * (1.0 - fk_miss_wav  / n_fact))
            parts_cons.append(100.0 * (1.0 - fk_miss_site / n_fact))
            parts_cons.append(100.0 * (1.0 - doy_mismatch / n_fact))
            parts_cons.append(100.0 * (1.0 - particle_mismatch / n_fact))
            parts_cons.append(100.0 * (1.0 - spectral_band_mismatch / n_fact))
            if enrich_checked > 0:
                parts_cons.append(100.0 * (1.0 - enrich_fk_miss / enrich_checked))
        cons_kpi = float(sum(parts_cons) / len(parts_cons)) if parts_cons else float("nan")

        uniq_kpi = 100.0
        issues_uniq = []
        if fact_id_dups > 0:
            issues_uniq.append(f"Duplicados en Fact_ID: {fact_id_dups}")
            uniq_kpi -= 20.0
        if combo_dups > 0:
            issues_uniq.append(f"Duplicados en (id_date,id_wavelength,id_site): {combo_dups}")
            uniq_kpi = max(0.0, uniq_kpi - 20.0)

        parts_val = []
        if n_fact > 0:
            parts_val.append(100.0 * (1.0 - aod_neg / n_fact))
            parts_val.append(100.0 * (1.0 - pw_neg  / n_fact))
            parts_val.append(100.0 * (1.0 - ae_out  / n_fact))
            parts_val.append(100.0 * (1.0 - lat_bad / n_fact))
            parts_val.append(100.0 * (1.0 - lon_bad / n_fact))
        val_kpi = float(sum(parts_val) / len(parts_val)) if parts_val else float("nan")

        dims = {
            "completeness": {"kpi_pct": compl_kpi, "by_col_pct": compl_pct_by_col},
            "consistency":  {
                "kpi_pct": cons_kpi,
                "fk_miss": {"date": fk_miss_date, "wavelength": fk_miss_wav, "site": fk_miss_site},
                "doy_mismatch_n": doy_mismatch,
                "particle_mismatch_n": particle_mismatch,
                "spectral_band_mismatch_n": spectral_band_mismatch,
                "weather_coverage": {
                    "checked_rows": enrich_checked,
                    "missing_matches": enrich_fk_miss,
                    "coverage_pct": (0.0 if enrich_checked==0 else round(100.0*(1.0 - enrich_fk_miss/enrich_checked), 2))
                }
            },
            "uniqueness":   {"kpi_pct": uniq_kpi, "issues": issues_uniq, "fact_id_dups": fact_id_dups, "combo_dups": combo_dups},
            "validity":     {
                "kpi_pct": val_kpi,
                "aod_negative_n": aod_neg,
                "aod_extreme_gt3_n": aod_ext_gt3,
                "pw_negative_n": pw_neg,
                "angstrom_out_of_range_n": ae_out,
                "lat_out_of_range_n": lat_bad,
                "lon_out_of_range_n": lon_bad,
            },
        }
        overall = float(sum([dims["completeness"]["kpi_pct"], dims["consistency"]["kpi_pct"],
                             dims["uniqueness"]["kpi_pct"], dims["validity"]["kpi_pct"]]) / 4.0)

        # Reportes
        reports_dir = os.path.join(self.DATA_DIR, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        ts = _dt.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report_json = os.path.join(reports_dir, f"dq_transformed_report_{ts}.json")
        payload = {
            "overall_quality_kpi_pct": overall,
            "dimensions": dims,
            "row_counts": {"fact": n_fact, "parts": n_parts},
            "generated_at_utc": ts,
            "threshold_pct": float(dq_min_score_pct),
            "artifacts_used": {k: str(v) for k, v in paths.items()},
            "used_weather": bool(has_weather),
        }
        with open(report_json, "w", encoding="utf-8") as f:
            _json.dump(payload, f, ensure_ascii=False, indent=2)

        summary_lines = [
            "=== RESUMEN – Calidad (post-transform + weather) ===",
            f"KPI global: {overall:.2f}%",
            f"Completitud: {dims['completeness']['kpi_pct']:.2f}%",
            f"Consistencia: {dims['consistency']['kpi_pct']:.2f}% "
            f"(FK miss d/w/s: {fk_miss_date}/{fk_miss_wav}/{fk_miss_site}, "
            f"DOY mis: {doy_mismatch}, Particle mis: {particle_mismatch}, "
            f"Spectral mis: {spectral_band_mismatch}, "
            f"Weather cov: {dims['consistency']['weather_coverage']['coverage_pct']}%)",
            f"Unicidad: {dims['uniqueness']['kpi_pct']:.2f}% "
            f"(Fact_ID dups: {fact_id_dups}, Combo dups: {combo_dups})",
            f"Validez: {dims['validity']['kpi_pct']:.2f}% "
            f"(AOD<0: {aod_neg}, AOD>3: {aod_ext_gt3}, PW<0: {pw_neg}, AE fuera: {ae_out}, "
            f"Lat out: {lat_bad}, Lon out: {lon_bad})",
            "==========================================================="
        ]
        summary_txt = "\n".join(summary_lines)
        report_txt = os.path.join(reports_dir, f"dq_transformed_report_{ts}.txt")
        with open(report_txt, "w", encoding="utf-8") as f:
            f.write(summary_txt)

        return {
            "passed": bool(overall >= float(dq_min_score_pct)),
            "score_pct": overall,
            "report_path": report_json,
            "summary_path": report_txt,
            "summary": summary_txt,
            "row_counts": {"fact": n_fact, "parts": n_parts},
            "metrics": payload["dimensions"],
        }

    # RUN 
    def run(self) -> None:
        print("========== INICIO ETL AEROSOL (Polars) ==========")
        df = self.extract()
        fact_df, dim_wavelength, dim_date, dim_site = self.transform(df)
        self.load_to_db(fact_df, dim_wavelength, dim_date, dim_site)
        print("========== ETL COMPLETADO ==========")