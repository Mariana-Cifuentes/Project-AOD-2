from typing import Optional, Dict
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from uuid import uuid4
import os
import logging
import polars as pl
from libs.lib import AerosolETL


# -----------------------------
# Helpers de configuración
# -----------------------------
def get_conf(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is not None:
        return v
    try:
        return Variable.get(name)
    except Exception:
        return default


def make_stage_path(staging_dir: str, prefix: str, ext: str = "parquet") -> str:
    os.makedirs(staging_dir, exist_ok=True)
    return os.path.join(staging_dir, f"{prefix}_{uuid4().hex}.{ext}")


# DAG

@dag(
    dag_id="etl_aeronet_aod_polars",
    start_date=datetime(2025, 10, 22),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["aeronet", "aod", "aerosol", "etl", "polars", "mysql"]
)
def etl_aeronet_aod():

    DATA_DIR = get_conf("DATA_DIR", "/opt/airflow/data")
    CSV_FILENAME = get_conf("CSV_FILENAME", "All_Sites_Times_Daily_Averages_AOD20.csv")
    CSV_PATH = os.path.join(DATA_DIR, CSV_FILENAME)
    STAGING_DIR = get_conf("STAGING_DIR", "/opt/airflow/data/staging")
    SAMPLE_CSV_NAME = get_conf("SAMPLE_CSV_NAME", "aod_sample_500.csv")
    SAMPLE_CSV_PATH = os.path.join(DATA_DIR, SAMPLE_CSV_NAME)
    DQ_MIN_SCORE_PCT = float(get_conf("DQ_MIN_SCORE_PCT", "90.0"))

   
    #EXTRACT
    @task(
        task_id="extract",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20)
    )
    def t_extract(csv_path: str, staging_dir: str) -> str:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"[Extract] No existe el archivo: {csv_path}")
        try:
            # Validación rápida de apertura
            pl.read_csv(csv_path, n_rows=5)
        except Exception as e:
            raise AirflowFailException(f"[Extract] No se pudo abrir {csv_path}: {e}")

        etl = AerosolETL()
        df_pl = etl.extract()

        out = make_stage_path(staging_dir, "aeronet_raw")
        df_pl.write_parquet(out) 
        logging.info(f"[Extract] OK. Guardado parquet en: {out}")
        return out

    
    # TRANSFORM (streaming)
    @task(
        task_id="transform",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=60)
    )
    def t_transform(raw_parquet_path: str, staging_dir: str) -> Dict[str, str]:
        if not os.path.exists(raw_parquet_path):
            raise AirflowFailException(f"[Transform] No existe {raw_parquet_path}")

        etl = AerosolETL()
        # Procesa por lotes de columnas, la librería genera dims + fact parts
        paths = etl.transform_streaming(
            raw_parquet_path=raw_parquet_path,
            out_dir=staging_dir,
            aod_chunk_size=6 
        )
        logging.info(f"[Transform] Dimensiones y fact por partes listos en: {paths}")
        return paths

    # ENRICH WEATHER (Open-Meteo)
    
    @task(
        task_id="weather_is_enriched",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=60)
    )
    def t_enrich_weather(sample_csv_path: str, staging_dir: str) -> str:
        import pandas as pd
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
        import time

        if not os.path.exists(sample_csv_path):
            raise AirflowFailException(f"[EnrichWeather] No existe el CSV de muestras: {sample_csv_path}")

        # Carga de la muestra (espera columnas: Date, Latitude, Longitude)
        muestra = pd.read_csv(sample_csv_path)
        expected_cols = {"Date", "Latitude", "Longitude"}
        if not expected_cols.issubset(set(muestra.columns)):
            raise AirflowFailException(
                f"[EnrichWeather] El CSV no contiene las columnas requeridas {expected_cols}. Columnas: {list(muestra.columns)}"
            )

        cache_dir = get_conf("CACHE_DIR", os.path.join(os.path.expanduser("~"), ".cache", "openmeteo"))
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, "http_cache")
        cache_session = requests_cache.CachedSession(cache_path, expire_after=-1)  # cache persistente
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        variables = [
            "temperature_2m_mean",
            "sunshine_duration",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration",
            "relative_humidity_2m_mean"
        ]

        url = "https://archive-api.open-meteo.com/v1/archive"

        resultados = []
        count = 1

        for idx, fila in muestra.iterrows():
            try:
                lat = float(fila["Latitude"])
                lon = float(fila["Longitude"])
                # Normalizar a YYYY-MM-DD (diario)
                fecha = pd.to_datetime(fila["Date"]).strftime("%Y-%m-%d")

                params = {
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": fecha,
                    "end_date": fecha,
                    "daily": variables
                }

                responses = openmeteo.weather_api(url, params=params)
                if not responses:
                    raise RuntimeError("Respuesta vacía de Open-Meteo")

                response = responses[0]
                daily = response.Daily()

                data = {
                    "Date": fecha,
                    "Latitude": lat,
                    "Longitude": lon,
                    "temperature_2m_mean": float(daily.Variables(0).ValuesAsNumpy()[0]),
                    "sunshine_duration": float(daily.Variables(1).ValuesAsNumpy()[0]),
                    "wind_speed_10m_max": float(daily.Variables(2).ValuesAsNumpy()[0]),
                    "wind_direction_10m_dominant": float(daily.Variables(3).ValuesAsNumpy()[0]),
                    "shortwave_radiation_sum": float(daily.Variables(4).ValuesAsNumpy()[0]),
                    "et0_fao_evapotranspiration": float(daily.Variables(5).ValuesAsNumpy()[0]),
                    "relative_humidity_2m_mean": float(daily.Variables(6).ValuesAsNumpy()[0]),
                }

                resultados.append(data)
                print(f"[EnrichWeather] Petición {count} completada ({lat}, {lon}, {fecha})")
                count += 1

                # Pausa para requests
                time.sleep(1)

            except Exception as e:
                print(f"[EnrichWeather] Error fila {idx} ({fila.get('Latitude')}, {fila.get('Longitude')}, {fila.get('Date')}): {e}")

        if len(resultados) == 0:
            raise AirflowFailException("[EnrichWeather] No se obtuvieron resultados de Open-Meteo.")

        df_final = pd.DataFrame(resultados)
        out_path = make_stage_path(staging_dir, "weather_daily", ext="parquet")

        # Guardar como Parquet con polars
        df_pl = pl.from_pandas(df_final)
        df_pl.write_parquet(out_path)

        logging.info(f"[EnrichWeather] OK. Guardado Parquet de enriquecimiento en: {out_path}")
        return out_path

    # 4) DATA QUALITY (post-transform + weather) CON GATE INTERNO
    @task(
        task_id="dq_quality",
        retries=0,
        execution_timeout=timedelta(minutes=25)
    )
    def t_dq_quality(paths: Dict[str, str], weather_parquet: str, dq_threshold_pct: float = 90.0) -> dict:

        etl = AerosolETL()
        res = etl.dq_validate_transformed_paths(
            paths=paths,
            dq_min_score_pct=dq_threshold_pct,   # el método calcula KPI y devuelve 'passed'
            weather_parquet=weather_parquet
        )
        logging.info("[DQ] score=%.2f%% passed=%s report=%s rows=%s",
                     res["score_pct"], res["passed"], res["report_path"], res["row_counts"])
        logging.info("\n" + res["summary"])

        # Gate interno
        if not res.get("passed", False):
            raise AirflowFailException(
                f"[DQ Gate] Quality score {res['score_pct']:.2f}% "
                f"< threshold {dq_threshold_pct:.2f}% → abortando carga."
            )
        
        res["paths"] = paths
        return res

    # LOAD 
    @task(
        task_id="load",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=60)
    )
    def t_load(paths: Dict[str, str]) -> str:
        required = ["wavelength", "date", "site", "fact_dir"]
        for k in required:
            v = paths.get(k)
            if not v or not os.path.exists(v):
                if k == "fact_dir":
                    if not v or not os.path.isdir(v):
                        raise AirflowFailException(f"[Load] Falta o no existe {k}: {v}")
                else:
                    raise AirflowFailException(f"[Load] Falta o no existe {k}: {v}")

        etl = AerosolETL()
        etl.load_to_db_from_paths(paths)
        logging.info("[Load] Carga completada en MySQL (por partes).")
        return "OK"

    # Orquestación
    raw_path   = t_extract(CSV_PATH, STAGING_DIR)
    t_paths    = t_transform(raw_path, STAGING_DIR)
    weather_p  = t_enrich_weather(SAMPLE_CSV_PATH, STAGING_DIR)

    # DQ, si pasa, continúa a load
    dq_res     = t_dq_quality(paths=t_paths, weather_parquet=weather_p, dq_threshold_pct=DQ_MIN_SCORE_PCT)
    load_task  = t_load(dq_res["paths"])
    t_paths >> weather_p >> dq_res >> load_task


dag = etl_aeronet_aod()