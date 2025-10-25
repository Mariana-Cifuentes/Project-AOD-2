# main.py
# Dashboard Streamlit – AOD + clima (lectura centralizada + visuales modulares)

import os
import numpy as np
import pandas as pd
import polars as pl
import streamlit as st

from visuals import (
    fig_geo_layers,
    fig_count_by_continent,
    fig_hist_aod,
    fig_count_particle_type,
    fig_trend_aod_by_day,
    fig_corr_heatmap,
    fig_scatter_with_reg,
    fig_monthly_aod_temp_dual_axis,
    fig_scatter_matrix,
)

st.set_page_config(
    page_title="AOD Dashboard",
    page_icon="🛰️",
    layout="wide"
)


@st.cache_data(show_spinner=True)
def load_csv_polars(path: str) -> pl.DataFrame:
    # Polars es rápido y robusto; infer_schema_length para columnas mixtas
    return pl.read_csv(path, infer_schema_length=5000)


def coerce_numeric(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    existing = [c for c in cols if c in df.columns]
    if not existing:
        return df
    return df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False) for c in existing
    ])


st.sidebar.header("⚙️ Carga de datos")

default_path = "data/aod_1000_enriched_half.csv"
use_default = st.sidebar.toggle("Usar archivo por defecto", value=True)

uploaded = None
if not use_default:
    uploaded = st.sidebar.file_uploader(
        "Sube un CSV con las columnas del demo",
        type=["csv"]
    )

if uploaded is not None:
    df_pl = pl.read_csv(uploaded, infer_schema_length=5000)
else:
    if not os.path.exists(default_path):
        st.error(f"No se encuentra el archivo por defecto: `{default_path}`")
        st.stop()
    df_pl = load_csv_polars(default_path)

# Cast numéricos clave por seguridad
df_pl = coerce_numeric(df_pl, [
    "Longitude", "Latitude", "AOD_Value", "Day_of_Year", "Month",
    "temperature_mean", "radiation_sum", "humidity_mean",
    "wind_speed_max", "evapotranspiration", "sunshine_duration_sec",
    "Angstrom_Exponent"
])

st.sidebar.header("🔎 Filtros")

# Enriquecidos
enriched_col = "is_enriched" if "is_enriched" in df_pl.columns else None
only_enriched = st.sidebar.checkbox(
    "Mostrar solo registros enriquecidos", value=False)

# Continente
continents = sorted(df_pl["Continent"].unique().drop_nulls(
).to_list()) if "Continent" in df_pl.columns else []
sel_continents = st.sidebar.multiselect(
    "Continentes", continents, default=continents)

# Tipo de partícula
ptype_vals = sorted(df_pl["Particle_type"].unique().drop_nulls(
).to_list()) if "Particle_type" in df_pl.columns else []
sel_ptypes = st.sidebar.multiselect(
    "Tipo de partícula", ptype_vals, default=ptype_vals)

# Rango AOD
aod_min = float(df_pl["AOD_Value"].min()
                ) if "AOD_Value" in df_pl.columns else 0.0
aod_max = float(df_pl["AOD_Value"].max()
                ) if "AOD_Value" in df_pl.columns else 1.0
sel_aod = st.sidebar.slider("Rango AOD", min_value=float(aod_min), max_value=float(aod_max),
                            value=(float(aod_min), float(aod_max)))

# Mes
if "Month" in df_pl.columns:
    month_series = df_pl["Month"].drop_nulls()
    if len(month_series) > 0:
        try:
            month_series = month_series.cast(pl.Int64, strict=False)
        except Exception:
            pass
        mmin = int(month_series.min())
        mmax = int(month_series.max())
        mmin = max(1, min(12, mmin))
        mmax = max(1, min(12, mmax))
        if mmin > mmax:
            mmin, mmax = 1, 12
        sel_months = st.sidebar.slider("Mes (1–12)", 1, 12, (mmin, mmax))
    else:
        sel_months = (1, 12)
else:
    sel_months = (1, 12)

df_flt = df_pl

if enriched_col and only_enriched:
    df_flt = df_flt.filter(pl.col(enriched_col) == 1)

if sel_continents and "Continent" in df_flt.columns:
    df_flt = df_flt.filter(pl.col("Continent").is_in(sel_continents))

if sel_ptypes and "Particle_type" in df_flt.columns:
    df_flt = df_flt.filter(pl.col("Particle_type").is_in(sel_ptypes))

if "AOD_Value" in df_flt.columns:
    df_flt = df_flt.filter(pl.col("AOD_Value").is_between(
        sel_aod[0], sel_aod[1], closed="both"))

if "Month" in df_flt.columns:
    df_flt = df_flt.filter(pl.col("Month").is_between(
        sel_months[0], sel_months[1], closed="both"))

# Dataframe auxiliar (enriquecidos si existe bandera)
df_enriched = df_flt.filter(pl.col(enriched_col) ==
                            1) if enriched_col else df_flt

st.sidebar.header("🗺️ Capas del mapa")
layer_options = ["AOD", "Temperatura", "Humedad"]
selected_layers = st.sidebar.multiselect(
    "Selecciona capas a mostrar",
    layer_options,
    default=["AOD"]
)

size_mode = st.sidebar.selectbox(
    "Tamaño del marcador",
    ["Por valor", "Fijo"],
    index=0
)

st.title("🛰️ AOD Dashboard (AERONET + clima)")

st.caption(
    "Todos los gráficos consumen **el mismo DataFrame filtrado** desde esta página. "
    "Puedes cambiar la fuente del CSV y los filtros en la barra lateral."
)

st.markdown("---")

st.subheader("Distribución geográfica (capas promedio)")
st.plotly_chart(
    fig_geo_layers(
        df_enriched,                     # pasamos el df ya filtrado
        layers=selected_layers,
        size_mode=("value" if size_mode == "Por valor" else "fixed")
    ),
    use_container_width=True
)

c1, c2 = st.columns((1, 1), gap="large")

with c1:
    st.subheader("Observaciones por continente")
    st.plotly_chart(fig_count_by_continent(df_flt), use_container_width=True)

with c2:
    st.subheader("Distribución de AOD")
    st.plotly_chart(fig_hist_aod(df_flt), use_container_width=True)


st.subheader("Frecuencia por tipo de partícula")
st.plotly_chart(fig_count_particle_type(df_flt), use_container_width=True)

st.subheader("Tendencia del AOD a lo largo del año")
st.plotly_chart(fig_trend_aod_by_day(df_flt), use_container_width=True)

st.subheader(
    "Correlación entre variables atmosféricas y climáticas (registros enriquecidos)")
st.plotly_chart(fig_corr_heatmap(df_enriched), use_container_width=True)

e1, e2 = st.columns(2, gap="large")

with e1:
    st.subheader("Temperatura vs AOD")
    st.plotly_chart(
        fig_scatter_with_reg(
            df_enriched,
            x="temperature_mean",
            y="AOD_Value",
            color="Particle_type" if "Particle_type" in df_enriched.columns else None,
            title="Relación entre temperatura media y AOD"
        ),
        use_container_width=True
    )

with e2:
    st.subheader("Humedad vs AOD")
    st.plotly_chart(
        fig_scatter_with_reg(
            df_enriched,
            x="humidity_mean",
            y="AOD_Value",
            color=None,
            title="Relación entre humedad media y AOD"
        ),
        use_container_width=True
    )

st.subheader("Tendencia mensual: AOD y Temperatura")
st.plotly_chart(fig_monthly_aod_temp_dual_axis(
    df_enriched), use_container_width=True)

st.subheader("Matriz de dispersión (subset enriquecido)")
st.plotly_chart(
    fig_scatter_matrix(
        df_enriched,
        cols=[
            "AOD_Value",
            "Angstrom_Exponent",
            "temperature_mean",
            "humidity_mean",
            "radiation_sum",
        ],
        labels={
            "AOD_Value": "AOD",
            "Angstrom_Exponent": "Exponente de Ångström (α)",
            "temperature_mean": "Temp. media (°C)",
            "humidity_mean": "Humedad media (%)",
            "radiation_sum": "Radiación acumulada",
        },
        title="Matriz de dispersión (subset enriquecido)",
        tickangle=0,
    ),
    use_container_width=True
)

st.markdown("---")
st.caption("Fuente: AERONET (AOD) + variables meteorológicas enriquecidas.")
