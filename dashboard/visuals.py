from typing import List, Optional, Tuple
import numpy as np
import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _to_pd(df: pl.DataFrame) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df
    return df.to_pandas(use_pyarrow_extension_array=True)


def _scale_sizes(values: np.ndarray, min_size: float = 6.0, max_size: float = 22.0) -> np.ndarray:
    """Escala tamaños de marcadores entre min_size y max_size."""
    if values.size == 0 or np.all(~np.isfinite(values)):
        return np.full_like(values, (min_size + max_size) / 2.0, dtype=float)
    v = values.astype(float)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return np.full_like(values, (min_size + max_size) / 2.0, dtype=float)
    vmin, vmax = float(np.min(v)), float(np.max(v))
    if np.isclose(vmin, vmax):
        return np.full(values.shape, (min_size + max_size) / 2.0, dtype=float)
    return (values - vmin) / (vmax - vmin) * (max_size - min_size) + min_size


def _site_grouping_columns(df_pl: pl.DataFrame) -> List[str]:
    """Determina columnas de agrupación para promediar por sitio."""
    cols = []
    if "Site" in df_pl.columns:
        cols.append("Site")
    elif "Site_Name" in df_pl.columns:
        cols.append("Site_Name")
    # Siempre incluir coordenadas
    if "Latitude" in df_pl.columns:
        cols.append("Latitude")
    if "Longitude" in df_pl.columns:
        cols.append("Longitude")
    # Útil para filtrar/hover
    if "Continent" in df_pl.columns:
        cols.append("Continent")
    return cols


def fig_geo_layers(
    df_pl: pl.DataFrame,
    layers: List[str],
    size_mode: str = "value"  # "value" o "fixed"
) -> go.Figure:
    """
    Mapa con capas superpuestas para AOD, Temperatura y/o Humedad (promedios por sitio).
    layers: lista con valores en {"AOD", "Temperatura", "Humedad"}.
    size_mode: "value" escala tamaño por valor; "fixed" usa tamaño fijo.
    """
    # Validar columnas necesarias
    have_latlon = all(c in df_pl.columns for c in ["Latitude", "Longitude"])
    if not have_latlon:
        return go.Figure()

    # Determinar agrupación
    group_cols = _site_grouping_columns(df_pl)
    if not group_cols:
        group_cols = ["Latitude", "Longitude"]  # fallback

    # Agregaciones solicitadas
    agg_exprs = []
    have_aod = "AOD_Value" in df_pl.columns
    have_temp = "temperature_mean" in df_pl.columns
    have_hum = "humidity_mean" in df_pl.columns

    if "AOD" in layers and have_aod:
        agg_exprs.append(pl.col("AOD_Value").mean().alias("AOD_avg"))
    if "Temperatura" in layers and have_temp:
        agg_exprs.append(pl.col("temperature_mean").mean().alias("Temp_avg"))
    if "Humedad" in layers and have_hum:
        agg_exprs.append(pl.col("humidity_mean").mean().alias("Hum_avg"))

    if not agg_exprs:
        # Nada que mostrar
        return go.Figure()

    g = df_pl.group_by(group_cols).agg(agg_exprs)
    pdf = _to_pd(g).dropna(
        subset=[c for c in ["Latitude", "Longitude"] if c in g.columns])

    # Columnas de apoyo
    site_col = "Site" if "Site" in pdf.columns else (
        "Site_Name" if "Site_Name" in pdf.columns else None)
    cont_col = "Continent" if "Continent" in pdf.columns else None

    fig = go.Figure()

    # Capa: AOD
    if "AOD" in layers and "AOD_avg" in pdf.columns:
        vals = pdf["AOD_avg"].to_numpy(dtype=float)
        sizes = _scale_sizes(
            vals) if size_mode == "value" else np.full_like(vals, 10.0)
        fig.add_trace(go.Scattergeo(
            lat=pdf["Latitude"],
            lon=pdf["Longitude"],
            mode="markers",
            name="AOD promedio",
            marker=dict(
                size=sizes,
                color=vals,
                colorscale="YlOrRd",
                cmin=float(np.nanmin(vals)) if np.isfinite(
                    vals).any() else None,
                cmax=float(np.nanmax(vals)) if np.isfinite(
                    vals).any() else None,
                colorbar=dict(title="AOD"),
                showscale=True,
                line=dict(width=0.5, color="rgba(0,0,0,0.5)")
            ),
            hovertemplate=(
                ("%{customdata[0]}<br>" if site_col else "") +
                ("Continente: %{customdata[1]}<br>" if cont_col else "") +
                "Lat: %{lat:.3f}, Lon: %{lon:.3f}<br>" +
                "AOD prom: %{marker.color:.3f}<extra>AOD</extra>"
            ),
            customdata=np.stack([
                pdf[site_col] if site_col else np.array([""]*len(pdf)),
                pdf[cont_col] if cont_col else np.array([""]*len(pdf)),
            ], axis=1) if (site_col or cont_col) else None
        ))

    # Capa: Temperatura
    if "Temperatura" in layers and "Temp_avg" in pdf.columns:
        vals = pdf["Temp_avg"].to_numpy(dtype=float)
        sizes = _scale_sizes(
            vals) if size_mode == "value" else np.full_like(vals, 10.0)
        fig.add_trace(go.Scattergeo(
            lat=pdf["Latitude"],
            lon=pdf["Longitude"],
            mode="markers",
            name="Temp. media (°C)",
            marker=dict(
                symbol="square",
                size=sizes,
                color=vals,
                colorscale="Blues",
                cmin=float(np.nanmin(vals)) if np.isfinite(
                    vals).any() else None,
                cmax=float(np.nanmax(vals)) if np.isfinite(
                    vals).any() else None,
                colorbar=dict(title="°C"),
                showscale=True,
                line=dict(width=0.5, color="rgba(0,0,0,0.5)")
            ),
            hovertemplate=(
                ("%{customdata[0]}<br>" if site_col else "") +
                ("Continente: %{customdata[1]}<br>" if cont_col else "") +
                "Lat: %{lat:.3f}, Lon: %{lon:.3f}<br>" +
                "Temp. media: %{marker.color:.2f} °C<extra>Temperatura</extra>"
            ),
            customdata=np.stack([
                pdf[site_col] if site_col else np.array([""]*len(pdf)),
                pdf[cont_col] if cont_col else np.array([""]*len(pdf)),
            ], axis=1) if (site_col or cont_col) else None
        ))

    # Capa: Humedad
    if "Humedad" in layers and "Hum_avg" in pdf.columns:
        vals = pdf["Hum_avg"].to_numpy(dtype=float)
        sizes = _scale_sizes(
            vals) if size_mode == "value" else np.full_like(vals, 10.0)
        fig.add_trace(go.Scattergeo(
            lat=pdf["Latitude"],
            lon=pdf["Longitude"],
            mode="markers",
            name="Humedad media (%)",
            marker=dict(
                symbol="diamond",
                size=sizes,
                color=vals,
                colorscale="Viridis",
                cmin=float(np.nanmin(vals)) if np.isfinite(
                    vals).any() else None,
                cmax=float(np.nanmax(vals)) if np.isfinite(
                    vals).any() else None,
                colorbar=dict(title="%"),
                showscale=True,
                line=dict(width=0.5, color="rgba(0,0,0,0.5)")
            ),
            hovertemplate=(
                ("%{customdata[0]}<br>" if site_col else "") +
                ("Continente: %{customdata[1]}<br>" if cont_col else "") +
                "Lat: %{lat:.3f}, Lon: %{lon:.3f}<br>" +
                "Humedad media: %{marker.color:.1f}%<extra>Humedad</extra>"
            ),
            customdata=np.stack([
                pdf[site_col] if site_col else np.array([""]*len(pdf)),
                pdf[cont_col] if cont_col else np.array([""]*len(pdf)),
            ], axis=1) if (site_col or cont_col) else None
        ))

    fig.update_layout(
        geo=dict(
            projection_type="natural earth",
            showland=True,
            landcolor="rgb(240, 240, 240)",
            showcountries=True,
        ),
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(title="Capas", orientation="h",
                    yanchor="bottom", y=0.99, xanchor="left", x=0.01)
    )
    return fig


def fig_count_by_continent(df_pl: pl.DataFrame) -> go.Figure:
    if "Continent" not in df_pl.columns:
        return go.Figure()
    pdf = _to_pd(
        df_pl.group_by("Continent").agg(pl.len().alias(
            "count")).sort("count", descending=True)
    )
    fig = px.bar(pdf, x="Continent", y="count", text="count")
    fig.update_traces(textposition="outside")
    fig.update_layout(yaxis_title="Número de observaciones",
                      xaxis_title="Continente")
    return fig


def fig_hist_aod(df_pl: pl.DataFrame) -> go.Figure:
    if "AOD_Value" not in df_pl.columns:
        return go.Figure()
    pdf = _to_pd(df_pl.select("AOD_Value").drop_nulls())
    fig = px.histogram(pdf, x="AOD_Value", nbins=30, marginal="rug")
    fig.update_layout(xaxis_title="AOD (Aerosol Optical Depth)",
                      yaxis_title="Frecuencia")
    return fig


def fig_count_particle_type(df_pl: pl.DataFrame) -> go.Figure:
    if "Particle_type" not in df_pl.columns:
        return go.Figure()
    pdf = _to_pd(
        df_pl.group_by("Particle_type").agg(
            pl.len().alias("count")).sort("count", descending=True)
    )
    fig = px.bar(pdf, x="Particle_type", y="count", text="count")
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_title="Tipo de partícula",
                      yaxis_title="Frecuencia")
    return fig


def fig_trend_aod_by_day(df_pl: pl.DataFrame) -> go.Figure:
    need = ["Day_of_Year", "AOD_Value"]
    if not set(need).issubset(df_pl.columns):
        return go.Figure()
    pdf = _to_pd(
        df_pl.group_by("Day_of_Year")
             .agg(pl.col("AOD_Value").mean().alias("AOD_Value"))
             .sort("Day_of_Year")
    )
    fig = px.line(pdf, x="Day_of_Year", y="AOD_Value")
    fig.update_layout(xaxis_title="Día del año", yaxis_title="AOD promedio")
    return fig


def fig_corr_heatmap(df_pl: pl.DataFrame) -> go.Figure:
    cols = [
        "AOD_Value", "temperature_mean", "radiation_sum", "humidity_mean",
        "wind_speed_max", "evapotranspiration", "sunshine_duration_sec"
    ]
    keep = [c for c in cols if c in df_pl.columns]
    if len(keep) < 2:
        return go.Figure()
    pdf = _to_pd(df_pl.select(keep)).dropna()
    if pdf.empty:
        return go.Figure()
    corr = pdf.corr(numeric_only=True)
    fig = go.Figure(
        data=go.Heatmap(
            z=corr.values,
            x=corr.columns,
            y=corr.columns,
            zmin=-1, zmax=1,
            colorscale="RdBu",
            colorbar=dict(title="ρ")
        )
    )
    fig.update_layout(xaxis_title="", yaxis_title="", width=None, height=500)
    return fig


def _linear_fit(x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
    mask = ~np.isnan(x) & ~np.isnan(y)
    if mask.sum() < 2:
        return (np.nan, np.nan)
    m, b = np.polyfit(x[mask], y[mask], 1)
    return (m, b)


def fig_scatter_with_reg(
    df_pl: pl.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    title: Optional[str] = None
) -> go.Figure:
    need = [x, y] + ([color] if color else [])
    if not set(need).issubset(df_pl.columns):
        return go.Figure()
    pdf = _to_pd(df_pl.select(need)).dropna()
    if pdf.empty:
        return go.Figure()

    fig = px.scatter(pdf, x=x, y=y, color=color)
    xvals = pdf[x].astype(float).to_numpy()
    yvals = pdf[y].astype(float).to_numpy()
    m, b = _linear_fit(xvals, yvals)
    if np.isfinite(m) and np.isfinite(b):
        xs = np.linspace(np.nanmin(xvals), np.nanmax(xvals), 100)
        ys = m * xs + b
        fig.add_traces(go.Scatter(
            x=xs, y=ys, mode="lines", name="Regresión (OLS)"))
    fig.update_layout(title=title or "", xaxis_title=x, yaxis_title=y)
    return fig


def fig_monthly_aod_temp_dual_axis(df_pl: pl.DataFrame) -> go.Figure:
    need = ["Month", "AOD_Value", "temperature_mean"]
    if not set(need).issubset(df_pl.columns):
        return go.Figure()

    g = (
        df_pl.group_by("Month")
             .agg([
                 pl.col("AOD_Value").mean().alias("AOD_mean"),
                 pl.col("temperature_mean").mean().alias("temp_mean"),
             ])
        .sort("Month")
    )
    pdf = _to_pd(g)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=pdf["Month"], y=pdf["AOD_mean"],
                   mode="lines+markers", name="AOD"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=pdf["Month"], y=pdf["temp_mean"],
                   mode="lines+markers", name="Temperatura"),
        secondary_y=True
    )
    fig.update_xaxes(title_text="Mes")
    fig.update_yaxes(title_text="AOD promedio", secondary_y=False)
    fig.update_yaxes(title_text="Temperatura media (°C)", secondary_y=True)
    return fig


def fig_scatter_matrix(
    df_pl: pl.DataFrame,
    cols: List[str],
    labels: Optional[dict] = None,
    title: Optional[str] = None,
    tickangle: int = 0,
) -> go.Figure:
    keep = [c for c in cols if c in df_pl.columns]
    if len(keep) < 2:
        return go.Figure()

    pdf = _to_pd(df_pl.select(keep)).dropna()
    if pdf.empty:
        return go.Figure()

    default_labels = {
        "AOD_Value": "AOD",
        "Angstrom_Exponent": "Exponente de Ångström (α)",
        "temperature_mean": "Temp. media (°C)",
        "humidity_mean": "Humedad media (%)",
        "radiation_sum": "Radiación acumulada",
    }
    lab = {c: (labels.get(c, default_labels.get(c, c))
               if labels else default_labels.get(c, c)) for c in keep}

    fig = px.scatter_matrix(
        pdf,
        dimensions=keep,
        labels=lab,
        title=title or None,
    )

    fig.update_traces(
        diagonal_visible=False,
        showupperhalf=False,
        marker=dict(size=4, opacity=0.6)
    )
    fig.update_layout(
        dragmode="select",
        hovermode="closest",
        margin=dict(l=70, r=40, t=50, b=50),
        font=dict(size=12),
        height=700,
    )
    fig.update_xaxes(automargin=True, tickangle=tickangle, showgrid=True)
    fig.update_yaxes(automargin=True, showgrid=True)
    return fig
