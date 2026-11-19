"""Graphiques Plotly au style sombre de l'application (survol, zoom et double-clic pour revenir)."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.colors import sample_colorscale

TEXT = "#e6edf3"
MUTED = "#8b98a9"
GRID = "rgba(139,152,169,0.15)"
ACCENT = "#38bdf8"
MAP_SCALE = "Turbo"


def _style(fig: go.Figure, height: int, legend: bool = False) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=0, r=0, t=8, b=0), showlegend=legend,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXT, size=12), hoverlabel=dict(bgcolor="#1f2633", font_color=TEXT),
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False, color=MUTED)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, color=MUTED)
    return fig


@lru_cache(maxsize=1)
def regions_geojson() -> dict:
    """Contours simplifiés des 13 régions métropolitaines (source : france-geojson, données IGN/INSEE)."""
    return json.loads((Path(__file__).parent / "assets" / "regions.geojson").read_text(encoding="utf-8"))


def _rings(geom: dict) -> list[list]:
    if geom["type"] == "Polygon":
        return [geom["coordinates"][0]]
    return [poly[0] for poly in geom["coordinates"]]  # MultiPolygon : contour extérieur de chaque île


def france_map(regions: pd.DataFrame, cities: pd.DataFrame | None, height: int = 470,
               value: str = "nb_offres", label: str = "Offres") -> go.Figure:
    """Carte de France dessinée à partir du GeoJSON local (aucun fond de carte téléchargé) :
    régions colorées selon `value`, bulles par ville si les coordonnées sont connues."""
    vals = dict(zip(regions["region"], regions[value].astype(float)))
    known = [v for v in vals.values() if v == v]
    vmin, vmax = (min(known), max(known)) if known else (0.0, 1.0)
    span = (vmax - vmin) or 1.0

    fig = go.Figure()
    for feat in regions_geojson()["features"]:
        name = feat["properties"]["nom"]
        v = vals.get(name)
        color = sample_colorscale(MAP_SCALE, [(v - vmin) / span])[0] if v is not None else "#232a38"
        xs, ys = [], []
        for ring in _rings(feat["geometry"]):
            xs += [p[0] for p in ring] + [None]
            ys += [p[1] for p in ring] + [None]
        fig.add_trace(go.Scatter(
            x=xs, y=ys, fill="toself", mode="lines", line=dict(color="#0e1117", width=1),
            fillcolor=color, hoveron="fills", name=name,
            text=f"<b>{name}</b><br>{label} : {int(v) if v is not None else 0}", hoverinfo="text"))
    # barre de couleurs (trace invisible)
    fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", hoverinfo="skip",
                             marker=dict(colorscale=MAP_SCALE, cmin=vmin, cmax=vmax, color=[vmin], showscale=True,
                                         colorbar=dict(title=dict(text=label, font=dict(color=MUTED)),
                                                       tickfont=dict(color=MUTED), thickness=12, len=0.75))))
    if cities is not None and not cities.empty:
        c = cities.nlargest(60, value)
        fig.add_trace(go.Scatter(
            x=c["lon"], y=c["lat"], mode="markers", text=c["ville"], customdata=c[[value]],
            marker=dict(size=np.clip(np.sqrt(c[value].astype(float)) * 3.2, 5, 42),
                        color="rgba(255,255,255,0.22)", line=dict(color="white", width=1)),
            hovertemplate="<b>%{text}</b><br>" + label + " : %{customdata[0]}<extra></extra>"))
    fig.update_xaxes(visible=False)
    # 1° de latitude est plus long que 1° de longitude en France : on corrige l'échelle verticale
    fig.update_yaxes(visible=False, scaleanchor="x", scaleratio=1 / np.cos(np.radians(46.5)))
    return _style(fig, height)


def donuts(items: list[tuple[float, str, str]], height: int = 190) -> go.Figure:
    """Plusieurs jauges circulaires côte à côte : [(pourcentage, libellé, couleur), ...]."""
    fig = go.Figure()
    n = len(items)
    for i, (pct, label, color) in enumerate(items):
        pct = max(0.0, min(100.0, float(pct)))
        x0, x1 = i / n + 0.04, (i + 1) / n - 0.04
        fig.add_trace(go.Pie(values=[pct, 100 - pct], hole=0.76, sort=False, direction="clockwise",
                             domain=dict(x=[x0, x1], y=[0.18, 1]), textinfo="none", hoverinfo="skip",
                             marker=dict(colors=[color, "rgba(139,152,169,0.18)"])))
        fig.add_annotation(x=(x0 + x1) / 2, y=0.59, xref="paper", yref="paper", showarrow=False,
                           text=f"<b>{pct:.0f} %</b>", font=dict(size=14, color=color))
        fig.add_annotation(x=(x0 + x1) / 2, y=0.02, xref="paper", yref="paper", showarrow=False,
                           text=label, font=dict(size=12, color=MUTED))
    fig = _style(fig, height)
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    return fig


def salary_ranges(df: pd.DataFrame, height: int = 330) -> go.Figure:
    """Écart interquartile (25 %–75 %) et médiane du salaire annuel, par métier."""
    d = df.sort_values("mediane")
    fig = go.Figure()
    fig.add_trace(go.Bar(y=d["metier"], x=d["q3"] - d["q1"], base=d["q1"], orientation="h",
                         marker=dict(color="rgba(56,189,248,0.35)", line=dict(color=ACCENT, width=1)),
                         customdata=d[["q1", "q3", "nb_offres"]],
                         hovertemplate="<b>%{y}</b><br>25 % : %{customdata[0]:,.0f} €<br>"
                                       "75 % : %{customdata[1]:,.0f} €<br>%{customdata[2]} offres<extra></extra>"))
    fig.add_trace(go.Scatter(y=d["metier"], x=d["mediane"], mode="markers",
                             marker=dict(color="#fbbf24", size=11, symbol="diamond"),
                             hovertemplate="Médiane : %{x:,.0f} €<extra></extra>"))
    fig.update_xaxes(ticksuffix=" €", tickformat=",.0f")
    return _style(fig, height)


def hbar(df: pd.DataFrame, x: str, y: str, color: str = ACCENT, height: int = 330, suffix: str = "") -> go.Figure:
    d = df.sort_values(x)
    fig = go.Figure(go.Bar(x=d[x], y=d[y], orientation="h", marker_color=color,
                           text=d[x].map(lambda v: f"{v:g}{suffix}"), textposition="outside",
                           textfont=dict(color=MUTED, size=11),
                           hovertemplate="<b>%{y}</b> : %{x}" + suffix + "<extra></extra>"))
    fig.update_xaxes(showgrid=False, showticklabels=False, range=[0, float(d[x].max()) * 1.22 if len(d) else 1])
    return _style(fig, height)


def area_by_month(df: pd.DataFrame, height: int = 330) -> go.Figure:
    fig = go.Figure(go.Scatter(x=df["mois"], y=df["nb_offres"], mode="lines+markers", fill="tozeroy",
                               line=dict(color=ACCENT, width=2), fillcolor="rgba(56,189,248,0.15)",
                               hovertemplate="%{x} : %{y} offres<extra></extra>"))
    return _style(fig, height)
