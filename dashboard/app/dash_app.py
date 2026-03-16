"""
Dashboard Dash simple pour OBRail - Comparaison trains de jour et de nuit.

CHOIX DE VISUALISATION (justification) :
-----------------------------------------
1. Camembert jour/nuit global
   → Le type de graphique le plus intuitif pour montrer une repartition
     en deux parts. Tout le monde comprend "la part bleue c'est la nuit".

2. Barres groupees par pays
   → Comparaison directe du nombre de trains jour vs nuit dans chaque pays.
     Les barres cote a cote sont le moyen le plus naturel de comparer
     deux valeurs pour une meme categorie.

3. Barres horizontales top operateurs
   → Classement simple du plus grand au plus petit. L'horizontal
     permet de lire les noms d'operateurs sans rotation de texte.

4. Barres horizontales top liaisons
   → Meme logique : classement des liaisons les plus desservies,
     empilees jour+nuit pour voir la composition de chaque liaison.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

sys.path.insert(0, str(Path(__file__).parent))

from analytics.analytics_loader import load_analytics_data
from analytics.network_analysis import get_routes_df

# Couleurs
JOUR = "#E6A700"
NUIT = "#183A5A"
FOND = "#F5F1E8"
CARTE = "#FFFDF8"
TEXTE = "#16212B"
GRIS = "#5E6B74"
BORD = "#D8D2C6"


def _fig_vide(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font={"size": 16, "color": GRIS})
    fig.update_layout(template="plotly_white", paper_bgcolor=CARTE, plot_bgcolor=CARTE,
                      xaxis={"visible": False}, yaxis={"visible": False},
                      margin={"l": 20, "r": 20, "t": 50, "b": 20})
    return fig


def _style_fig(fig: go.Figure) -> go.Figure:
    fig.update_layout(template="plotly_white", paper_bgcolor=CARTE, plot_bgcolor=CARTE,
                      margin={"l": 20, "r": 20, "t": 60, "b": 20},
                      legend={"orientation": "h", "y": 1.12, "x": 1, "xanchor": "right"})
    return fig


def _train_key(df: pd.DataFrame) -> pd.Series:
    if "train_id" in df.columns:
        return df["train_id"].astype(str)
    if "train_number" in df.columns:
        return df["train_number"].astype(str)
    return pd.Series(range(len(df)), index=df.index).astype(str)


# -- Donnees ------------------------------------------------------------------

def charger_donnees() -> pd.DataFrame:
    return load_analytics_data()


def filtrer(df: pd.DataFrame, pays: str) -> pd.DataFrame:
    if df.empty or pays == "all":
        return df
    if "operator_country" in df.columns:
        return df[df["operator_country"] == pays]
    return df


def options_pays(df: pd.DataFrame) -> list[dict]:
    opts = [{"label": "Tous les pays", "value": "all"}]
    if "operator_country" in df.columns:
        for p in sorted(df["operator_country"].dropna().unique()):
            opts.append({"label": p, "value": p})
    return opts


def compter_jour_nuit(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty or "train_type_normalized" not in df.columns:
        return 0, 0
    g = df.assign(k=_train_key(df)).groupby("train_type_normalized")["k"].nunique()
    return int(g.get("day", 0)), int(g.get("night", 0))


# -- Graphiques ---------------------------------------------------------------

def fig_camembert(jour: int, nuit: int) -> go.Figure:
    """Repartition globale jour / nuit."""
    if jour + nuit == 0:
        return _fig_vide("Aucune donnee")

    fig = px.pie(
        names=["Jour", "Nuit"], values=[jour, nuit],
        color_discrete_sequence=[JOUR, NUIT],
        title="Repartition jour / nuit",
    )
    fig.update_traces(textinfo="percent+label", textfont_size=14,
                      hovertemplate="%{label} : %{value:,} trains (%{percent})<extra></extra>")
    fig.update_layout(paper_bgcolor=CARTE, plot_bgcolor=CARTE,
                      margin={"l": 20, "r": 20, "t": 60, "b": 20}, showlegend=False)
    return fig


def fig_par_pays(df: pd.DataFrame) -> go.Figure:
    """Nombre de trains jour vs nuit par pays."""
    if df.empty or "operator_country" not in df.columns:
        return _fig_vide("Aucune donnee par pays")

    g = (df.assign(k=_train_key(df))
         .dropna(subset=["operator_country", "train_type_normalized"])
         .groupby(["operator_country", "train_type_normalized"])["k"]
         .nunique().reset_index(name="trains")
         .rename(columns={"operator_country": "Pays", "train_type_normalized": "type"}))

    total = g.groupby("Pays")["trains"].sum().sort_values(ascending=False)
    top = total.head(10).index.tolist()
    g = g[g["Pays"].isin(top)]
    g["Type"] = g["type"].map({"day": "Jour", "night": "Nuit"})

    fig = px.bar(g, x="Pays", y="trains", color="Type", barmode="group",
                 category_orders={"Pays": top},
                 color_discrete_map={"Jour": JOUR, "Nuit": NUIT},
                 title="Nombre de trains par pays",
                 labels={"trains": "Trains", "Pays": ""})
    return _style_fig(fig)


def fig_top_operateurs(df: pd.DataFrame) -> go.Figure:
    """Top 10 operateurs par nombre de trains."""
    if df.empty or "operator" not in df.columns:
        return _fig_vide("Aucune donnee operateur")

    g = (df.assign(k=_train_key(df))
         .dropna(subset=["operator", "train_type_normalized"])
         .groupby(["operator", "train_type_normalized"])["k"]
         .nunique().reset_index(name="trains")
         .rename(columns={"train_type_normalized": "type"}))

    total = g.groupby("operator")["trains"].sum().sort_values(ascending=False)
    top = total.head(10).index.tolist()
    g = g[g["operator"].isin(top)]
    g["Type"] = g["type"].map({"day": "Jour", "night": "Nuit"})

    ordre = list(reversed(top))
    fig = px.bar(g, y="operator", x="trains", color="Type", barmode="group",
                 orientation="h", category_orders={"operator": ordre},
                 color_discrete_map={"Jour": JOUR, "Nuit": NUIT},
                 title="Top 10 operateurs",
                 labels={"trains": "Trains", "operator": ""})
    return _style_fig(fig)


def fig_top_liaisons(df: pd.DataFrame) -> go.Figure:
    """Top 10 liaisons les plus desservies, empilees jour + nuit."""
    routes = get_routes_df(df)
    if routes.empty or "train_type_normalized" not in routes.columns:
        return _fig_vide("Aucune liaison disponible")

    g = (routes.assign(k=_train_key(routes))
         .groupby(["route", "train_type_normalized"])["k"]
         .nunique().reset_index(name="trains"))

    pivot = g.pivot_table(index="route", columns="train_type_normalized",
                          values="trains", fill_value=0).reset_index()
    pivot.columns.name = None
    pivot["day"] = pivot.get("day", 0)
    pivot["night"] = pivot.get("night", 0)
    pivot["total"] = pivot["day"] + pivot["night"]
    pivot = pivot.sort_values("total", ascending=False).head(10)
    pivot = pivot.sort_values("total", ascending=True)

    fig = go.Figure()
    fig.add_bar(x=pivot["day"], y=pivot["route"], orientation="h",
                name="Jour", marker_color=JOUR)
    fig.add_bar(x=pivot["night"], y=pivot["route"], orientation="h",
                name="Nuit", marker_color=NUIT)
    fig.update_layout(barmode="stack", title="Top 10 liaisons les plus desservies",
                      xaxis_title="Trains", yaxis_title="")
    return _style_fig(fig)


# -- KPIs ---------------------------------------------------------------------

def kpi_card(titre: str, valeur: str, couleur: str) -> html.Div:
    return html.Div([
        html.Div(titre, style={"fontSize": "0.95rem", "color": GRIS}),
        html.Div(valeur, style={"fontSize": "2.2rem", "fontWeight": "700", "color": TEXTE}),
    ], style={"background": couleur, "border": f"1px solid {BORD}",
              "borderRadius": "16px", "padding": "18px 20px",
              "boxShadow": "0 8px 20px rgba(22,33,43,0.05)"})


# -- App ----------------------------------------------------------------------

df_init = charger_donnees()
app = Dash(__name__)
server = app.server
app.title = "OBRail - Jour vs Nuit"

app.layout = html.Div([

    # Titre
    html.H1("Trains de jour vs trains de nuit en Europe",
            style={"color": TEXTE, "marginBottom": "4px"}),
    html.P("Combien de trains circulent le jour et la nuit ? Dans quels pays ? Avec quels operateurs ?",
           style={"color": GRIS, "marginBottom": "20px", "fontSize": "1.05rem"}),

    # Filtre pays
    html.Div([
        html.Label("Filtrer par pays :", style={"fontWeight": "600", "color": TEXTE, "marginRight": "10px"}),
        dcc.Dropdown(id="pays-filtre", options=options_pays(df_init), value="all",
                     clearable=False, style={"width": "250px"}),
    ], style={"display": "flex", "alignItems": "center", "marginBottom": "20px"}),

    # KPIs
    html.Div(id="kpis", style={"display": "grid",
                                "gridTemplateColumns": "repeat(3, 1fr)",
                                "gap": "16px", "marginBottom": "24px"}),

    # Ligne 1 : camembert + barres par pays
    html.Div([
        html.Div([dcc.Graph(id="fig-camembert", config={"displayModeBar": False})],
                 style={"background": CARTE, "borderRadius": "16px", "padding": "10px",
                        "border": f"1px solid {BORD}"}),
        html.Div([dcc.Graph(id="fig-pays", config={"displayModeBar": False})],
                 style={"background": CARTE, "borderRadius": "16px", "padding": "10px",
                        "border": f"1px solid {BORD}"}),
    ], style={"display": "grid", "gridTemplateColumns": "1fr 2fr",
              "gap": "16px", "marginBottom": "16px"}),

    # Ligne 2 : operateurs + liaisons
    html.Div([
        html.Div([dcc.Graph(id="fig-operateurs", config={"displayModeBar": False})],
                 style={"background": CARTE, "borderRadius": "16px", "padding": "10px",
                        "border": f"1px solid {BORD}"}),
        html.Div([dcc.Graph(id="fig-liaisons", config={"displayModeBar": False})],
                 style={"background": CARTE, "borderRadius": "16px", "padding": "10px",
                        "border": f"1px solid {BORD}"}),
    ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
              "gap": "16px", "marginBottom": "16px"}),

], style={"background": FOND, "minHeight": "100vh", "padding": "28px 32px",
          "fontFamily": "Helvetica, Arial, sans-serif"})


@app.callback(
    Output("kpis", "children"),
    Output("fig-camembert", "figure"),
    Output("fig-pays", "figure"),
    Output("fig-operateurs", "figure"),
    Output("fig-liaisons", "figure"),
    Input("pays-filtre", "value"),
)
def maj_dashboard(pays: str):
    df = filtrer(charger_donnees(), pays)
    jour, nuit = compter_jour_nuit(df)
    total = jour + nuit
    pct = f"{nuit / total * 100:.1f}%" if total else "0%"

    kpis = [
        kpi_card("Trains de jour", f"{jour:,}", "#F8E4A0"),
        kpi_card("Trains de nuit", f"{nuit:,}", "#D7E2EE"),
        kpi_card("Part de nuit", pct, "#D8EEE8"),
    ]

    return (
        kpis,
        fig_camembert(jour, nuit),
        fig_par_pays(df),
        fig_top_operateurs(df),
        fig_top_liaisons(df),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
