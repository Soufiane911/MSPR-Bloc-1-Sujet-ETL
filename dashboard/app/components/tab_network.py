import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from analytics.analytics_loader import get_data_source_label
from analytics.long_distance_analysis import (
    get_distance_segments,
    get_long_distance_kpis,
    get_long_distance_co2,
    get_night_corridors,
    get_night_shift_candidates,
)
from analytics.network_analysis import (
    get_network_kpis,
    get_od_coverage,
    get_service_km,
    get_top_operators,
    get_country_breakdown,
)
from analytics.shared_metrics import get_operator_summary

COLOR_DAY = "#3498db"
COLOR_NIGHT = "#1f3a5f"


def render_tab_network(df: pd.DataFrame) -> None:
    st.subheader("Réseau & Longue distance")
    st.caption(f"Source: {get_data_source_label()}")

    if df.empty:
        st.warning("Aucune donnée disponible.")
        return

    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Réseau", "Longue distance", "Opérateurs"])

    with sub_tab1:
        _render_network(df)

    with sub_tab2:
        _render_long_distance(df)

    with sub_tab3:
        _render_operators(df)


def _render_network(df: pd.DataFrame) -> None:
    kpis = get_network_kpis(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("OD Jour uniquement", f"{int(kpis['only_day']):,}")
    col2.metric("OD Nuit uniquement", f"{int(kpis['only_night']):,}")
    col3.metric("OD Partagés", f"{int(kpis['both']):,}")
    col4.metric("Service-km total", f"{(kpis['day_km'] + kpis['night_km']) / 1e6:.1f}M")

    st.markdown("---")

    coverage = get_od_coverage(df)
    service_km = get_service_km(df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Couverture OD unique")
        if not coverage.empty:
            fig = px.bar(
                coverage,
                x="label",
                y="nb_routes",
                color="train_type_normalized",
                color_discrete_map={"day": COLOR_DAY, "night": COLOR_NIGHT},
                labels={"label": "Type", "nb_routes": "Routes uniques"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Non disponible")

    with col2:
        st.markdown("#### Contribution en service-km")
        if not service_km.empty:
            fig = px.pie(
                service_km,
                values="total_km",
                names="label",
                color="train_type_normalized",
                color_discrete_map={"day": COLOR_DAY, "night": COLOR_NIGHT},
                hole=0.5,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Non disponible")

    st.markdown("---")
    st.markdown("#### Répartition par pays")
    country_breakdown = get_country_breakdown(df, top_n=15)
    if not country_breakdown.empty:
        fig = px.bar(
            country_breakdown,
            x="country",
            y="count",
            color="train_type",
            barmode="group",
            color_discrete_map={"day": COLOR_DAY, "night": COLOR_NIGHT},
            labels={"country": "Pays", "count": "Services"},
        )
        fig.update_layout(xaxis_title="Pays", yaxis_title="Services")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Non disponible")


def _render_long_distance(df: pd.DataFrame) -> None:
    kpis = get_long_distance_kpis(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Services >= 500 km", f"{int(kpis['services']):,}")
    col2.metric("Part nuit", f"{kpis['night_pct']}%")
    col3.metric("Part jour", f"{kpis['day_pct']}%")
    col4.metric("CO2 moyen économisé", f"{kpis['avg_co2_night']:.0f} kg")

    st.markdown("---")

    segments = get_distance_segments(df)
    co2_df = get_long_distance_co2(df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Part de nuit par segment")
        if not segments.empty:
            fig = px.bar(
                segments,
                x="segment",
                y="night_pct",
                color_discrete_sequence=[COLOR_NIGHT],
                labels={"segment": "Segment", "night_pct": "% Nuit"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Non disponible")

    with col2:
        st.markdown("#### CO2 économisé par type")
        if not co2_df.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=co2_df["train_type"],
                    y=co2_df["avg_co2"],
                    marker_color=[
                        COLOR_DAY if t == "day" else COLOR_NIGHT
                        for t in co2_df["train_type"]
                    ],
                )
            )
            fig.update_layout(xaxis_title="Type", yaxis_title="CO2 moyen (kg)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Non disponible")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Corridors de nuit (>= 500 km)")
        night_corridors = get_night_corridors(df)
        if not night_corridors.empty:
            st.dataframe(
                night_corridors.head(10), use_container_width=True, hide_index=True
            )
        else:
            st.info("Aucun corridor identifié")

    with col2:
        st.markdown("#### Candidats passage de nuit")
        candidates = get_night_shift_candidates(df)
        if not candidates.empty:
            st.dataframe(candidates.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("Aucun candidat identifié")


def _render_operators(df: pd.DataFrame) -> None:
    st.markdown("#### Top opérateurs par type de service")

    top_ops = get_top_operators(df, top_n=10)
    if not top_ops.empty:
        fig = px.bar(
            top_ops,
            x="count",
            y="operator",
            orientation="h",
            color="train_type_normalized",
            barmode="group",
            color_discrete_map={"day": COLOR_DAY, "night": COLOR_NIGHT},
            labels={"count": "Services", "operator": "Opérateur"},
        )
        fig.update_layout(yaxis_title="", xaxis_title="Services", height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Non disponible")

    st.markdown("---")
    st.markdown("#### Détail par opérateur")

    df_ops = get_operator_summary(df)
    if not df_ops.empty:
        st.dataframe(
            df_ops.head(15),
            use_container_width=True,
            hide_index=True,
            column_config={
                "operator_name": st.column_config.TextColumn(
                    "Opérateur", width="large"
                ),
                "country": st.column_config.TextColumn("Pays", width="small"),
                "nb_trains": st.column_config.NumberColumn("Trains", width="small"),
                "day_trains": st.column_config.NumberColumn("Jour", width="small"),
                "night_trains": st.column_config.NumberColumn("Nuit", width="small"),
                "avg_duration": st.column_config.NumberColumn(
                    "Durée moy. (min)", width="small"
                ),
                "avg_distance": st.column_config.NumberColumn(
                    "Dist. moy. (km)", width="small"
                ),
            },
        )
    else:
        st.info("Non disponible")
