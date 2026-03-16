import streamlit as st
import plotly.express as px
import pandas as pd
from analytics.shared_metrics import (
    get_train_type_counts,
    get_country_train_counts,
    get_top_operators_summary,
)
from analytics.analytics_loader import get_data_source_label


def render_tab_overview(df: pd.DataFrame) -> None:
    st.subheader("Vue d'ensemble")
    st.caption(f"Source: {get_data_source_label()}")

    if df.empty:
        st.warning("Aucune donnée disponible.")
        return

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Répartition Jour/Nuit")
        df_type = get_train_type_counts(df)
        if not df_type.empty:
            df_type["train_type_label"] = df_type["train_type"].replace(
                {"day": "Jour", "night": "Nuit"}
            )
            fig = px.pie(
                df_type,
                values="count",
                names="train_type_label",
                color="train_type",
                color_discrete_map={"day": "#FFD700", "night": "#191970"},
                hole=0.4,
            )
            fig.update_traces(textinfo="percent+label", textfont_size=14)
            fig.update_layout(showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Données non disponibles")

    with col_right:
        st.markdown("#### Répartition par pays")
        df_country = get_country_train_counts(df, limit=10)
        if not df_country.empty:
            fig = px.bar(
                df_country,
                x="country",
                y="nb_trains",
                color="nb_trains",
                color_continuous_scale="Blues",
                labels={"country": "Pays", "nb_trains": "Nombre de trains"},
            )
            fig.update_layout(
                xaxis_title="Pays",
                yaxis_title="Nombre de trains",
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Données non disponibles")

    st.markdown("---")
    st.markdown("#### Top opérateurs")
    df_ops = get_top_operators_summary(df, limit=10)
    if not df_ops.empty:
        st.dataframe(
            df_ops,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Opérateur": st.column_config.TextColumn("Opérateur", width="large"),
                "Pays": st.column_config.TextColumn("Pays", width="small"),
                "Trains": st.column_config.NumberColumn("Trains", width="small"),
                "Jour": st.column_config.NumberColumn("Jour", width="small"),
                "Nuit": st.column_config.NumberColumn("Nuit", width="small"),
                "Dessertes": st.column_config.NumberColumn("Dessertes", width="small"),
            },
        )
    else:
        st.info("Données opérateurs non disponibles")
