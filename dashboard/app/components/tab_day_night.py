import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from analytics.shared_metrics import get_day_night_country_summary
from analytics.analytics_loader import get_data_source_label

COLOR_DAY = "#FFD700"
COLOR_NIGHT = "#191970"


def _has_classification_columns(df: pd.DataFrame) -> bool:
    return "classification_method" in df.columns


def _get_classification_summary(df: pd.DataFrame) -> dict | None:
    if not _has_classification_columns(df):
        return None

    trains_df = (
        df.drop_duplicates(subset=["train_id"]) if "train_id" in df.columns else df
    )
    method_counts = (
        trains_df["classification_method"].value_counts().to_dict()
        if "classification_method" in trains_df.columns
        else {}
    )

    review_count = 0
    if "needs_manual_review" in trains_df.columns:
        review_count = int(trains_df["needs_manual_review"].sum())

    confidence_stats = None
    if "classification_confidence" in trains_df.columns:
        conf_series = trains_df["classification_confidence"].dropna()
        if not conf_series.empty:
            confidence_stats = {
                "mean": float(conf_series.mean()),
                "median": float(conf_series.median()),
                "min": float(conf_series.min()),
                "max": float(conf_series.max()),
            }

    return {
        "method_counts": method_counts,
        "review_count": review_count,
        "total_trains": len(trains_df),
        "confidence_stats": confidence_stats,
    }


def render_tab_day_night(df: pd.DataFrame) -> None:
    st.subheader("Comparaison Jour/Nuit")
    st.caption(f"Source: {get_data_source_label()}")

    if df.empty:
        st.warning("Aucune donnée disponible.")
        return

    sub_tab1, sub_tab2, sub_tab3 = st.tabs(
        ["Descriptif", "Normalisé", "Qualité classification"]
    )

    with sub_tab1:
        _render_descriptive(df)

    with sub_tab2:
        _render_normalized(df)

    with sub_tab3:
        _render_classification_quality(df)


def _render_descriptive(df: pd.DataFrame) -> None:
    summary_df = get_day_night_country_summary(df)

    if summary_df.empty:
        st.info("Aucune donnée disponible pour l'analyse descriptive.")
        return

    df_display = summary_df.copy()
    df_display["Type"] = df_display["train_type"].replace(
        {"day": "Jour", "night": "Nuit"}
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Nombre de trains")
        fig = px.bar(
            df_display,
            x="country",
            y="nb_trains",
            color="Type",
            barmode="group",
            color_discrete_map={"Jour": COLOR_DAY, "Nuit": COLOR_NIGHT},
            labels={"nb_trains": "Nombre de trains", "country": "Pays"},
        )
        fig.update_layout(
            xaxis_title="Pays", yaxis_title="Nombre de trains", legend_title="Type"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Durée moyenne (minutes)")
        fig = px.bar(
            df_display,
            x="country",
            y="avg_duration",
            color="Type",
            barmode="group",
            color_discrete_map={"Jour": COLOR_DAY, "Nuit": COLOR_NIGHT},
            labels={"avg_duration": "Durée moyenne (min)", "country": "Pays"},
        )
        fig.update_layout(
            xaxis_title="Pays", yaxis_title="Durée (minutes)", legend_title="Type"
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("#### Distance moyenne (km)")
        fig = px.bar(
            df_display,
            x="country",
            y="avg_distance",
            color="Type",
            barmode="group",
            color_discrete_map={"Jour": COLOR_DAY, "Nuit": COLOR_NIGHT},
            labels={"avg_distance": "Distance moyenne (km)", "country": "Pays"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.markdown("#### Nombre de dessertes")
        fig = px.bar(
            df_display,
            x="country",
            y="nb_schedules",
            color="Type",
            barmode="group",
            color_discrete_map={"Jour": COLOR_DAY, "Nuit": COLOR_NIGHT},
            labels={"nb_schedules": "Nombre de dessertes", "country": "Pays"},
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_normalized(df: pd.DataFrame) -> None:
    st.markdown("""
    **Pourquoi des métriques normalisées ?**
    
    Comparer le nombre brut de trains jour/nuit est biaisé car il y a naturellement plus de trains de jour.
    Ces métriques ajustées permettent une comparaison juste.
    """)

    if "distance_km" not in df.columns:
        st.info("Données de distance non disponibles pour l'analyse normalisée.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Distribution par distance")
        df_dist = df.dropna(subset=["distance_km", "train_type_normalized"]).copy()
        if not df_dist.empty:
            df_dist["segment"] = pd.cut(
                df_dist["distance_km"],
                bins=[0, 200, 400, 600, 800, 1200, float("inf")],
                labels=["0-200", "200-400", "400-600", "600-800", "800-1200", "1200+"],
                right=False,
            )
            segment_counts = (
                df_dist.groupby(["segment", "train_type_normalized"], observed=True)
                .size()
                .reset_index(name="count")
            )
            fig = px.bar(
                segment_counts,
                x="segment",
                y="count",
                color="train_type_normalized",
                barmode="group",
                color_discrete_map={"day": COLOR_DAY, "night": COLOR_NIGHT},
                labels={"segment": "Segment de distance", "count": "Services"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Données insuffisantes")

    with col2:
        st.markdown("#### Part de nuit par distance")
        if not df_dist.empty:
            night_share = (
                df_dist.groupby("segment", observed=True)
                .apply(
                    lambda x: (x["train_type_normalized"] == "night").sum()
                    / len(x)
                    * 100
                )
                .reset_index(name="night_pct")
            )
            fig = px.bar(
                night_share,
                x="segment",
                y="night_pct",
                color_discrete_sequence=[COLOR_NIGHT],
                labels={"segment": "Segment", "night_pct": "% Nuit"},
            )
            fig.update_layout(yaxis_title="Part de trains de nuit (%)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Données insuffisantes")


def _render_classification_quality(df: pd.DataFrame) -> None:
    classification_summary = _get_classification_summary(df)

    if not classification_summary:
        st.info("Données de classification non disponibles.")
        return

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### Méthode de classification")
        method_counts = classification_summary["method_counts"]
        if method_counts:
            fig = px.pie(
                values=list(method_counts.values()),
                names=list(method_counts.keys()),
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Non disponible")

    with col2:
        st.markdown("#### Révision manuelle")
        review_count = classification_summary["review_count"]
        total_trains = classification_summary["total_trains"]
        review_pct = (review_count / total_trains * 100) if total_trains > 0 else 0

        st.metric(
            label="Trains à revoir",
            value=f"{review_count} / {total_trains}",
            delta=f"{review_pct:.1f}%",
        )

    with col3:
        st.markdown("#### Confiance")
        conf_stats = classification_summary["confidence_stats"]
        if conf_stats:
            st.metric(
                label="Confiance moyenne",
                value=f"{conf_stats['mean']:.2%}",
                delta=f"Min: {conf_stats['min']:.2%}, Max: {conf_stats['max']:.2%}",
            )
        else:
            st.info("Non disponible")
