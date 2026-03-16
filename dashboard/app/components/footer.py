import streamlit as st
import pandas as pd
import io

try:
    from database import get_connection
except ImportError:
    get_connection = None


def render_footer(df: pd.DataFrame) -> None:
    st.markdown("---")

    col1, col2 = st.columns([2, 1])

    with col1:
        with st.expander("Qualité des données", expanded=False):
            _render_data_quality()

    with col2:
        _render_exports(df)


def _render_data_quality() -> None:
    if get_connection is None:
        st.info("Base de données non configurée")
        return

    try:
        conn = get_connection()
        df_quality = pd.read_sql("SELECT * FROM v_data_quality", conn)

        if not df_quality.empty:
            st.dataframe(
                df_quality,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "table_name": st.column_config.TextColumn("Table", width="medium"),
                    "total_records": st.column_config.NumberColumn(
                        "Total", width="small"
                    ),
                },
            )
        else:
            st.info("Données de qualité non disponibles")

        df_missing = pd.read_sql(
            """
            SELECT 
                'stations' as table_name,
                'coordinates' as field,
                COUNT(*) FILTER (WHERE latitude IS NULL OR longitude IS NULL) as missing_count,
                COUNT(*) as total_count
            FROM stations
            UNION ALL
            SELECT 
                'stations' as table_name,
                'uic_code' as field,
                COUNT(*) FILTER (WHERE uic_code IS NULL) as missing_count,
                COUNT(*) as total_count
            FROM stations
            UNION ALL
            SELECT 
                'trains' as table_name,
                'category' as field,
                COUNT(*) FILTER (WHERE category IS NULL) as missing_count,
                COUNT(*) as total_count
            FROM trains
            UNION ALL
            SELECT 
                'schedules' as table_name,
                'distance_km' as field,
                COUNT(*) FILTER (WHERE distance_km IS NULL) as missing_count,
                COUNT(*) as total_count
            FROM schedules
        """,
            conn,
        )

        if not df_missing.empty:
            df_missing["missing_pct"] = (
                df_missing["missing_count"] / df_missing["total_count"] * 100
            ).round(2)

            st.markdown("#### Valeurs manquantes")
            st.dataframe(
                df_missing,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "table_name": st.column_config.TextColumn("Table", width="small"),
                    "field": st.column_config.TextColumn("Champ", width="medium"),
                    "missing_count": st.column_config.NumberColumn(
                        "Manquantes", width="small"
                    ),
                    "total_count": st.column_config.NumberColumn(
                        "Total", width="small"
                    ),
                    "missing_pct": st.column_config.NumberColumn("%", width="small"),
                },
            )
    except Exception:
        st.info("Données de qualité non disponibles")


def _render_exports(df: pd.DataFrame) -> None:
    st.markdown("#### Export")

    col1, col2 = st.columns(2)

    with col1:
        if not df.empty:
            csv = df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                label="CSV",
                data=csv,
                file_name="obrail_export.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.button("CSV", disabled=True, use_container_width=True)

    with col2:
        if not df.empty:
            try:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name="Données", index=False)

                st.download_button(
                    label="Excel",
                    data=output.getvalue(),
                    file_name="obrail_export.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            except ImportError:
                st.button(
                    "Excel (openpyxl requis)", disabled=True, use_container_width=True
                )
        else:
            st.button("Excel", disabled=True, use_container_width=True)
