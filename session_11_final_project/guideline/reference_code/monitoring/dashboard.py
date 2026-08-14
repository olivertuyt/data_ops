"""Small operational dashboard backed by Trino audit and Gold tables."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
import trino


@st.cache_resource
def connection():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "dashboard"),
        catalog="shopvn",
        schema="gold",
    )


def query(sql: str) -> pd.DataFrame:
    cursor = connection().cursor()
    cursor.execute(sql)
    columns = [item[0] for item in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


st.set_page_config(page_title="ShopVN DataOps", layout="wide")
st.title("ShopVN DataOps Dashboard")

latest_publish = query(
    """
    SELECT max(recorded_at) AS finished_at
    FROM shopvn.audit.pipeline_runs
    WHERE stage = 'gold_publish' AND object_name = 'all_gold_tables' AND status = 'PASS'
    """
)
finished_at = latest_publish.iloc[0]["finished_at"] if not latest_publish.empty else None
st.metric("Latest successful pipeline finish", str(finished_at or "No successful run"))

col1, col2 = st.columns(2)
with col1:
    st.subheader("Latest component status")
    st.dataframe(
        query(
            """
            WITH ranked AS (
              SELECT *, row_number() OVER (
                PARTITION BY stage, object_name ORDER BY recorded_at DESC
              ) AS rank_no
              FROM shopvn.audit.pipeline_runs
            )
            SELECT stage, object_name, status, business_date, recorded_at, error_message
            FROM ranked WHERE rank_no = 1 ORDER BY stage, object_name
            """
        ),
        use_container_width=True,
    )
with col2:
    st.subheader("Blocking DQ and reconciliation")
    st.dataframe(
        query(
            """
            SELECT run_id, check_name, passed, actual_value, expected_value, checked_at
            FROM shopvn.audit.data_quality_results
            ORDER BY checked_at DESC LIMIT 50
            """
        ),
        use_container_width=True,
    )

st.subheader("Revenue freshness and source reconciliation")
st.dataframe(
    query(
        """
        SELECT metric_date, sales_channel, revenue_basis, net_revenue, discount_cost, refund_amount,
               order_count, run_id, published_at
        FROM shopvn.gold.fact_daily_revenue
        WHERE metric_date >= current_date - INTERVAL '14' DAY
        ORDER BY metric_date DESC, sales_channel
        """
    ),
    use_container_width=True,
)

st.subheader("Recent SFTP integrity status")
st.dataframe(
    query(
        """
        SELECT business_date, partner, file_name, status, expected_md5, actual_md5,
               error_message, checked_at
        FROM shopvn.audit.source_manifests
        ORDER BY checked_at DESC LIMIT 100
        """
    ),
    use_container_width=True,
)
