"""Read-only evidence dashboard for the four ShopVN business use cases."""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import streamlit as st
import trino

SERVING_CATALOG = "shopvn"
SERVING_SCHEMA = "serving"


@st.cache_resource
def connection():
    """Create the read-only Trino connection used by every dashboard view."""
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user=os.environ["TRINO_USER"],
        catalog=SERVING_CATALOG,
        schema=SERVING_SCHEMA,
    )


def query(sql: str) -> pd.DataFrame:
    """Run a fixed dashboard query and return a labelled result frame."""
    cursor = connection().cursor()
    cursor.execute(sql)
    columns = [item[0] for item in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def date_literal(value: date) -> str:
    """Return a SQL date literal from a Streamlit-controlled date value."""
    return f"DATE '{value.isoformat()}'"


def format_vnd(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):,.0f} VND"


def format_compact_vnd(value: float | int | None) -> str:
    """Format VND for narrow KPI cards without truncating the value."""
    if value is None or pd.isna(value):
        return "N/A"
    numeric_value = float(value)
    if abs(numeric_value) >= 1_000_000_000:
        return f"{numeric_value / 1_000_000_000:.2f}B VND"
    if abs(numeric_value) >= 1_000_000:
        return f"{numeric_value / 1_000_000:.2f}M VND"
    return format_vnd(numeric_value)


def format_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}%"


def metric_card(
    container,
    label: str,
    value: str,
    *,
    exact_value: str | None = None,
    help_text: str | None = None,
) -> None:
    """Render a readable KPI plus optional exact evidence below it."""
    container.metric(label, value, help=help_text)
    if exact_value:
        container.caption(f"Exact: {exact_value}")


def present_vnd(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Format monetary columns for reviewer-readable tables without changing source data."""
    result = frame.copy()
    for column in columns:
        if column in result:
            result[column] = result[column].map(format_vnd)
    return result


def latest_publications() -> pd.DataFrame:
    return query(
        """
        SELECT domain, max(published_at) AS finished_at,
               max_by(start_date, published_at) AS start_date,
               max_by(end_date, published_at) AS end_date
        FROM shopvn.audit.published_domains
        WHERE status = 'PASS'
        GROUP BY domain
        ORDER BY domain
        """
    )


def dashboard_dates() -> list[date]:
    dates = query(
        """
        SELECT DISTINCT metric_date
        FROM shopvn.serving.fact_daily_revenue
        WHERE revenue_basis = 'earned'
        ORDER BY metric_date DESC
        """
    )
    return [value for value in dates["metric_date"].tolist() if isinstance(value, date)]


def render_evidence_header(selected_date: date) -> None:
    """Show why the selected serving slice is valid evidence for the four use cases."""
    business_date = selected_date.isoformat()
    publication = query(
        f"""
        SELECT count(*) AS published_domains,
               count_if(status = 'PASS') AS passed_domains
        FROM shopvn.audit.published_domains
        WHERE start_date = '{business_date}' AND end_date = '{business_date}'
        """
    ).iloc[0]
    quality = query(
        f"""
        SELECT count_if(severity = 'BLOCKING') AS blocking_checks,
               count_if(severity = 'BLOCKING' AND passed) AS passed_checks,
               count_if(severity = 'BLOCKING' AND NOT passed) AS failed_checks,
               count_if(severity = 'WARNING' AND NOT passed) AS retained_warnings
        FROM shopvn.audit.data_quality_results
        WHERE start_date = '{business_date}'
        """
    ).iloc[0]
    st.info(
        "Scope: one DQ-approved serving snapshot for the selected business date. "
        "Finance cards show earned revenue; warnings remain visible and are not silently removed."
    )
    date_card, publication_card, dq_card, warning_card = st.columns(4)
    metric_card(date_card, "Evidence date", business_date)
    metric_card(
        publication_card,
        "Domains published",
        (
            f"{int(publication['passed_domains'] or 0)}/"
            f"{int(publication['published_domains'] or 0)} PASS"
        ),
        help_text="Customer, Finance, Operations, and Product markers for this date.",
    )
    metric_card(
        dq_card,
        "Blocking DQ",
        f"{int(quality['passed_checks'] or 0)}/{int(quality['blocking_checks'] or 0)} PASS",
        help_text="Only blocking checks determine whether serving data may be published.",
    )
    metric_card(
        warning_card,
        "Retained anomaly warnings",
        f"{int(quality['retained_warnings'] or 0)}",
        help_text=(
            "Non-blocking anomalies remain auditable and do not make the run look cleaner "
            "than it is."
        ),
    )
    if int(quality["failed_checks"] or 0) > 0:
        st.error(
            "A non-warning DQ failure exists for this date; do not use this slice as "
            "completed evidence."
        )


def render_finance(selected_date: date) -> None:
    day = date_literal(selected_date)
    st.subheader("UC1 — Finance: revenue and spending")
    st.caption(
        "Daily direct/marketplace revenue, discounts, refunds, customer spend, and voucher cost."
    )
    headline = query(
        f"""
        SELECT
          CAST(sum(net_revenue) AS DOUBLE) AS net_revenue,
          sum(order_count) AS order_count,
          CAST(sum(discount_cost) AS DOUBLE) AS discount_cost,
          CAST(sum(refund_amount) AS DOUBLE) AS refund_amount
        FROM shopvn.serving.fact_daily_revenue
        WHERE metric_date = {day} AND revenue_basis = 'earned'
        """
    ).iloc[0]
    metrics = st.columns(4)
    metric_card(
        metrics[0],
        "Earned net revenue",
        format_compact_vnd(headline["net_revenue"]),
        exact_value=format_vnd(headline["net_revenue"]),
        help_text="After discounts, refunded returns, and marketplace commissions.",
    )
    metric_card(metrics[1], "Eligible orders", f"{int(headline['order_count'] or 0):,}")
    metric_card(
        metrics[2],
        "Discount cost",
        format_compact_vnd(headline["discount_cost"]),
        exact_value=format_vnd(headline["discount_cost"]),
    )
    metric_card(
        metrics[3],
        "Refund amount",
        format_compact_vnd(headline["refund_amount"]),
        exact_value=format_vnd(headline["refund_amount"]),
    )

    channel_revenue = query(
        f"""
        SELECT channel_group, sales_channel,
               CAST(net_revenue AS DOUBLE) AS net_revenue,
               order_count
        FROM shopvn.serving.fact_daily_revenue
        WHERE metric_date = {day} AND revenue_basis = 'earned'
        ORDER BY net_revenue DESC
        """
    )
    if not channel_revenue.empty:
        leading_channel = channel_revenue.iloc[0]
        direct_revenue = channel_revenue.loc[
            channel_revenue["channel_group"] == "direct", "net_revenue"
        ].sum()
        marketplace_revenue = channel_revenue.loc[
            channel_revenue["channel_group"] == "marketplace", "net_revenue"
        ].sum()
        total_revenue = direct_revenue + marketplace_revenue
        direct_share = 100 * direct_revenue / total_revenue if total_revenue else 0
        st.success(
            f"Insight: {leading_channel['sales_channel']} is the leading channel with "
            f"{format_vnd(leading_channel['net_revenue'])}. Direct contributes "
            f"{format_percent(direct_share)} of earned revenue."
        )
    left, right = st.columns((3, 2))
    with left:
        st.markdown("**Revenue by channel**")
        st.bar_chart(channel_revenue.set_index("sales_channel")["net_revenue"])
        st.dataframe(
            present_vnd(channel_revenue, ["net_revenue"]),
            column_config={
                "order_count": st.column_config.NumberColumn("Orders", format="%,d")
            },
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.markdown("**Customer daily spend**")
        spend_by_tier = query(
            f"""
            SELECT loyalty_tier,
                   count(*) AS customers,
                   CAST(avg(daily_net_spend) AS DOUBLE) AS avg_daily_spend,
                   CAST(avg(month_to_date_spend) AS DOUBLE) AS avg_month_to_date_spend
            FROM shopvn.serving.fact_customer_daily
            WHERE metric_date = {day}
            GROUP BY loyalty_tier
            ORDER BY avg_month_to_date_spend DESC
            """
        )
        spend_display = present_vnd(
            spend_by_tier, ["avg_daily_spend", "avg_month_to_date_spend"]
        ).rename(
            columns={
                "loyalty_tier": "Tier",
                "customers": "Customers",
                "avg_daily_spend": "Avg daily spend",
                "avg_month_to_date_spend": "Avg MTD spend",
            }
        )
        st.dataframe(
            spend_display,
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("**Voucher and campaign cost impact**")
    campaign_impact = query(
        f"""
        SELECT campaign_id, sum(usage_count) AS voucher_uses,
               CAST(sum(net_revenue) AS DOUBLE) AS attributed_revenue,
               CAST(sum(discount_cost) AS DOUBLE) AS discount_cost
        FROM shopvn.serving.fact_voucher_daily
        WHERE metric_date = {day}
        GROUP BY campaign_id
        ORDER BY attributed_revenue DESC
        LIMIT 10
        """
    )
    st.dataframe(
        present_vnd(campaign_impact, ["attributed_revenue", "discount_cost"]),
        use_container_width=True,
        hide_index=True,
    )


def render_operations(selected_date: date) -> None:
    day = date_literal(selected_date)
    st.subheader("UC2 — Operations: delivery performance")
    st.caption(
        "Carrier delivery quality, failure reasons, redelivery attempts, and return rate."
    )
    carrier = query(
        f"""
        SELECT carrier,
               sum(shipment_count) AS shipments,
               CAST(100.0 * sum(delivered_count) / nullif(sum(shipment_count), 0) AS DOUBLE)
                 AS delivery_success_pct,
               CAST(100.0 * sum(failed_count) / nullif(sum(shipment_count), 0) AS DOUBLE)
                 AS delivery_failure_pct,
               CAST(100.0 * sum(redelivery_count) / nullif(sum(shipment_count), 0) AS DOUBLE)
                 AS redelivery_pct,
               CAST(
                 sum(avg_delivery_hours * shipment_count) / nullif(sum(shipment_count), 0)
                 AS DOUBLE
               )
                 AS weighted_avg_delivery_hours
        FROM shopvn.serving.fact_delivery_daily
        WHERE metric_date = {day} AND carrier <> 'unassigned'
        GROUP BY carrier
        ORDER BY delivery_success_pct DESC
        """
    )
    if not carrier.empty:
        best_carrier = carrier.iloc[0]
        st.success(
            f"Insight: {best_carrier['carrier']} has the highest delivery success rate at "
            f"{format_percent(best_carrier['delivery_success_pct'])} for the selected date."
        )
    left, right = st.columns((3, 2))
    with left:
        st.markdown("**Carrier scorecard**")
        carrier_display = carrier.rename(
            columns={
                "delivery_success_pct": "Success rate",
                "delivery_failure_pct": "Failure rate",
                "redelivery_pct": "Redelivery rate",
                "weighted_avg_delivery_hours": "Avg delivery hours",
            }
        )
        for column in ("Success rate", "Failure rate", "Redelivery rate"):
            carrier_display[column] = carrier_display[column].map(format_percent)
        carrier_display["Avg delivery hours"] = carrier_display[
            "Avg delivery hours"
        ].map(lambda value: "N/A" if pd.isna(value) else f"{value:.1f}h")
        st.dataframe(carrier_display, use_container_width=True, hide_index=True)
    with right:
        st.markdown("**Delivery success rate**")
        if not carrier.empty:
            st.bar_chart(carrier.set_index("carrier")["delivery_success_pct"])

    failures, returns = st.columns(2)
    with failures:
        st.markdown("**Failure reasons**")
        failure_reasons = query(
            f"""
            SELECT recipient_province, failure_reason, sum(failed_count) AS failed_shipments
            FROM shopvn.serving.fact_delivery_daily
            WHERE metric_date = {day} AND failed_count > 0
            GROUP BY recipient_province, failure_reason
            ORDER BY failed_shipments DESC
            LIMIT 12
            """
        )
        st.dataframe(
            failure_reasons.rename(
                columns={
                    "recipient_province": "Province",
                    "failure_reason": "Reason",
                    "failed_shipments": "Failed shipments",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with returns:
        st.markdown("**Return rate by category and channel**")
        return_rates = query(
            f"""
            SELECT category, sales_channel, sold_units, returned_units,
                   CAST(100.0 * return_rate AS DOUBLE) AS return_rate_pct
            FROM shopvn.serving.fact_return_daily
            WHERE metric_date = {day}
            ORDER BY return_rate DESC
            LIMIT 12
            """
        )
        return_rates["return_rate_pct"] = return_rates["return_rate_pct"].map(
            format_percent
        )
        st.dataframe(
            return_rates.rename(
                columns={"sales_channel": "Channel", "return_rate_pct": "Return rate"}
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_customer_marketing(selected_date: date) -> None:
    day = date_literal(selected_date)
    st.subheader("UC3 — Customer & Marketing: behavior and campaigns")
    st.caption(
        "Loyalty segmentation, lifecycle, campaign effectiveness, and direct-channel ratings."
    )
    segments = query(
        f"""
        SELECT loyalty_tier,
               count(*) AS customers,
               CAST(avg(month_to_date_spend) AS DOUBLE) AS avg_month_to_date_spend,
               CAST(sum(daily_net_spend) AS DOUBLE) AS daily_spend
        FROM shopvn.serving.fact_customer_daily
        WHERE metric_date = {day}
        GROUP BY loyalty_tier
        ORDER BY avg_month_to_date_spend DESC
        """
    )
    if not segments.empty:
        leading_tier = segments.iloc[0]
        st.success(
            f"Insight: {leading_tier['loyalty_tier']} customers have the highest average "
            f"month-to-date spend at {format_vnd(leading_tier['avg_month_to_date_spend'])}."
        )
    left, right = st.columns(2)
    with left:
        st.markdown("**Spend by loyalty tier**")
        st.bar_chart(segments.set_index("loyalty_tier")["avg_month_to_date_spend"])
        st.dataframe(
            present_vnd(segments, ["avg_month_to_date_spend", "daily_spend"]).rename(
                columns={
                    "loyalty_tier": "Tier",
                    "customers": "Customers",
                    "avg_month_to_date_spend": "Avg MTD spend",
                    "daily_spend": "Daily spend",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.markdown("**Customer lifecycle**")
        lifecycle = query(
            f"""
            SELECT lifecycle_stage, count(*) AS customers,
                   sum(CASE WHEN is_repeat_within_30_days THEN 1 ELSE 0 END)
                     AS repeat_30d_customers
            FROM shopvn.serving.fact_customer_daily
            WHERE metric_date = {day}
            GROUP BY lifecycle_stage
            ORDER BY customers DESC
            """
        )
        st.dataframe(
            lifecycle.rename(
                columns={
                    "lifecycle_stage": "Lifecycle stage",
                    "customers": "Customers",
                    "repeat_30d_customers": "Repeat within 30d",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    campaigns, ratings = st.columns(2)
    with campaigns:
        st.markdown("**Campaign effectiveness**")
        campaign_results = query(
            f"""
            SELECT campaign_id, sum(usage_count) AS uses,
                   CAST(sum(net_revenue) AS DOUBLE) AS revenue,
                   CAST(sum(discount_cost) AS DOUBLE) AS discount_cost
            FROM shopvn.serving.fact_voucher_daily
            WHERE metric_date = {day}
            GROUP BY campaign_id
            ORDER BY revenue DESC
            LIMIT 10
            """
        )
        if not campaign_results.empty:
            top_campaign = campaign_results.iloc[0]
            st.caption(
                f"Highest attributed revenue: {top_campaign['campaign_id']} "
                f"({format_vnd(top_campaign['revenue'])})."
            )
        st.dataframe(
            present_vnd(campaign_results, ["revenue", "discount_cost"]).rename(
                columns={
                    "campaign_id": "Campaign",
                    "uses": "Uses",
                    "revenue": "Attributed revenue",
                    "discount_cost": "Discount cost",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with ratings:
        st.markdown("**Product ratings (direct channel only)**")
        rating_results = query(
            f"""
            SELECT category, sales_channel,
                   CAST(average_rating AS DOUBLE) AS average_rating,
                   review_count
            FROM shopvn.serving.fact_product_rating_daily
            WHERE metric_date = {day}
            ORDER BY average_rating DESC, review_count DESC
            LIMIT 12
            """
        )
        st.dataframe(
            rating_results.rename(
                columns={
                    "sales_channel": "Channel",
                    "average_rating": "Average rating",
                    "review_count": "Reviews",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("**Current loyalty-tier versions**")
    st.dataframe(
        query(
            """
            SELECT loyalty_tier, count(*) AS current_customers
            FROM shopvn.serving.dim_customer_scd2
            WHERE is_current = true
            GROUP BY loyalty_tier
            ORDER BY current_customers DESC
            """
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_product(selected_date: date) -> None:
    day = date_literal(selected_date)
    st.subheader("UC4 — Inventory & Product: stock and performance")
    st.caption(
        "End-of-day inventory, product/channel performance, loss flags, and stockout forecast."
    )
    headline = query(
        f"""
        SELECT
          count(DISTINCT product_id) AS products,
          sum(CASE WHEN is_selling_at_loss THEN 1 ELSE 0 END) AS loss_making_rows,
          sum(CASE WHEN days_to_stockout BETWEEN 0 AND 7 THEN 1 ELSE 0 END) AS stockout_risk_rows,
          CAST(sum(units_sold) AS DOUBLE) AS units_sold
        FROM shopvn.serving.fact_product_channel_daily
        WHERE metric_date = {day}
        """
    ).iloc[0]
    metrics = st.columns(4)
    metric_card(metrics[0], "Products sold", f"{int(headline['products'] or 0):,}")
    metric_card(metrics[1], "Units sold", f"{int(headline['units_sold'] or 0):,}")
    metric_card(
        metrics[2],
        "Product-channel rows at loss",
        f"{int(headline['loss_making_rows'] or 0):,}",
    )
    metric_card(
        metrics[3],
        "Stockout risks (within 7d)",
        f"{int(headline['stockout_risk_rows'] or 0):,}",
    )
    if int(headline["stockout_risk_rows"] or 0) > 0:
        st.warning(
            "Insight: prioritise replenishment for the product-channel rows in the "
            "seven-day stockout forecast below."
        )

    channel, stockout = st.columns(2)
    with channel:
        st.markdown("**Sales by channel**")
        channel_sales = query(
            f"""
            SELECT sales_channel, sum(units_sold) AS units_sold,
                   CAST(sum(net_revenue) AS DOUBLE) AS net_revenue
            FROM shopvn.serving.fact_product_channel_daily
            WHERE metric_date = {day}
            GROUP BY sales_channel
            ORDER BY net_revenue DESC
            """
        )
        st.bar_chart(channel_sales.set_index("sales_channel")["net_revenue"])
        st.dataframe(
            present_vnd(channel_sales, ["net_revenue"]).rename(
                columns={
                    "sales_channel": "Channel",
                    "units_sold": "Units sold",
                    "net_revenue": "Net revenue",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with stockout:
        st.markdown("**Stockout forecast**")
        stockout_forecast = query(
            f"""
            SELECT product_id, category, sales_channel, current_stock_qty,
                   CAST(avg_daily_units_sold_7d AS DOUBLE) AS avg_daily_units_sold_7d,
                   CAST(days_to_stockout AS DOUBLE) AS days_to_stockout
            FROM shopvn.serving.fact_product_channel_daily
            WHERE metric_date = {day} AND days_to_stockout BETWEEN 0 AND 7
            ORDER BY days_to_stockout, product_id
            LIMIT 15
            """
        )
        st.dataframe(
            stockout_forecast.rename(
                columns={
                    "product_id": "Product ID",
                    "sales_channel": "Channel",
                    "current_stock_qty": "Stock qty",
                    "avg_daily_units_sold_7d": "Avg daily units",
                    "days_to_stockout": "Days left",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    losses, inventory = st.columns(2)
    with losses:
        st.markdown("**Products selling below cost**")
        loss_products = query(
            f"""
            SELECT product_id, category, sales_channel, cost_price,
                   average_net_unit_price, units_sold
            FROM shopvn.serving.fact_product_channel_daily
            WHERE metric_date = {day} AND is_selling_at_loss = true
            ORDER BY units_sold DESC
            LIMIT 15
            """
        )
        st.dataframe(
            present_vnd(loss_products, ["cost_price", "average_net_unit_price"]).rename(
                columns={
                    "product_id": "Product ID",
                    "sales_channel": "Channel",
                    "cost_price": "Cost price",
                    "average_net_unit_price": "Avg net unit price",
                    "units_sold": "Units sold",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with inventory:
        st.markdown("**End-of-day inventory**")
        inventory_eod = query(
            f"""
            SELECT warehouse_id, count(*) AS product_rows,
                   sum(eod_stock_qty) AS total_eod_stock
            FROM shopvn.serving.fact_inventory_eod
            WHERE snapshot_date = {day}
            GROUP BY warehouse_id
            ORDER BY warehouse_id
            """
        )
        st.dataframe(
            inventory_eod.rename(
                columns={
                    "warehouse_id": "Warehouse",
                    "product_rows": "Product rows",
                    "total_eod_stock": "End-of-day stock",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_operational_evidence() -> None:
    with st.expander(
        "Operational evidence: publication, DQ, reconciliation, and source integrity"
    ):
        first, second = st.columns(2)
        with first:
            st.markdown("**Latest successful publication by domain**")
            st.dataframe(
                latest_publications(), use_container_width=True, hide_index=True
            )
        with second:
            st.markdown("**Latest blocking DQ results**")
            st.dataframe(
                query(
                    """
                    SELECT domain, dataset, check_name, passed, actual_value,
                           expected_value, checked_at
                    FROM shopvn.audit.data_quality_results
                    WHERE severity = 'BLOCKING'
                    ORDER BY checked_at DESC
                    LIMIT 100
                    """
                ),
                use_container_width=True,
                hide_index=True,
            )
        st.markdown("**Recent SFTP integrity status**")
        st.dataframe(
            query(
                """
                SELECT business_date, partner, file_name, status, byte_count,
                       error_message, checked_at
                FROM shopvn.audit.source_manifests
                ORDER BY checked_at DESC
                LIMIT 100
                """
            ),
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title="ShopVN Business Evidence", layout="wide")
    st.title("ShopVN — Four Use-Case Evidence Dashboard")
    st.caption(
        "Read-only serving data. Metrics are visible only after the relevant domain "
        "passes blocking DQ."
    )

    try:
        dates = dashboard_dates()
    except (
        Exception
    ) as exc:  # Streamlit must show an operational error rather than a blank page.
        st.error(f"Unable to read published serving data: {type(exc).__name__}")
        st.stop()

    if not dates:
        st.warning(
            "No DQ-approved Finance serving date is available yet. Run shopvn_daily first."
        )
        st.stop()

    selected_date = st.sidebar.selectbox(
        "Published business date",
        options=dates,
        format_func=lambda value: value.isoformat(),
        help=(
            "All tabs use this published date. The selection is limited to serving dates "
            "already available."
        ),
    )
    st.sidebar.caption("Finance earned revenue drives the available-date selector.")
    render_evidence_header(selected_date)

    finance, operations, marketing, product = st.tabs(
        [
            "UC1 Finance",
            "UC2 Operations",
            "UC3 Customer & Marketing",
            "UC4 Inventory & Product",
        ]
    )
    with finance:
        render_finance(selected_date)
    with operations:
        render_operations(selected_date)
    with marketing:
        render_customer_marketing(selected_date)
    with product:
        render_product(selected_date)
    render_operational_evidence()


if __name__ == "__main__":
    main()
