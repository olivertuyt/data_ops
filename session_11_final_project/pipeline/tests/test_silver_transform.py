from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from transformation.silver import customers, direct_orders, marketplace_sales


class Tables:
    def __init__(self, mapping):
        self.mapping = mapping

    def table(self, name):
        return self.mapping[name]


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("shopvn-transform-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_customer_transform_removes_plaintext_pii_and_preserves_null_phone(spark) -> None:
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("customer_id", T.LongType(), False),
            T.StructField("full_name", T.StringType(), False),
            T.StructField("phone", T.StringType(), True),
            T.StructField("email", T.StringType(), False),
            T.StructField("city", T.StringType(), False),
            T.StructField("district", T.StringType(), False),
            T.StructField("ward", T.StringType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("date_of_birth", T.DateType(), True),
            T.StructField("loyalty_tier", T.StringType(), False),
            T.StructField("created_at", T.TimestampType(), False),
            T.StructField("_run_id", T.StringType(), False),
        ]
    )
    source = spark.createDataFrame(
        [
            (
                1,
                "Test Customer",
                None,
                "customer@example.invalid",
                "HCM",
                "District 1",
                "Ward 1",
                "female",
                date(1990, 1, 1),
                "gold",
                datetime(2026, 6, 1),
                "run-1",
            )
        ],
        schema,
    )
    result = customers.build(Tables({"polaris.bronze.customers": source}), pii_salt="salt")
    assert not {"full_name", "phone", "email", "ward"} & set(result.columns)
    assert result.first()["phone_hash"] is None


def test_direct_revenue_excludes_discount_anomaly_and_deducts_refund(spark) -> None:
    orders = spark.createDataFrame(
        [
            (
                "ORD-1",
                1,
                "SAVE10",
                "web",
                date(2026, 6, 1),
                "returned",
                Decimal("100.00"),
                Decimal("10.00"),
                Decimal("10.00"),
                Decimal("100.00"),
                "card",
                "refunded",
                datetime(2026, 6, 1),
                "run-1",
            ),
            (
                "ORD-2",
                1,
                None,
                "app",
                date(2026, 6, 1),
                "delivered",
                Decimal("100.00"),
                Decimal("0.00"),
                Decimal("110.00"),
                Decimal("0.00"),
                "card",
                "paid",
                datetime(2026, 6, 1),
                "run-1",
            ),
        ],
        [
            "order_id",
            "customer_id",
            "voucher_code",
            "channel",
            "order_date",
            "status",
            "subtotal",
            "shipping_fee",
            "discount_amount",
            "total_amount",
            "payment_method",
            "payment_status",
            "created_at",
            "_run_id",
        ],
    )
    returns = spark.createDataFrame(
        [(1, "ORD-1", "damaged", "refunded", Decimal("25.00"), datetime(2026, 6, 2))],
        ["return_id", "order_id", "reason", "return_status", "refund_amount", "created_at"],
    )
    result = direct_orders.build(
        Tables(
            {
                "polaris.bronze.orders": orders,
                "polaris.silver.returns": returns,
            }
        )
    )
    rows = {row["order_id"]: row for row in result.collect()}
    assert rows["ORD-1"]["eligible_net_revenue"] == Decimal("75.00")
    assert rows["ORD-2"]["is_discount_anomaly"] is True
    assert rows["ORD-2"]["eligible_net_revenue"] == Decimal("0.00")


def test_marketplace_revenue_matches_source_binary_float_floor(
    spark, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SFTP_PARTNERS", "tiktok")
    source = spark.createDataFrame(
        [
            (
                "TT-1",
                "1",
                "SKU-1",
                "tiktok",
                "2026-06-01",
                "4",
                "959500",
                "0",
                "0.07",
                "3569339",
                "2026-06-16",
                "completed",
                "/landing/tiktok.csv.ready",
                "row-hash",
                "run-1",
                datetime(2026, 6, 1),
            )
        ],
        [
            "external_order_id",
            "shopvn_product_id",
            "seller_sku",
            "partner",
            "order_date",
            "quantity_sold",
            "sale_price",
            "platform_discount",
            "commission_rate",
            "net_revenue",
            "settlement_date",
            "status",
            "_source_file",
            "_source_row_hash",
            "_run_id",
            "_ingested_at",
        ],
    )

    row = marketplace_sales.build(Tables({"polaris.bronze.marketplace_sales": source})).first()

    assert row["calculated_net_revenue"] == Decimal("3569339.00")
    assert row["is_net_revenue_mismatch"] is False
