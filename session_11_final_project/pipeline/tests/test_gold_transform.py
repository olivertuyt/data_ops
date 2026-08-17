from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from transformation.gold import fact_voucher_daily


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
        .appName("shopvn-gold-transform-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_voucher_metrics_exclude_discount_anomaly(spark) -> None:
    orders = spark.createDataFrame(
        [
            (
                date(2026, 6, 7),
                "FLASH",
                "ORD-VALID",
                Decimal("90.00"),
                Decimal("10.00"),
                False,
                True,
            ),
            (
                date(2026, 6, 7),
                "FLASH",
                "ORD-ANOMALY",
                Decimal("0.00"),
                Decimal("120.00"),
                True,
                False,
            ),
        ],
        [
            "order_date",
            "voucher_code",
            "order_id",
            "eligible_net_revenue",
            "discount_amount",
            "is_discount_anomaly",
            "is_revenue_eligible",
        ],
    )
    vouchers = spark.createDataFrame([("FLASH", "FLASHSALE")], ["voucher_code", "campaign_id"])
    args = SimpleNamespace(run_id="run-1", start_date="2026-06-07", end_date="2026-06-07")
    row = fact_voucher_daily.build(
        Tables(
            {
                "polaris.silver.direct_orders": orders,
                "polaris.silver.vouchers": vouchers,
            }
        ),
        args,
    ).first()
    assert row["usage_count"] == 1
    assert row["net_revenue"] == Decimal("90.00")
    assert row["discount_cost"] == Decimal("10.00")
    assert row["anomaly_count"] == 1
