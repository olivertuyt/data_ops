from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_all_contracts_are_valid_yaml() -> None:
    files = sorted((ROOT / "contracts").rglob("*.yaml"))
    assert files
    for path in files:
        assert yaml.safe_load(path.read_text(encoding="utf-8"))


def test_gold_contract_has_every_published_model() -> None:
    contract = yaml.safe_load(
        (ROOT / "contracts" / "models" / "gold_models.yaml").read_text(encoding="utf-8")
    )
    assert set(contract["models"]) == {
        "fact_daily_revenue",
        "fact_customer_daily",
        "fact_voucher_daily",
        "fact_delivery_daily",
        "fact_return_daily",
        "fact_product_rating_daily",
        "fact_inventory_eod",
        "fact_product_channel_daily",
        "dim_customer_scd2",
    }
