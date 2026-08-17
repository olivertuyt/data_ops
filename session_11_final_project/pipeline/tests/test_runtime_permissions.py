from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_landing_volumes_are_initialized_for_airflow_user() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    common = compose["x-airflow-common"]
    init = compose["services"]["landing-init"]

    assert common["depends_on"]["landing-init"]["condition"] == ("service_completed_successfully")
    assert init["user"] == "0:0"
    assert "chown -R 50000:0" in init["command"][-1]
    assert "api-landing:/landing/api" in init["volumes"]
    assert "marketplace-landing:/landing/marketplace" in init["volumes"]
