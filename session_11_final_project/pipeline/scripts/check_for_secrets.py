"""Fail CI on known fixture secrets or service URLs embedded in Python logic."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = [ROOT / "spark" / "jobs", ROOT / "dags", ROOT / "monitoring"]
PATTERNS = {
    "known_fixture_secret": re.compile(
        r"readonly123|sftp_readonly_2026|shopvn-logistics-key-2026|minioadmin",
        re.IGNORECASE,
    ),
    # A scheme immediately followed by a host is a concrete embedded endpoint. Scheme
    # validation and f-strings assembled solely from validated config remain allowed.
    "service_url_in_python": re.compile(
        r"(?:https?://|jdbc:postgresql://)[A-Za-z0-9]", re.IGNORECASE
    ),
}


def main() -> None:
    failures: list[str] = []
    for root in SCAN_ROOTS:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            for name, pattern in PATTERNS.items():
                if pattern.search(text):
                    failures.append(f"{path.relative_to(ROOT)}: {name}")
    if failures:
        raise SystemExit("\n".join(failures))
    print("PASS: no known secrets or service URLs embedded in Python logic")


if __name__ == "__main__":
    main()
