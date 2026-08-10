"""Deterministic sample ad-event data — the same logical_date always yields the
same events, so reruns and backfills are reproducible.

Click rates differ per campaign (retargeting clicks most, brand awareness least),
which mirrors real ad ops and gives each campaign a distinct CTR."""

from __future__ import annotations

from datetime import datetime, timedelta

CAMPAIGNS = ["brand_awareness", "retargeting", "prospecting"]

# Click every N impressions → CTR 0.10 / 0.50 / 0.30 over 10 impressions each.
_CLICK_EVERY = {"brand_awareness": 10, "retargeting": 2, "prospecting": 4}


def generate_ad_events(logical_date: datetime, n_events: int = 30) -> list[dict]:
    """One row per ad impression; some impressions are also clicks."""
    base = int(logical_date.strftime("%Y%m%d"))
    seen = {campaign: 0 for campaign in CAMPAIGNS}
    events = []
    for i in range(n_events):
        campaign = CAMPAIGNS[i % len(CAMPAIGNS)]
        rank = seen[campaign]
        seen[campaign] += 1
        is_click = rank % _CLICK_EVERY[campaign] == 0
        events.append(
            {
                "event_id": base * 1000 + i,
                "campaign": campaign,
                "is_click": is_click,
                "cost_usd": round(0.10 + (0.40 if is_click else 0.0) + (i % 3) * 0.05, 2),
                "event_ts": (logical_date + timedelta(minutes=i)).isoformat(),
            }
        )
    return events


def aggregate_campaigns(events: list[dict]) -> list[dict]:
    """Roll raw impressions up to one metrics row per campaign."""
    agg: dict[str, dict] = {}
    for event in events:
        row = agg.setdefault(
            event["campaign"],
            {"campaign": event["campaign"], "impressions": 0, "clicks": 0, "spend_usd": 0.0},
        )
        row["impressions"] += 1
        row["clicks"] += 1 if event["is_click"] else 0
        row["spend_usd"] += event["cost_usd"]
    rows = []
    for row in agg.values():
        row["spend_usd"] = round(row["spend_usd"], 2)
        row["ctr"] = round(row["clicks"] / row["impressions"], 4) if row["impressions"] else 0.0
        rows.append(row)
    return sorted(rows, key=lambda r: r["campaign"])
