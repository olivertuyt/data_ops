from __future__ import annotations

import os
import sys
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
JOBS_ROOT = PIPELINE_ROOT / "spark" / "jobs"
DAGS_ROOT = PIPELINE_ROOT / "dags"
sys.path.insert(0, str(JOBS_ROOT))

os.environ.setdefault("SHOPVN_TIMEZONE", "Asia/Ho_Chi_Minh")
