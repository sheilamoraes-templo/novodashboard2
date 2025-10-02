from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class KPI:
    users: float
    sessions: float
    pageviews: float
    avg_session_duration: Optional[float] = None


