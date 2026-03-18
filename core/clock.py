from __future__ import annotations

import time


class WallClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000
