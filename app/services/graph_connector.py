from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime


@dataclass
class GraphResult:
    ok: bool
    status_code: int
    retry_after: int | None = None


class GraphConnector:
    """
    MVP용 Graph 커넥터 스텁.
    실제 API 키가 없을 때도 재시도/스로틀링 정책 동작을 검증할 수 있다.
    """

    def __init__(self) -> None:
        self.last_429_at: datetime | None = None
        self.recent_429_count: int = 0

    def call_with_backoff(self, max_attempts: int = 4) -> GraphResult:
        for attempt in range(1, max_attempts + 1):
            simulated = self._simulate_call()
            if simulated.ok:
                return simulated

            if simulated.status_code == 429:
                self.last_429_at = datetime.utcnow()
                self.recent_429_count += 1
                retry_after = simulated.retry_after or min(2**attempt, 15)
                time.sleep(retry_after / 10)
                continue

            return simulated

        return GraphResult(ok=False, status_code=429, retry_after=15)

    @staticmethod
    def _simulate_call() -> GraphResult:
        # 10% 확률로 429를 내서 백오프 경로를 검증한다.
        if random.random() < 0.1:
            return GraphResult(ok=False, status_code=429, retry_after=2)
        return GraphResult(ok=True, status_code=200)
