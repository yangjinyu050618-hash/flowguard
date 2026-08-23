"""HTTPX asynchronous client resilience adapter."""

from typing import Any
from flowguard.core.pipeline import FlowGuard


class ResilientHTTPClient:
    """Wrap an HTTPX AsyncClient with FlowGuard resilience protection."""

    def __init__(self, client: Any, guard: FlowGuard) -> None:
        self._client = client
        self.guard = guard

    async def get(self, url: str, **kwargs: Any) -> Any:
        return await self.guard.execute(self._client.get, url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Any:
        return await self.guard.execute(self._client.post, url, **kwargs)

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        return await self.guard.execute(self._client.request, method, url, **kwargs)
