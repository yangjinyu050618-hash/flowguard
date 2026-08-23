"""HTTPX asynchronous client resilience adapter."""

from typing import Any, Callable, Coroutine
from flowguard.core.pipeline import FlowGuard
from flowguard.exceptions import TransientHTTPError, PermanentHTTPError

TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


class ResilientHTTPClient:
    """
    Wrap an HTTPX AsyncClient with FlowGuard resilience protection.

    Automatically maps downstream HTTP response status codes to TransientHTTPError (retryable)
    and PermanentHTTPError (fatal client errors) when raise_for_status is True.
    """

    def __init__(self, client: Any, guard: FlowGuard, raise_for_status: bool = True) -> None:
        self._client = client
        self.guard = guard
        self.raise_for_status = raise_for_status

    async def _execute_http(
        self, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any
    ) -> Any:
        async def _call() -> Any:
            resp = await func(*args, **kwargs)
            if self.raise_for_status and hasattr(resp, "status_code"):
                code = resp.status_code
                if code in TRANSIENT_STATUS_CODES:
                    msg = getattr(resp, "text", "")
                    raise TransientHTTPError(code, msg)
                elif 400 <= code < 500:
                    msg = getattr(resp, "text", "")
                    raise PermanentHTTPError(code, msg)
            return resp

        return await self.guard.execute(_call)

    async def get(self, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.get, url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.post, url, **kwargs)

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.request, method, url, **kwargs)
