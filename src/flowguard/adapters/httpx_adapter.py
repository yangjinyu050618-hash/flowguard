"""HTTPX asynchronous client resilience adapter."""

from typing import Any, Callable, Coroutine
from flowguard.core.pipeline import FlowGuard
from flowguard.exceptions import TransientHTTPError, PermanentHTTPError


def check_http_response_status(resp: Any) -> None:
    """Classify HTTP response status code into TransientHTTPError or PermanentHTTPError."""
    if hasattr(resp, "status_code") and isinstance(resp.status_code, int):
        code = resp.status_code
        if code == 429 or 500 <= code < 600:
            msg = getattr(resp, "text", "")
            raise TransientHTTPError(code, msg)
        elif 400 <= code < 500:
            msg = getattr(resp, "text", "")
            raise PermanentHTTPError(code, msg)


class ResilientHTTPClient:
    """
    Wrap an HTTPX AsyncClient with FlowGuard resilience protection.

    Automatically maps downstream HTTP response status codes:
    - 429 and 5xx (500, 501, 502, 503, 504, etc.) -> TransientHTTPError (retried)
    - 4xx (400, 401, 403, 404, 422, etc.) -> PermanentHTTPError (fail fast)
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
            if self.raise_for_status:
                check_http_response_status(resp)
            return resp

        return await self.guard.execute(_call)

    async def get(self, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.get, url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.post, url, **kwargs)

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        return await self._execute_http(self._client.request, method, url, **kwargs)
