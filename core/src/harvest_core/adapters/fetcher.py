"""httpx fetcher with byte caps and escape-preserving percent-encoding.

Government sites publish URLs containing spaces; existing escapes must
be preserved, never double-encoded (old spec §6.1). HTML-entity
unescaping (`&amp;`) belongs to link extraction, not here.
"""

from __future__ import annotations

from urllib.parse import quote

import httpx

from ..errors import FetchError, UnreachableHost
from ..ports import FetchResponse

USER_AGENT = "harvest-fleet/1.0 (+public-records corpus builder)"

# '%' in safe preserves existing escapes; reserved characters keep their meaning.
_SAFE = ":/?#[]@!$&'()*+,;=%"


def encode_url(url: str) -> str:
    return quote(url, safe=_SAFE)


class HttpxFetcher:
    def __init__(self, client: httpx.Client | None = None, timeout: float = 60.0) -> None:
        self._client = client or httpx.Client(
            timeout=timeout, follow_redirects=True, headers={"User-Agent": USER_AGENT}
        )

    def get(self, url: str, max_bytes: int) -> FetchResponse:
        encoded = encode_url(url)
        try:
            with self._client.stream("GET", encoded) as resp:
                content = b""
                if resp.status_code == 200:
                    for chunk in resp.iter_bytes():
                        content += chunk
                        if len(content) >= max_bytes:
                            content = content[:max_bytes]
                            break
                return FetchResponse(
                    status=resp.status_code,
                    content=content,
                    content_type=resp.headers.get("content-type"),
                )
        except (httpx.ConnectError, httpx.UnsupportedProtocol) as exc:
            raise UnreachableHost(str(exc)) from exc
        except httpx.HTTPError as exc:
            raise FetchError(str(exc)) from exc
