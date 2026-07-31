"""In-memory fakes — exactly one per external system, honest semantics."""

from .clock import VirtualClock
from .datastore import FakeDatastore
from .fetcher import FakeFetcher
from .kv import FakeKeyValue
from .llm import FakeLLM
from .objectstore import FakeObjectStore
from .portal import FakePortalDiscoverer
from .queue import FakeQueue
from .search import FakeSearch

__all__ = [
    "FakeDatastore",
    "FakeFetcher",
    "FakeKeyValue",
    "FakeLLM",
    "FakeObjectStore",
    "FakePortalDiscoverer",
    "FakeQueue",
    "FakeSearch",
    "VirtualClock",
]
