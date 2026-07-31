"""Real CensusSource: the Census place-by-county reference file.

Downloaded from census.gov with 3 attempts and exponential backoff at a
120-second timeout — that host is routinely slow from cloud networks
(old spec §2.1). Parsing is unit-tested against fixtures; the download
path is live-only.
"""

from __future__ import annotations

import logging
import time

import httpx

from .census import CensusPlace, CensusState

log = logging.getLogger(__name__)

PLACES_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_places_by_county2020.txt"

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


def parse_places_file(text: str, state: str) -> CensusState:
    """Parse the pipe-delimited national places file for one state.

    Columns: STATE|STATEFP|PLACEFP|PLACENAME|TYPE|FUNCSTAT|COUNTY
    Counties come from the distinct COUNTY column values — including
    legacy unsplit 'A County, B County' strings, which are inserted and
    later excluded from scope by the comma rule, never repaired here.
    """
    places: list[CensusPlace] = []
    seen_counties: set[str] = set()
    seen_places: set[str] = set()
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 7 or parts[0] != state:
            continue
        _, _, place_fp, place_name, _, _, county = (p.strip() for p in parts[:7])
        if place_name.upper() == "PLACENAME":
            continue  # header
        if county and county not in seen_counties:
            seen_counties.add(county)
            places.append(CensusPlace(name=county, level="county"))
        if place_name and place_name not in seen_places:
            seen_places.add(place_name)
            places.append(
                CensusPlace(
                    name=place_name,
                    level="city",
                    fips=place_fp or None,
                    parent_name=county or None,
                )
            )
    return CensusState(state_name=STATE_NAMES.get(state, state), places=places)


class CensusGovSource:
    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=120.0, follow_redirects=True)
        self._cached: str | None = None

    def _download(self) -> str:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                resp = self._client.get(PLACES_URL)
                resp.raise_for_status()
                return resp.text
            except httpx.HTTPError as exc:
                last_error = exc
                time.sleep(2**attempt)
        raise RuntimeError(f"census download failed after 3 attempts: {last_error}")

    def load_state(self, state: str) -> CensusState:
        if self._cached is None:
            self._cached = self._download()
        return parse_places_file(self._cached, state)
