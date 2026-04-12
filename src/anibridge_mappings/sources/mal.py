"""Metadata provider that crawls MAL anime rankings."""

import asyncio
import os
from logging import getLogger
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlparse

import aiohttp
from anibridge.utils.limiter import Limiter

from anibridge_mappings.core.meta import SourceMeta, SourceType, normalize_titles
from anibridge_mappings.sources.base import CachedMetadataSource

log = getLogger(__name__)

mal_limiter = Limiter(rate=1, capacity=1)


class MalSource(CachedMetadataSource):
    """Collect MAL metadata by crawling the anime ranking endpoint."""

    API_URL = "https://api.myanimelist.net/v2/anime/ranking"
    DEFAULT_CLIENT_ID = "b11a4e1ead0db8142268906b4bb676a4"

    CACHE_VERSION = 2

    PAGE_LIMIT = 500
    MEDIA_TYPES: ClassVar[dict[str, SourceType]] = {
        "movie": SourceType.MOVIE,
        "music": SourceType.MOVIE,
        "cm": SourceType.MOVIE,
        "pv": SourceType.MOVIE,
        "tv": SourceType.TV,
        "ova": SourceType.TV,
        "ona": SourceType.TV,
        "special": SourceType.TV,
        "tv_special": SourceType.TV,
    }
    REQUEST_FIELDS = (
        "alternative_titles,start_date,media_type,num_episodes,average_episode_duration"
    )

    provider_key = "mal"
    cache_filename = "mal.json"

    def __init__(self) -> None:
        """Initialize the MAL metadata source."""
        super().__init__(concurrency=1)

    async def prepare(self) -> None:
        """Load the cached MAL dataset or crawl it when missing."""
        await super().prepare()
        if self._cache:
            return

        client_id = (os.environ.get("MAL_CLIENT_ID") or self.DEFAULT_CLIENT_ID).strip()
        if not client_id:
            raise RuntimeError("MAL metadata fetches require MAL_CLIENT_ID")

        async with aiohttp.ClientSession() as session:
            self._cache = await self._fetch_ranking_cache(session, client_id)

        self._persist_cache()

    async def _fetch_missing(
        self,
        entry_ids: list[tuple[str, str | None]],
    ) -> list[tuple[str, dict[str | None, SourceMeta] | None, bool]]:
        """Mark uncached MAL IDs as absent once the ranking crawl is complete."""
        return [(entry_id, None, True) for entry_id, _scope in entry_ids]

    async def _fetch_ranking_cache(
        self,
        session: aiohttp.ClientSession,
        client_id: str,
    ) -> dict[str, dict[str | None, SourceMeta] | None]:
        """Fetch the entire MAL anime ranking listing into the metadata cache."""
        cache: dict[str, dict[str | None, SourceMeta] | None] = {}
        offset = 0

        while True:
            payload = await self._request_ranking_page(session, client_id, offset)
            data = payload.get("data") or []

            for entry in data:
                node = entry["node"]
                cache[str(node["id"])] = self._build_scope_meta(node)

            next_url = (payload.get("paging") or {}).get("next")
            if not next_url:
                break
            offset = int(parse_qs(urlparse(next_url).query)["offset"][0])

        return cache

    async def _request_ranking_page(
        self,
        session: aiohttp.ClientSession,
        client_id: str,
        offset: int,
    ) -> dict[str, Any]:
        """Fetch one MAL ranking page with rate-limit handling."""
        params = {
            "ranking_type": "all",
            "limit": str(self.PAGE_LIMIT),
            "offset": str(offset),
            "fields": self.REQUEST_FIELDS,
        }
        headers = {
            "Accept": "application/json",
            "X-MAL-CLIENT-ID": client_id,
        }

        while True:
            await mal_limiter.acquire(asynchronous=True)
            async with session.get(
                self.API_URL, params=params, headers=headers
            ) as response:
                if response.status == 429:
                    retry = int(response.headers.get("Retry-After", "2"))
                    log.warning(
                        "MAL ranking rate limit hit at offset %s; sleeping %s",
                        offset,
                        retry,
                    )
                    await asyncio.sleep(retry + 1)
                    continue

                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as exc:
                    raise RuntimeError(
                        f"MAL ranking request failed at offset {offset}: {exc}"
                    ) from exc

                payload: dict[str, Any] = await response.json()
                return payload

    @classmethod
    def _build_scope_meta(cls, node: dict[str, Any]) -> dict[str | None, SourceMeta]:
        """Convert one MAL anime payload into scoped metadata."""
        alternative_titles = node.get("alternative_titles") or {}
        media_type = cls.MEDIA_TYPES.get(str(node.get("media_type")))
        episodes = node.get("num_episodes") or None
        if episodes is None and media_type is SourceType.MOVIE:
            episodes = 1

        titles = normalize_titles(
            (
                node.get("title"),
                alternative_titles.get("en"),
                alternative_titles.get("ja"),
            )
        )
        raw_duration = node.get("average_episode_duration")
        duration = round(raw_duration / 60) if isinstance(raw_duration, int) else None

        return {
            None: SourceMeta(
                type=media_type,
                episodes=episodes,
                duration=duration,
                start_year=(
                    int(node["start_date"][:4]) if node.get("start_date") else None
                ),
                titles=titles,
            )
        }
