"""HyakAnime bulk source for ID mappings and metadata."""

import asyncio
from datetime import UTC, datetime
from logging import getLogger
from pathlib import Path
from typing import Any, ClassVar

import aiohttp
import orjson
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.meta import (
    MetaStore,
    SourceMeta,
    SourceType,
    normalize_titles,
)
from anibridge_mappings.sources.base import IdMappingSource, MetadataSource

log = getLogger(__name__)


class HyakAnimeDate(BaseModel):
    """Partial date payload returned by HyakAnime."""

    day: int | None = None
    month: int | None = None
    year: int | None = None

    model_config = ConfigDict(extra="ignore")

    def to_datetime(self) -> datetime | None:
        """Convert the partial date into a valid UTC datetime when possible."""
        if self.year is None or self.month is None or self.day is None:
            return None
        try:
            return datetime(self.year, self.month, self.day, tzinfo=UTC)
        except ValueError:
            return None


class HyakAnimeEntry(BaseModel):
    """Anime payload returned by `anime/{id}`."""

    id: int
    title: str | None = None
    alt: list[str] = Field(default_factory=list)
    type: str | None = None
    status: int | None = None
    title_en: str | None = Field(default=None, alias="titleEN")
    title_jp: str | None = Field(default=None, alias="titleJP")
    romaji: str | None = Field(default=None, alias="romanji")
    id_mal: int | None = Field(default=None, alias="idMAL")
    id_anilist: int | None = Field(default=None, alias="idAnilist")
    total_episodes: int | None = Field(default=None, alias="NbEpisodes")
    average_duration: float | int | None = Field(default=None, alias="EpAverage")
    start: HyakAnimeDate | None = None

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @field_validator("alt", mode="before")
    @classmethod
    def _normalize_alt_titles(cls, value: object) -> list[str]:
        """Drop null or blank alternate titles from the payload."""
        if value is None or not isinstance(value, list):
            return []
        return [
            item.strip() for item in value if isinstance(item, str) and item.strip()
        ]

    @field_validator("average_duration", mode="before")
    @classmethod
    def _normalize_average_duration(cls, value: object) -> int | None:
        """Coerce HyakAnime episode durations to an integer minute count."""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return round(value) if value > 0 else None
        if isinstance(value, str):
            try:
                parsed = float(value)
            except ValueError:
                return None
            return round(parsed) if parsed > 0 else None
        return None

    @property
    def provider_id(self) -> str:
        """Return the HyakAnime identifier as a string."""
        return str(self.id)

    @property
    def anilist_provider_id(self) -> str | None:
        """Return the linked AniList identifier as a string when present."""
        if self.id_anilist is None or self.id_anilist <= 0:
            return None
        return str(self.id_anilist)

    @property
    def parsed_type(self) -> SourceType | None:
        """Return the coarse AniBridge source type for the entry."""
        raw_type = (self.type or "").strip().upper()
        if raw_type in {"MOVIE", "MUSIC"}:
            return SourceType.MOVIE
        if raw_type in {"TV", "OVA", "ONA", "SPECIAL"}:
            return SourceType.TV
        return None

    @property
    def release_year(self) -> int | None:
        """Return the best-effort release year from the start date."""
        if self.start is None:
            return None
        parsed = self.start.to_datetime()
        return parsed.year if parsed is not None else self.start.year

    @property
    def titles(self) -> tuple[str, ...]:
        """Return deduplicated titles suitable for metadata matching."""
        return normalize_titles(
            (
                self.title,
                self.title_en,
                self.romaji,
                self.title_jp,
                *self.alt,
            )
        )

    def to_source_meta(self) -> SourceMeta:
        """Convert the HyakAnime entry into generic source metadata."""
        duration = (
            self.average_duration
            if self.average_duration is not None and self.average_duration > 0
            else None
        )
        episodes = (
            self.total_episodes
            if self.total_episodes is not None and self.total_episodes > 0
            else None
        )
        return SourceMeta(
            type=self.parsed_type,
            episodes=episodes,
            duration=duration,
            start_year=self.release_year,
            titles=self.titles,
        )


class HyakAnimeSource(IdMappingSource, MetadataSource):
    """Fetch the HyakAnime catalog and link it to AniList."""

    API_ROOT: ClassVar[str] = "https://api-v5.hyakanime.fr/"
    CACHE_VERSION: ClassVar[int] = 1
    CACHE_PATH: ClassVar[Path] = Path("data/meta/hyakanime.json")
    MIN_ENTRY_COUNT: ClassVar[int] = 1000
    provider_key: ClassVar[str] = "hyakanime"

    def __init__(self, *, concurrency: int = 16) -> None:
        """Initialize the HyakAnime source."""
        self._concurrency = max(1, concurrency)
        self._entries: dict[str, HyakAnimeEntry] = {}
        self._prepared = False

    async def prepare(self) -> None:
        """Fetch the HyakAnime catalog, with cache fallback on transient failures."""
        cached_entries = self._load_cache()
        try:
            entries = await self._fetch_entries()
            if len(entries) < self.MIN_ENTRY_COUNT:
                raise RuntimeError(
                    "HyakAnime catalog fetch returned too few entries "
                    f"({len(entries)} < {self.MIN_ENTRY_COUNT})"
                )
            self._entries = entries
            self._persist_cache(entries)
        except Exception:
            if not cached_entries:
                raise
            log.warning(
                "HyakAnime live fetch failed, falling back to %d cached entries",
                len(cached_entries),
                exc_info=True,
            )
            self._entries = cached_entries
        self._prepared = True

    async def collect_metadata(self, id_graph: IdMappingGraph) -> MetaStore:
        """Return metadata for HyakAnime entries linked into the ID graph."""
        store = MetaStore()
        for entry_id, _scope in self._eligible_ids(id_graph):
            entry = self._entries.get(entry_id)
            if entry is None:
                continue
            store.set("hyakanime", entry_id, entry.to_source_meta())
        return store

    def build_id_graph(self) -> IdMappingGraph:
        """Build the HyakAnime-to-AniList equivalence graph."""
        graph = IdMappingGraph()
        for entry in self._require_entries().values():
            anilist_id = entry.anilist_provider_id
            if anilist_id is None:
                continue
            graph.add_equivalence_class(
                [
                    ("hyakanime", entry.provider_id, None),
                    ("anilist", anilist_id, None),
                ]
            )
        return graph

    def _require_entries(self) -> dict[str, HyakAnimeEntry]:
        """Return cached entries or raise if `prepare` was skipped."""
        if not self._prepared:
            raise RuntimeError("Source not initialized.")
        return self._entries

    async def _fetch_entries(self) -> dict[str, HyakAnimeEntry]:
        """Fetch the full HyakAnime catalog and return it keyed by ID."""
        async with aiohttp.ClientSession(**self._session_kwargs()) as session:
            entry_ids = await self._fetch_catalog_ids(session)
            entries = await self._fetch_entry_details(session, entry_ids)

        return {entry.provider_id: entry for entry in entries}

    def _session_kwargs(self) -> dict[str, Any]:
        """Return session settings for HyakAnime requests."""
        return {
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "AniBridgeMappings/3",
            },
            "timeout": aiohttp.ClientTimeout(total=30),
        }

    async def _fetch_catalog_ids(self, session: aiohttp.ClientSession) -> list[str]:
        """Fetch paginated HyakAnime catalog IDs from the explore endpoint."""
        page = 1
        entry_ids: list[str] = []
        seen_ids: set[str] = set()

        while True:
            payload = await self._request_json(
                session,
                f"explore/anime?page={page}",
                label=f"catalog page {page}",
            )
            if not payload:
                break
            if not isinstance(payload, list):
                raise RuntimeError(
                    f"Unexpected HyakAnime catalog payload on page {page}"
                )

            for item in payload:
                if not isinstance(item, dict):
                    continue
                candidate = self._normalize_numeric(item.get("id"))
                if candidate is None or candidate in seen_ids:
                    continue
                seen_ids.add(candidate)
                entry_ids.append(candidate)

            page += 1

        log.info(
            "Fetched %d HyakAnime catalog IDs across %d pages",
            len(entry_ids),
            max(page - 1, 0),
        )
        return entry_ids

    async def _fetch_entry_details(
        self,
        session: aiohttp.ClientSession,
        entry_ids: list[str],
    ) -> list[HyakAnimeEntry]:
        """Fetch detailed HyakAnime entries concurrently."""
        semaphore = asyncio.Semaphore(self._concurrency)
        tasks = [
            asyncio.create_task(
                self._fetch_entry_with_semaphore(session, semaphore, entry_id)
            )
            for entry_id in entry_ids
        ]
        results = await asyncio.gather(*tasks)
        return [entry for entry in results if entry is not None]

    async def _fetch_entry_with_semaphore(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        entry_id: str,
    ) -> HyakAnimeEntry | None:
        """Fetch one detailed HyakAnime entry while respecting concurrency."""
        async with semaphore:
            payload = await self._request_json(
                session,
                f"anime/{entry_id}",
                label=f"anime {entry_id}",
                allow_not_found=True,
            )
            if payload is None:
                return None
            try:
                return HyakAnimeEntry.model_validate(payload)
            except ValidationError:
                log.warning(
                    "Skipping invalid HyakAnime entry %s",
                    entry_id,
                    exc_info=True,
                )
                return None

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        *,
        label: str,
        allow_not_found: bool = False,
    ) -> Any:
        """Perform a HyakAnime request with light retry and rate-limit handling."""
        delay = 1.0
        url = self.API_ROOT + path.lstrip("/")

        for attempt in range(3):
            try:
                async with session.get(url) as response:
                    if response.status == 429 and attempt < 2:
                        retry_after = response.headers.get("Retry-After")
                        sleep_for = float(retry_after) if retry_after else delay
                        log.warning(
                            "HyakAnime rate limit hit for %s; sleeping %.1fs",
                            label,
                            sleep_for,
                        )
                        await asyncio.sleep(max(sleep_for, 0))
                        delay *= 2
                        continue

                    if response.status in {500, 502, 503, 504} and attempt < 2:
                        log.warning(
                            "HyakAnime transient error %s for %s; retrying",
                            response.status,
                            label,
                        )
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue

                    if response.status == 404 and allow_not_found:
                        return None

                    response.raise_for_status()
                    body = await response.read()
                    if not body:
                        return None
                    return orjson.loads(body)
            except aiohttp.ClientError:
                if attempt >= 2:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

        raise RuntimeError(f"HyakAnime request failed for {label}")

    def _eligible_ids(self, id_graph: IdMappingGraph) -> list[tuple[str, str | None]]:
        """Return HyakAnime IDs in the graph, preserving the metadata contract."""
        ids: set[tuple[str, str | None]] = set()
        for provider, entry_id, scope in id_graph.nodes():
            if provider == "hyakanime":
                ids.add((entry_id, scope))
        return sorted(ids)

    @classmethod
    def _load_cache(cls) -> dict[str, HyakAnimeEntry]:
        """Load the local HyakAnime cache if present and version-compatible."""
        path = cls.CACHE_PATH
        if not path.exists():
            return {}
        payload = orjson.loads(path.read_bytes())
        if not isinstance(payload, dict) or payload.get("version") != cls.CACHE_VERSION:
            return {}
        raw_entries = payload.get("entries") or []
        entries: dict[str, HyakAnimeEntry] = {}
        for raw_entry in raw_entries:
            try:
                entry = HyakAnimeEntry.model_validate(raw_entry)
            except ValidationError:
                continue
            entries[entry.provider_id] = entry
        return entries

    @classmethod
    def _persist_cache(cls, entries: dict[str, HyakAnimeEntry]) -> None:
        """Persist the HyakAnime catalog cache to disk."""
        cls.CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": cls.CACHE_VERSION,
            "meta": {
                "generated_on": datetime.now(UTC).isoformat(),
                "length": len(entries),
            },
            "entries": [
                entry.model_dump(by_alias=True, exclude_none=True)
                for entry in sorted(entries.values(), key=lambda item: item.id)
            ],
        }
        cls.CACHE_PATH.write_bytes(orjson.dumps(payload))

    @staticmethod
    def _normalize_numeric(value: object) -> str | None:
        """Normalize a numeric identifier into a decimal string."""
        text = str(value).strip()
        if not text.isdigit():
            return None
        return text
