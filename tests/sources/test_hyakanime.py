import asyncio

from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.sources.hyakanime import HyakAnimeEntry, HyakAnimeSource


class _FakeResponse:
    def __init__(self, *, status: int, payload):
        self.status = status
        self.headers: dict[str, str] = {}
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        if self.status >= 400 and self.status != 404 and self.status != 429:
            raise RuntimeError("http error")

    async def read(self) -> bytes:
        import orjson

        return orjson.dumps(self._payload)


class _FakeSession:
    def __init__(self, responses: dict[str, list[_FakeResponse]], **kwargs):
        del kwargs
        self._responses = responses

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str):
        queue = self._responses.get(url)
        if not queue:
            raise AssertionError(f"Unexpected URL: {url}")
        return queue.pop(0)


def test_hyakanime_prepare_build_id_graph_and_collect_metadata(monkeypatch) -> None:
    source = HyakAnimeSource(concurrency=2)

    monkeypatch.setattr(HyakAnimeSource, "MIN_ENTRY_COUNT", 1)
    monkeypatch.setattr(HyakAnimeSource, "_load_cache", classmethod(lambda cls: {}))
    monkeypatch.setattr(
        HyakAnimeSource,
        "_persist_cache",
        classmethod(lambda cls, entries: None),
    )

    responses = {
        "https://api-v5.hyakanime.fr/explore/anime?page=1": [
            _FakeResponse(status=200, payload=[{"id": 1}, {"id": 2}])
        ],
        "https://api-v5.hyakanime.fr/explore/anime?page=2": [
            _FakeResponse(status=200, payload=[{"id": 2}, {"id": 3}])
        ],
        "https://api-v5.hyakanime.fr/explore/anime?page=3": [
            _FakeResponse(status=200, payload=[])
        ],
        "https://api-v5.hyakanime.fr/anime/1": [
            _FakeResponse(
                status=200,
                payload={
                    "id": 1,
                    "title": "One Piece",
                    "titleEN": "One Piece",
                    "romanji": "ONE PIECE",
                    "titleJP": "ONE PIECE",
                    "idAnilist": 21,
                    "NbEpisodes": 1157,
                    "EpAverage": 24,
                    "type": "TV",
                    "start": {"day": 20, "month": 10, "year": 1999},
                    "alt": ["OP", None, "  "],
                },
            )
        ],
        "https://api-v5.hyakanime.fr/anime/2": [
            _FakeResponse(
                status=200,
                payload={
                    "id": 2,
                    "title": "Movie Example",
                    "idAnilist": 22,
                    "NbEpisodes": 1,
                    "EpAverage": 95,
                    "type": "MOVIE",
                    "start": {"day": 1, "month": 1, "year": 2024},
                },
            )
        ],
        "https://api-v5.hyakanime.fr/anime/3": [
            _FakeResponse(
                status=200,
                payload={
                    "id": 3,
                    "title": "Unlinked Entry",
                    "NbEpisodes": 12,
                    "type": "TV",
                },
            )
        ],
    }

    monkeypatch.setattr(
        "anibridge_mappings.sources.hyakanime.aiohttp.ClientSession",
        lambda **kwargs: _FakeSession(responses, **kwargs),
    )

    asyncio.run(source.prepare())

    assert sorted(source._entries) == ["1", "2", "3"]

    graph = source.build_id_graph()
    assert graph.has_edge(("hyakanime", "1", None), ("anilist", "21", None))
    assert graph.has_edge(("hyakanime", "2", None), ("anilist", "22", None))
    assert not graph.has_edge(("hyakanime", "3", None), ("anilist", "3", None))

    store = asyncio.run(source.collect_metadata(graph))

    meta_1 = store.peek("hyakanime", "1", None)
    meta_2 = store.peek("hyakanime", "2", None)
    meta_3 = store.peek("hyakanime", "3", None)

    assert meta_1 is not None
    assert meta_1.episodes == 1157
    assert meta_1.duration == 24
    assert meta_1.start_year == 1999
    assert meta_1.type is not None and meta_1.type.value == "tv"
    assert meta_1.titles == ("One Piece", "OP")

    assert meta_2 is not None
    assert meta_2.episodes == 1
    assert meta_2.duration == 95
    assert meta_2.type is not None and meta_2.type.value == "movie"

    assert meta_3 is None


def test_hyakanime_collect_metadata_only_uses_entries_present_in_graph() -> None:
    source = HyakAnimeSource()
    source._prepared = True

    source._entries = {
        "11": HyakAnimeEntry.model_validate(
            {
                "id": 11,
                "title": "Mapped",
                "idAnilist": 111,
                "NbEpisodes": 12,
                "type": "TV",
            }
        ),
        "12": HyakAnimeEntry.model_validate(
            {
                "id": 12,
                "title": "Unmapped",
                "NbEpisodes": 24,
                "type": "TV",
            }
        ),
    }

    graph = IdMappingGraph()
    graph.add_edge(("hyakanime", "11", None), ("anilist", "111", None))

    store = asyncio.run(source.collect_metadata(graph))

    assert store.peek("hyakanime", "11", None) is not None
    assert store.peek("hyakanime", "12", None) is None


def test_hyakanime_entry_normalizes_float_episode_duration() -> None:
    entry = HyakAnimeEntry.model_validate(
        {
            "id": 99,
            "title": "Short Entry",
            "idAnilist": 199,
            "NbEpisodes": 10,
            "EpAverage": 1.42,
            "type": "TV",
        }
    )

    assert entry.average_duration == 1
    assert entry.to_source_meta().duration == 1
