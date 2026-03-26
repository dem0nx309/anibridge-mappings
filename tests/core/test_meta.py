from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType


def test_source_meta_to_from_dict_round_trip() -> None:
    meta = SourceMeta(type=SourceType.TV, episodes=12, duration=24, start_year=2024)

    payload = meta.to_dict()
    parsed = SourceMeta.from_dict(payload)

    assert parsed == meta


def test_source_meta_to_dict_excludes_none_when_requested() -> None:
    meta = SourceMeta(type=SourceType.MOVIE)

    assert meta.to_dict(include_none=False) == {"type": "movie"}


def test_meta_store_get_update_set_peek_and_merge() -> None:
    store = MetaStore()

    created = store.get("anidb", "1", "R")
    assert created == SourceMeta()

    updated = store.update("anidb", "1", "R", episodes=13, type=SourceType.TV)
    assert updated.episodes == 13
    assert updated.type == SourceType.TV

    replacement = SourceMeta(type=SourceType.MOVIE, episodes=1, duration=100)
    store.set("anidb", "1", replacement, "R")
    assert store.peek("anidb", "1", "R") == replacement

    other = MetaStore()
    other.set("mal", "22", SourceMeta(type=SourceType.TV, episodes=24), None)
    store.merge(other)

    assert len(store) == 2
    keys = {key for key, _meta in store.items()}
    assert ("anidb", "1", "R") in keys
    assert ("mal", "22", None) in keys
