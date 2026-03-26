from anibridge_mappings.utils.provider_ids import normalize_imdb_id


def test_normalize_imdb_id_accepts_raw_and_prefixed_values() -> None:
    assert normalize_imdb_id("123") == "tt0000123"
    assert normalize_imdb_id("tt0012345") == "tt0012345"


def test_normalize_imdb_id_rejects_invalid_values() -> None:
    assert normalize_imdb_id(None) is None
    assert normalize_imdb_id("") is None
    assert normalize_imdb_id("ttabc") is None
    assert normalize_imdb_id("1" * 10) is None
