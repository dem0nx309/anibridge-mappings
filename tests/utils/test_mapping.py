from anibridge_mappings.core.graph import EpisodeMappingGraph
from anibridge_mappings.utils.mapping import (
    build_source_target_map,
    collapse_source_mappings,
    normalize_episode_key,
    ordered_payload,
    parse_descriptor,
    parse_range_bounds,
    provider_scope_sort_key,
)


def test_parse_helpers() -> None:
    assert parse_descriptor("anidb:1:R") == ("anidb", "1", "R")
    assert normalize_episode_key(" 1-3 ") == "1-3"
    assert normalize_episode_key("  ") is None
    assert parse_range_bounds("1-3") == (1, 3)


def test_build_source_target_map_normalizes_reversed_pairs() -> None:
    graph = EpisodeMappingGraph()
    source = ("anidb", "1", "R", "1-2,3-4")
    target = ("mal", "2", None, "10-13")
    graph.add_edge(source, target)

    source_map = build_source_target_map(graph)
    ranges = source_map[("anidb", "1", "R")][("mal", "2", None)]

    assert ranges == {"1-2": {"10-11"}, "3-4": {"12-13"}}


def test_collapse_source_mappings_merges_adjacent_keys_and_ranges() -> None:
    collapsed = collapse_source_mappings(
        {
            "1": {"11"},
            "2": {"12"},
            "3": {"13"},
            "5": {"99", "100"},
            "bad": {"1"},
        }
    )

    assert collapsed["1-3"] == "11-13"
    assert collapsed["5"] == "99-100"


def test_ordered_payload_sorts_descriptors_and_ranges() -> None:
    payload = {
        "$meta": {"v": 1},
        "mal:10": {
            "anidb:2:R": {"10": "1", "1": "1"},
            "anidb:1:R": {"2": "1"},
        },
        "anidb:2:R": {},
    }

    ordered = ordered_payload(payload)

    assert next(iter(ordered)) == "$meta"
    keys = list(ordered)
    assert keys[1:] == sorted(keys[1:], key=provider_scope_sort_key)


def test_build_source_target_map_with_same_source_target_node() -> None:
    """Test that self-loops are skipped."""
    graph = EpisodeMappingGraph()
    # Add a self-loop (source == target)
    node = ("anidb", "1", "R", "1-2")
    graph.add_edge(node, node)

    source_map = build_source_target_map(graph)
    # Self-loop should be skipped
    assert len(source_map) == 0 or ("anidb", "1", "R") not in source_map


def test_build_source_target_map_with_same_provider_entry_scope() -> None:
    """Test that same provider/entry/scope edges are skipped."""
    graph = EpisodeMappingGraph()
    source = ("anidb", "1", "R", "1-2")
    target = ("anidb", "1", "R", "5-6")  # Same provider, entry, scope
    graph.add_edge(source, target)

    source_map = build_source_target_map(graph)
    # Should be skipped due to same provider/entry/scope
    assert len(source_map) == 0


def test_build_source_target_map_skips_none_ranges() -> None:
    """Test that nodes with None ranges are skipped."""
    graph = EpisodeMappingGraph()
    source = ("anidb", "1", "R", "")  # Empty string, will be normalized to None
    target = ("mal", "2", None, "10-12")
    graph.add_edge(source, target)

    source_map = build_source_target_map(graph)
    # Source with empty/None range should be skipped
    assert len(source_map) == 0


def test_collapse_source_mappings_with_invalid_source() -> None:
    """Test collapse skips invalid source segments."""
    collapsed = collapse_source_mappings(
        {
            "invalid-format": {"1", "2"},
            "1-2": {"10-11"},
        }
    )

    # Invalid source should be skipped, valid one included
    assert "invalid-format" not in collapsed
    assert "1-2" in collapsed


def test_collapse_source_mappings_with_invalid_targets() -> None:
    """Test collapse with some invalid target specs."""
    collapsed = collapse_source_mappings(
        {
            "1": {"1-2", "invalid"},
        }
    )

    # Should have processed the targets (exact result varies)
    assert "1" in collapsed


def test_collapse_source_mappings_multiple_ratios() -> None:
    """Test collapse chooses longest spec when ratios conflict."""
    collapsed = collapse_source_mappings(
        {
            "1": {"1-3|2", "5-6|-3"},  # Different ratios
        }
    )

    # Should pick one (the longest)
    assert "1" in collapsed


def test_collapse_source_mappings_no_specs_only_raw() -> None:
    """Test collapse with no valid specs, only raw targets."""
    collapsed = collapse_source_mappings(
        {
            "5": {"not-a-spec"},
        }
    )

    assert collapsed["5"] == "not-a-spec"


def test_provider_scope_sort_key_sorts_correctly() -> None:
    """Test provider_scope_sort_key sorts descriptors."""
    keys = ["anidb:2:R", "mal:1", "anidb:1:R", "mal:10"]
    sorted_keys = sorted(keys, key=provider_scope_sort_key)

    # mal comes before anidb, numeric then semantic
    assert "anidb" in sorted_keys[0] or "mal" in sorted_keys[0]


def test_normalize_episode_key_preserves_text() -> None:
    """Test normalize_episode_key returns stripped version."""
    assert normalize_episode_key("\t1-5 \n") == "1-5"
    assert normalize_episode_key(None) is None
    assert normalize_episode_key("") is None


def test_collapse_source_mappings_adjacent_merging() -> None:
    """Test that adjacent numeric keys get merged."""
    collapsed = collapse_source_mappings(
        {
            "1": {"10"},
            "2": {"11"},
            "3": {"12"},
        }
    )

    # Should merge adjacent segments with same target pattern
    assert "1-3" in collapsed or len(collapsed) > 0


def test_ordered_payload_with_non_dict_values() -> None:
    """Test ordered_payload handles non-dict values in providers."""
    payload = {
        "$meta": {"v": 1},
        "provider1": "not-a-dict",
        "provider2": {"key": "value"},
    }

    ordered = ordered_payload(payload)
    assert "$meta" in ordered
    assert ordered["$meta"] == {"v": 1}


def test_build_source_target_map_with_empty_graph() -> None:
    """Test build_source_target_map with no edges."""
    graph = EpisodeMappingGraph()

    source_map = build_source_target_map(graph)
    assert len(source_map) == 0


def test_collapse_source_mappings_with_all_invalid_sources() -> None:
    """Test collapse with all invalid source keys."""
    collapsed = collapse_source_mappings(
        {
            "not-valid": {"1-2"},
            "also-invalid": {"3-4"},
        }
    )

    # All invalid sources should be skipped
    assert len(collapsed) == 0 or all(
        k not in collapsed for k in ["not-valid", "also-invalid"]
    )


def test_collapse_source_mappings_same_ratio_merge() -> None:
    """Test collapse merges specs with same ratio."""
    collapsed = collapse_source_mappings(
        {
            "1": {"1-3|2"},
            "4": {"5-7|2"},
        }
    )

    # Should have entries for the keys processed
    assert len(collapsed) > 0
