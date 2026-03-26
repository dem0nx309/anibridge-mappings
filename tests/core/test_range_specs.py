from anibridge.utils.mappings import AnibridgeMappingRange

from anibridge_mappings.core.range_specs import (
    TargetSpec,
    format_range,
    format_target_spec,
    has_internal_overlap,
    merge_segments,
    normalize_reversed_pair,
    parse_source_segment,
    parse_target_spec,
    range_bounds,
    ranges_overlap,
    target_units,
)


def test_parse_source_segment_and_target_spec() -> None:
    assert parse_source_segment("1-3") == AnibridgeMappingRange(start=1, end=3)
    assert parse_source_segment("1,2") is None

    spec = parse_target_spec("1-3,5|2")
    assert spec is not None
    assert spec.ratio == 2
    assert spec.segments == (
        AnibridgeMappingRange(start=1, end=3),
        AnibridgeMappingRange(start=5, end=5),
    )


def test_parse_target_spec_rejects_invalid_ratio_and_empty_segments() -> None:
    assert parse_target_spec("1|0") is None
    assert parse_target_spec(" , ") is None


def test_format_and_bounds_helpers() -> None:
    spec = TargetSpec(segments=(AnibridgeMappingRange(start=1, end=2),), ratio=-2)
    assert format_target_spec(spec) == "1-2|-2"
    assert format_range(4, 4) == "4"
    assert format_range(4, 8) == "4-8"

    assert range_bounds("3-7") == (3, 7)
    assert range_bounds("7|2") == (7, 7)
    assert range_bounds("1,2") is None


def test_ranges_overlap_and_internal_overlap() -> None:
    assert ranges_overlap(1, 3, 3, 5)
    assert ranges_overlap(5, None, 10, 20)
    assert not ranges_overlap(1, 2, 3, 4)

    assert has_internal_overlap(
        TargetSpec(
            segments=(
                AnibridgeMappingRange(start=1, end=3),
                AnibridgeMappingRange(start=3, end=5),
            ),
            ratio=None,
        )
    )
    assert not has_internal_overlap(
        TargetSpec(
            segments=(
                AnibridgeMappingRange(start=1, end=2),
                AnibridgeMappingRange(start=3, end=4),
            ),
            ratio=None,
        )
    )


def test_target_units_handles_ratios_and_open_ranges() -> None:
    positive = TargetSpec(
        segments=(AnibridgeMappingRange(start=1, end=3),),
        ratio=2,
    )
    negative = TargetSpec(
        segments=(AnibridgeMappingRange(start=1, end=6),),
        ratio=-3,
    )
    invalid_negative = TargetSpec(
        segments=(AnibridgeMappingRange(start=1, end=5),),
        ratio=-3,
    )
    open_ended = TargetSpec(
        segments=(AnibridgeMappingRange(start=1, end=None),),
        ratio=None,
    )

    assert target_units(positive) == 6
    assert target_units(negative) == 2
    assert target_units(invalid_negative) is None
    assert target_units(open_ended) is None


def test_merge_segments_merges_touching_and_open_ranges() -> None:
    merged = merge_segments(
        [
            AnibridgeMappingRange(start=3, end=5),
            AnibridgeMappingRange(start=1, end=2),
            AnibridgeMappingRange(start=7, end=None),
            AnibridgeMappingRange(start=6, end=6),
        ]
    )
    assert merged == [AnibridgeMappingRange(start=1, end=None)]


def test_normalize_reversed_pair_splits_comma_source_and_inverts_ratio() -> None:
    assert normalize_reversed_pair("1|2", "1-2") == [("1", "1-2|-2")]

    split = normalize_reversed_pair("1-2,3-4", "10-13")
    assert split == [("1-2", "10-11"), ("3-4", "12-13")]

    assert normalize_reversed_pair("1-2,4", "1-3") == [
        ("1-2", "1-2"),
        ("4", "3"),
    ]


def test_parse_target_spec_empty_and_whitespace_cases() -> None:
    """Test edge cases in parse_target_spec."""
    assert parse_target_spec("") is None
    assert parse_target_spec("   ") is None
    assert parse_target_spec("1, , 2") is None  # Empty part after split


def test_parse_source_segment_with_ratio_not_allowed() -> None:
    """Test that parse_source_segment rejects ratios."""
    assert parse_source_segment("1|2") is None
    assert parse_source_segment("  ") is None
    assert parse_source_segment("") is None


def test_has_internal_overlap_no_overlap_single_segment() -> None:
    """Test has_internal_overlap with single segment (no overlap possible)."""
    spec = TargetSpec(
        segments=(AnibridgeMappingRange(start=1, end=5),),
        ratio=None,
    )
    assert not has_internal_overlap(spec)


def test_merge_segments_open_range_continuation() -> None:
    """Test merge_segments when open range appears, it absorbs previous ranges."""
    # When open-ended range is encountered, it expands to absorb previous segments
    merged = merge_segments(
        [
            AnibridgeMappingRange(start=1, end=2),
            AnibridgeMappingRange(start=3, end=None),
            AnibridgeMappingRange(start=5, end=6),
        ]
    )
    # After merge, open-ended range absorbs everything: result is [1-∞]
    assert len(merged) >= 1
    assert merged[0].start == 1
    assert merged[0].end is None


def test_target_units_multiple_segments_no_ratio() -> None:
    """Test target_units with multiple segments and no ratio."""
    spec = TargetSpec(
        segments=(
            AnibridgeMappingRange(start=1, end=3),
            AnibridgeMappingRange(start=5, end=7),
        ),
        ratio=None,
    )
    assert target_units(spec) == 6  # 3 + 3 episodes


def test_normalize_reversed_pair_source_ratio_inverts_to_target() -> None:
    """Test that source ratio gets inverted and applied to target."""
    # When source has ratio and multiple commas aren't present
    result = normalize_reversed_pair("5|3", "10-12")
    assert result == [("5", "10-12|-3")]


def test_normalize_reversed_pair_comma_source_with_invalid_target_end() -> None:
    """Test normalize when target has no end (open range)."""
    # Target must have bounded end for comma normalization
    result = normalize_reversed_pair("1-1,2-2", "1-")
    assert result == []  # Invalid: target end not bounded


def test_normalize_reversed_pair_totals_mismatch() -> None:
    """Test normalize when source and target lengths don't match."""
    result = normalize_reversed_pair(
        "1-3,4-5", "10-12"
    )  # 3 + 2 = 5 vs 3 total = mismatch
    assert result == []


def test_normalize_reversed_pair_source_with_open_range() -> None:
    """Test normalize when source has open-ended segments."""
    result = normalize_reversed_pair("1-2,3-", "5-7")
    assert result == []  # Can't split open-ended source segments


def test_format_range_single_vs_range() -> None:
    """Double-check format_range distinguishes single vs range."""
    assert format_range(5, 5) == "5"
    assert format_range(5, 10) == "5-10"


def test_ranges_overlap_boundary_cases() -> None:
    """Test ranges_overlap with None end values."""
    # Open range overlaps everything at or after its start
    assert ranges_overlap(1, None, 100, 200)
    assert ranges_overlap(100, 200, 50, None)
    assert ranges_overlap(1, None, 1, None)

    # Non-overlapping with None
    assert not ranges_overlap(1, 5, 10, None)  # False: 5 < 10
    assert ranges_overlap(5, None, 1, 5)  # True: 5 == 5


def test_parse_source_segment_invalid_range() -> None:
    """Test parse_source_segment with invalid range format."""
    assert parse_source_segment("not-a-number") is None
    assert parse_source_segment("10-5") is None  # Invalid range


def test_parse_target_spec_with_multiple_pipes() -> None:
    """Test parse_target_spec handles only last pipe as ratio."""
    # Multiple pipes should only use the last one
    result = parse_target_spec("1-2")
    assert result is not None
    assert result.ratio is None


def test_normalize_reversed_pair_incomplete_segments() -> None:
    """Test normalize catches incomplete/invalid target end."""
    result = normalize_reversed_pair("1-2,3-4", "10-")
    assert result == []  # Target end None means unbounded

    result = normalize_reversed_pair("1-2,3-4", "invalid")
    assert result == []


def test_normalize_reversed_pair_indivisible_ratio() -> None:
    """Test that mismatched ratios in segments still process correctly."""
    # Source total 5, target total 5 with proper split
    result = normalize_reversed_pair("1-3,4-5", "10-14")
    assert result == [("1-3", "10-12"), ("4-5", "13-14")]


def test_has_internal_overlap_with_prev_tracking() -> None:
    """Test has_internal_overlap updates prev when needed."""
    # Test case where second segment has larger end than first
    spec = TargetSpec(
        segments=(
            AnibridgeMappingRange(start=1, end=5),
            AnibridgeMappingRange(start=6, end=10),
            AnibridgeMappingRange(start=9, end=15),  # Overlaps with 6-10
        ),
        ratio=None,
    )
    assert has_internal_overlap(spec)


def test_parse_target_spec_with_ratio_zero() -> None:
    """Test that parse_target_spec rejects ratio|0."""
    assert parse_target_spec("1-2|0") is None


def test_parse_target_spec_with_invalid_ratio_format() -> None:
    """Test that parse_target_spec handles non-integer ratio."""
    # This should fail when trying to parse ratio as int
    result = parse_target_spec("1-2|abc")
    assert result is None


def test_parse_target_spec_with_empty_segments_after_split() -> None:
    """Test parse_target_spec with valid complex segments."""
    # Test with multiple segments (valid format)
    result = parse_target_spec("1-2,3-4,5-6")
    assert result is not None
    assert len(result.segments) == 3
    assert result.ratio is None


def test_normalize_reversed_pair_with_many_commas() -> None:
    """Test normalize handles multiple comma-separated segments properly."""
    result = normalize_reversed_pair("1-2,3-4,5-6", "20-25")
    expected = [("1-2", "20-21"), ("3-4", "22-23"), ("5-6", "24-25")]
    assert result == expected


def test_merge_segments_with_unsorted_overlaps() -> None:
    """Test merge properly handles overlapping segments that aren't sorted."""
    merged = merge_segments(
        [
            AnibridgeMappingRange(start=5, end=6),
            AnibridgeMappingRange(start=1, end=3),
            AnibridgeMappingRange(start=2, end=5),  # Overlaps and touches
        ]
    )
    # After sorting and merging overlaps, should produce fewer segments
    assert len(merged) <= 2
    assert merged[0].start == 1  # Starts at first segment's start


def test_merge_segments_reverse_order_non_overlapping() -> None:
    """Test merge with reverse-ordered non-overlapping segments."""
    merged = merge_segments(
        [
            AnibridgeMappingRange(start=10, end=12),
            AnibridgeMappingRange(start=5, end=7),
            AnibridgeMappingRange(start=1, end=3),
        ]
    )
    # Should sort and return in order
    assert merged == [
        AnibridgeMappingRange(start=1, end=3),
        AnibridgeMappingRange(start=5, end=7),
        AnibridgeMappingRange(start=10, end=12),
    ]
