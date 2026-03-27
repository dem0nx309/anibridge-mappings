from anibridge_mappings.core.aggregator import AggregationArtifacts
from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.core.stats import (
    _compact_count,
    build_stats,
    render_stats_markdown,
)
from anibridge_mappings.core.validators import ValidationIssue


def test_compact_count() -> None:
    assert _compact_count(999) == "999"
    assert _compact_count(1_000) == "1k"
    assert _compact_count(1_500_000) == "1.5m"


def test_build_and_render_stats_payload() -> None:
    id_graph = IdMappingGraph()
    id_graph.add_edge(("anidb", "1", "R"), ("mal", "2", None))

    episode_graph = EpisodeMappingGraph()
    episode_graph.add_edge(("anidb", "1", "R", "1-2"), ("mal", "2", None, "1-2"))

    payload = {
        "$meta": {"schema_version": "x"},
        "anidb:1:R": {"mal:2": {"1-2": "1-2"}},
    }

    artifacts = AggregationArtifacts(
        id_graph=id_graph,
        meta_store=MetaStore(),
        episode_graph=episode_graph,
        validation_issues=[
            ValidationIssue(
                validator="mapping_ranges",
                message="bad",
                source="anidb:1:R",
                target="mal:2",
            )
        ],
    )

    stats = build_stats(artifacts, payload)
    assert stats["summary"]["providers"] == 2
    assert stats["validator"]["total_issues"] == 1

    md = render_stats_markdown(stats)
    assert "Raw stats JSON" in md
