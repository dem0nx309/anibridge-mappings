"""Inference helpers for metadata-backed episode mappings."""

from collections.abc import Iterable
from itertools import combinations
from logging import getLogger

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph, IdNode
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType

log = getLogger(__name__)


def infer_episode_mappings(
    meta_store: MetaStore,
    id_graph: IdMappingGraph,
) -> EpisodeMappingGraph:
    """Infer episode mappings when metadata align."""
    inferred = EpisodeMappingGraph()

    for component in _iter_components(id_graph):
        candidates = _component_meta_candidates(meta_store, component)
        if len(candidates) < 2:
            continue

        for (meta_left, node_left), (meta_right, node_right) in combinations(
            candidates,
            2,
        ):
            if not _meta_match(meta_left, meta_right):
                continue

            episode_range = _episode_range(meta_left)
            if episode_range is None:
                continue

            inferred.add_edge((*node_left, episode_range), (*node_right, episode_range))

    if inferred.node_count():
        log.info(
            "Inferred %d episode mapping node(s) from metadata",
            inferred.node_count(),
        )

    return inferred


def _component_meta_candidates(
    meta_store: MetaStore,
    component: set[IdNode],
) -> list[tuple[SourceMeta, IdNode]]:
    """Find all nodes in the component with valid metadata for episode inference."""
    candidates: list[tuple[SourceMeta, IdNode]] = []
    for node in component:
        meta = meta_store.peek(*node)
        if meta is None or meta.episodes is None or meta.episodes <= 0:
            continue
        candidates.append((meta, node))
    return candidates


def _meta_match(left: SourceMeta, right: SourceMeta) -> bool:
    if left.type != right.type or left.episodes != right.episodes:
        return False

    if not _year_match(left, right):
        return False
    return _duration_match(left, right)


def _year_match(left: SourceMeta, right: SourceMeta) -> bool:
    """Check if years match with some tolerance."""
    left_year, right_year = left.start_year, right.start_year
    if left.type == SourceType.MOVIE:
        return bool(left_year and right_year and left_year == right_year)
    if left_year and right_year:
        return left_year == right_year
    return True


def _duration_match(left: SourceMeta, right: SourceMeta) -> bool:
    """Check if durations match with some tolerance."""
    left_duration, right_duration = left.duration, right.duration
    if left.type == SourceType.MOVIE:
        if not left_duration or not right_duration:
            return False
        return _relative_delta(left_duration, right_duration) <= 0.1

    if left_duration and right_duration:
        return _relative_delta(left_duration, right_duration) <= 0.1
    return True


def _relative_delta(a: int, b: int) -> float:
    """Calculate relative delta between two values."""
    denominator = max(abs(a), abs(b))
    if denominator == 0:
        return 0.0
    return abs(a - b) / denominator


def _episode_range(meta: SourceMeta) -> str | None:
    """Format episode range from metadata."""
    episodes = meta.episodes
    if episodes is None or episodes <= 0:
        return None
    return "1" if episodes == 1 else f"1-{episodes}"


def _iter_components(id_graph: IdMappingGraph) -> Iterable[set[IdNode]]:
    """Yield connected components of the ID mapping graph."""
    visited: set[IdNode] = set()
    for node in id_graph.nodes():
        if node in visited:
            continue
        component = id_graph.get_component(node)
        visited.update(component)
        yield component
