from anibridge_mappings.core.graph import IdMappingGraph
from anibridge_mappings.core.inference import (
    _duration_match,
    _episode_range,
    _meta_match,
    _relative_delta,
    _year_match,
    infer_episode_mappings,
)
from anibridge_mappings.core.meta import MetaStore, SourceMeta, SourceType


def test_infer_episode_mappings_links_matching_component_nodes() -> None:
    id_graph = IdMappingGraph()
    a = ("anidb", "1", "R")
    b = ("anilist", "2", None)
    c = ("mal", "3", None)
    id_graph.add_equivalence_class([a, b, c])

    store = MetaStore()
    shared = SourceMeta(type=SourceType.TV, episodes=12, duration=24, start_year=2020)
    store.set(a[0], a[1], shared, a[2])
    store.set(b[0], b[1], shared, b[2])
    store.set(
        c[0],
        c[1],
        SourceMeta(type=SourceType.TV, episodes=24, duration=24, start_year=2020),
        c[2],
    )

    episode_graph = infer_episode_mappings(store, id_graph)

    assert episode_graph.has_edge(
        ("anidb", "1", "R", "1-12"), ("anilist", "2", None, "1-12")
    )
    assert not episode_graph.has_edge(
        ("anidb", "1", "R", "1-12"), ("mal", "3", None, "1-24")
    )


def test_inference_matching_helpers() -> None:
    tv_a = SourceMeta(type=SourceType.TV, episodes=10, duration=24, start_year=2020)
    tv_b = SourceMeta(type=SourceType.TV, episodes=10, duration=25, start_year=2020)
    movie_a = SourceMeta(
        type=SourceType.MOVIE, episodes=1, duration=100, start_year=2020
    )
    movie_b = SourceMeta(
        type=SourceType.MOVIE, episodes=1, duration=109, start_year=2020
    )

    assert _year_match(
        tv_a, SourceMeta(type=SourceType.TV, episodes=10, duration=24, start_year=None)
    )
    assert _duration_match(tv_a, tv_b)
    assert _duration_match(movie_a, movie_b)
    assert _meta_match(tv_a, tv_b)

    assert _relative_delta(10, 12) == 2 / 12
    assert _episode_range(SourceMeta(episodes=1)) == "1"
    assert _episode_range(SourceMeta(episodes=5)) == "1-5"
    assert _episode_range(SourceMeta(episodes=0)) is None
