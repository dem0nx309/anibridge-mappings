"""Validation helpers for mapping integrity checks."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from anibridge.utils.mappings import (
    AnibridgeMapping,
    AnibridgeMappingRange,
    format_mapping_range,
    is_valid_target_range,
)

from anibridge_mappings.core.graph import EpisodeMappingGraph, IdMappingGraph
from anibridge_mappings.core.meta import MetaStore
from anibridge_mappings.utils.mapping import (
    SourceTargetMap,
    build_source_target_map,
    provider_scope_sort_key,
)


@dataclass(slots=True)
class ValidationIssue:
    """Represents a validation finding."""

    validator: str
    message: str
    source: str | None = None
    target: str | None = None
    source_range: str | None = None
    target_range: str | None = None
    details: dict[str, Any] | None = None


@dataclass(slots=True)
class ValidationContext:
    """Shared context for validators.

    This caches the computed source-target map so that validators
    can iterate the same derived structures without recomputing them.
    """

    episode_graph: EpisodeMappingGraph
    meta_store: MetaStore
    id_graph: IdMappingGraph
    source_map: SourceTargetMap

    @classmethod
    def from_graphs(
        cls,
        episode_graph: EpisodeMappingGraph,
        meta_store: MetaStore,
        id_graph: IdMappingGraph,
    ) -> ValidationContext:
        """Construct a validation context with a cached source map."""
        return cls(
            episode_graph=episode_graph,
            meta_store=meta_store,
            id_graph=id_graph,
            source_map=build_source_target_map(episode_graph),
        )


class MappingValidator:
    """Base class for mapping validators."""

    name: str = "validator"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return validation issues for the provided graphs.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Validation issues, if any.
        """
        raise NotImplementedError

    def issue(
        self,
        message: str,
        *,
        source: str | None = None,
        target: str | None = None,
        source_range: str | None = None,
        target_range: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> ValidationIssue:
        """Build a standardized validation issue."""
        return ValidationIssue(
            validator=self.name,
            message=message,
            source=source,
            target=target,
            source_range=source_range,
            target_range=target_range,
            details=details,
        )


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Build a provider descriptor string from components."""
    if scope is None:
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def _iter_target_ranges(
    source_ranges: dict[str, set[str]],
) -> Iterable[tuple[str, str]]:
    """Yield (source_range, target_range_spec) pairs from a source-range map."""
    for source_range in sorted(source_ranges):
        target_ranges = source_ranges[source_range]
        for target_range in sorted(target_ranges):
            yield source_range, target_range


def _parse_source_range(range_key: str) -> AnibridgeMappingRange | None:
    """Parse one source range using the canonical anibridge-utils parser."""
    normalized = range_key.strip()
    if not normalized:
        return None
    try:
        return AnibridgeMappingRange.parse(normalized)
    except ValueError:
        return None


def _parse_mapping_pair(
    source_range: str,
    target_range: str,
) -> tuple[AnibridgeMapping | None, str | None]:
    """Parse one source-target mapping pair.

    Returns mapping object and optional parsing error detail.
    """
    try:
        return AnibridgeMapping.parse(source_range, target_range), None
    except ValueError as exc:
        return None, str(exc)


def _ranges_overlap(
    start_a: int,
    end_a: int | None,
    start_b: int,
    end_b: int | None,
) -> bool:
    """Return True if two inclusive ranges overlap (supports open-ended)."""
    return not (
        (end_a is not None and end_a < start_b)
        or (end_b is not None and end_b < start_a)
    )


class MappingRangeValidator(MappingValidator):
    """Validate mapping range syntax and consistency."""

    name = "mapping_ranges"

    def validate(self, context: ValidationContext) -> list[ValidationIssue]:
        """Return issues found across source/target range specifications.

        Args:
            context (ValidationContext): Shared validation context.

        Returns:
            list[ValidationIssue]: Range validation issues found.
        """
        issues: list[ValidationIssue] = []

        for (src_provider, src_id, src_scope), targets in context.source_map.items():
            source_descriptor = _descriptor(src_provider, src_id, src_scope)
            provider_source_ranges: dict[
                str, list[tuple[int, int | None, str, str, str]]
            ] = {}
            for (t_provider, t_id, t_scope), source_ranges in targets.items():
                target_descriptor = _descriptor(t_provider, t_id, t_scope)
                if src_provider == t_provider:
                    for source_range, target_range in _iter_target_ranges(
                        source_ranges
                    ):
                        issues.append(
                            self.issue(
                                "Same-provider cross-link",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"provider": src_provider},
                            )
                        )
                    continue

                meta = context.meta_store.peek(t_provider, t_id, t_scope)
                limit = meta.episodes if meta else None
                target_segments: list[tuple[int, int | None, str, str]] = []

                for source_range, target_range in _iter_target_ranges(source_ranges):
                    source_segment = _parse_source_range(source_range)
                    if source_segment is None:
                        issues.append(
                            self.issue(
                                "Invalid source range syntax",
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={"source_range": source_range},
                            )
                        )
                    else:
                        source_start = source_segment.start
                        source_end = source_segment.end
                        provider_source_ranges.setdefault(t_provider, []).append(
                            (
                                source_start,
                                source_end,
                                target_descriptor,
                                source_range,
                                target_range,
                            )
                        )

                    parsed_mapping, parse_error = _parse_mapping_pair(
                        source_range,
                        target_range,
                    )
                    if parsed_mapping is None:
                        if not is_valid_target_range(target_range.strip()):
                            message = "Invalid target range syntax"
                        else:
                            message = "Invalid mapping ratio semantics"
                        issues.append(
                            self.issue(
                                message,
                                source=source_descriptor,
                                target=target_descriptor,
                                source_range=source_range,
                                target_range=target_range,
                                details={
                                    "target_range": target_range,
                                    "error": parse_error,
                                },
                            )
                        )
                        continue

                    segments = list(parsed_mapping.target_ranges)
                    for segment in segments:
                        target_segments.append(
                            (
                                segment.start,
                                segment.end,
                                source_range,
                                target_range,
                            )
                        )

                        if limit and limit > 0:
                            if segment.end is None:
                                if segment.start > limit:
                                    issues.append(
                                        self.issue(
                                            "Target mapping exceeds available episodes",
                                            source=source_descriptor,
                                            target=target_descriptor,
                                            source_range=source_range,
                                            target_range=format_mapping_range(segment),
                                            details={
                                                "source_range": source_range,
                                                "target_range": format_mapping_range(
                                                    segment
                                                ),
                                                "episode_limit": limit,
                                            },
                                        )
                                    )
                            elif segment.end > limit:
                                issues.append(
                                    self.issue(
                                        "Target mapping exceeds available episodes",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=source_range,
                                        target_range=format_mapping_range(segment),
                                        details={
                                            "source_range": source_range,
                                            "target_range": format_mapping_range(
                                                segment
                                            ),
                                            "episode_limit": limit,
                                        },
                                    )
                                )

                    if len(segments) > 1:
                        segments.sort(
                            key=lambda item: (
                                item.start,
                                float("inf") if item.end is None else item.end,
                            )
                        )
                        prev = segments[0]
                        for current in segments[1:]:
                            if _ranges_overlap(
                                prev.start,
                                prev.end,
                                current.start,
                                current.end,
                            ):
                                issues.append(
                                    self.issue(
                                        "Overlapping target segments within a mapping",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=source_range,
                                        target_range=target_range,
                                        details={
                                            "overlaps_with": format_mapping_range(prev),
                                            "segment": format_mapping_range(current),
                                        },
                                    )
                                )
                            prev_end_value = (
                                float("inf") if prev.end is None else prev.end
                            )
                            current_end_value = (
                                float("inf") if current.end is None else current.end
                            )
                            if current_end_value > prev_end_value:
                                prev = current

                if len(target_segments) > 1:
                    target_segments.sort(
                        key=lambda item: (
                            item[0],
                            float("inf") if item[1] is None else item[1],
                        )
                    )
                    prev: tuple[int, int | None, str, str] | None = None
                    for start, end, src_range, tgt_range in target_segments:
                        if prev is not None:
                            prev_start, prev_end, prev_src, prev_base = prev
                            if src_range != prev_src and _ranges_overlap(
                                start, end, prev_start, prev_end
                            ):
                                issues.append(
                                    self.issue(
                                        "Overlapping target episode ranges for the "
                                        "same target scope",
                                        source=source_descriptor,
                                        target=target_descriptor,
                                        source_range=src_range,
                                        target_range=tgt_range,
                                        details={
                                            "source_range": src_range,
                                            "target_range": tgt_range,
                                            "overlaps_with_source_range": prev_src,
                                            "overlaps_with_target_range": prev_base,
                                        },
                                    )
                                )
                        prev_end_value = (
                            float("inf")
                            if prev is None
                            else (float("inf") if prev[1] is None else prev[1])
                        )
                        current_end_value = float("inf") if end is None else end
                        if prev is None or current_end_value >= prev_end_value:
                            prev = (start, end, src_range, tgt_range)

            for target_provider, items in provider_source_ranges.items():
                if len(items) <= 1:
                    continue
                items.sort(
                    key=lambda item: (
                        item[0],
                        10**9 if item[1] is None else item[1],
                        provider_scope_sort_key(item[2]),
                        item[3],
                        item[4],
                    )
                )
                accepted: list[tuple[int, int | None, str, str, str]] = []
                for start, end, target_descriptor, source_range, target_range in items:
                    overlap_with = next(
                        (
                            accepted_item
                            for accepted_item in accepted
                            if _ranges_overlap(
                                start,
                                end,
                                accepted_item[0],
                                accepted_item[1],
                            )
                        ),
                        None,
                    )
                    if overlap_with is None:
                        accepted.append(
                            (
                                start,
                                end,
                                target_descriptor,
                                source_range,
                                target_range,
                            )
                        )
                        continue

                    _, _, prev_target, prev_source_range, prev_target_range = (
                        overlap_with
                    )
                    issues.append(
                        self.issue(
                            "Overlapping source episode ranges for the same target "
                            "provider",
                            source=source_descriptor,
                            target=target_descriptor,
                            source_range=source_range,
                            target_range=target_range,
                            details={
                                "target_provider": target_provider,
                                "overlaps_with_target": prev_target,
                                "overlaps_with_source_range": prev_source_range,
                                "overlaps_with_target_range": prev_target_range,
                            },
                        )
                    )

        return issues
