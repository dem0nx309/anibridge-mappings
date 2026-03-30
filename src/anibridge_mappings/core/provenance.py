"""Compact provenance serialization helpers."""

import importlib.metadata
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson

from anibridge_mappings.core.graph import EpisodeMappingGraph, ProvenanceEvent


def _normalize_timestamp(value: datetime | None) -> str:
    """Normalize a datetime to an ISO 8601 UTC string."""
    if value is None:
        value = datetime.now(tz=UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _descriptor(provider: str, entry_id: str, scope: str | None) -> str:
    """Build a unique descriptor string for a given ID with optional scope."""
    if scope is None or scope == "":
        return f"{provider}:{entry_id}"
    return f"{provider}:{entry_id}:{scope}"


def _event_payload(
    event: ProvenanceEvent,
    *,
    source_range: str,
    target_range: str,
    include_details: bool,
) -> dict[str, Any]:
    """Serialize a single oriented event into a JSON-ready payload."""
    payload: dict[str, Any] = {
        "seq": event.seq,
        "action": event.action,
        "stage": event.stage,
        "effective": event.effective,
        "source_range": source_range,
        "target_range": target_range,
    }
    if event.actor is not None:
        payload["actor"] = event.actor
    if event.reason is not None:
        payload["reason"] = event.reason
    contributor = _event_contributor(event)
    if contributor is not None:
        payload["contributor"] = contributor
    if include_details and event.details:
        payload["details"] = event.details
    return payload


def _event_contributor(event: ProvenanceEvent) -> str | None:
    """Return a normalized contributor label for an event, if available."""
    if event.details:
        contributor = event.details.get("contributor")
        if isinstance(contributor, str) and contributor.strip():
            return contributor.strip()

    if event.actor and event.stage.startswith("Source ingestion"):
        marker = "Provider source:"
        if marker in event.actor:
            _, _, raw = event.actor.partition(marker)
            candidate = raw.strip()
            if candidate:
                return candidate
    return None


def _active_ranges(events: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Compute current active range pairs by replaying effective events."""
    active: set[tuple[str, str]] = set()
    for event in events:
        if not event.get("effective"):
            continue
        pair = (str(event["source_range"]), str(event["target_range"]))
        if event.get("action") == "add":
            active.add(pair)
        elif event.get("action") == "remove":
            active.discard(pair)

    return [
        {"source_range": source_range, "target_range": target_range}
        for source_range, target_range in sorted(active)
    ]


def _mapping_contributors(events: list[dict[str, Any]]) -> list[str]:
    """Return all contributors that emitted events for a mapping pair."""
    contributors = {
        contributor
        for event in events
        if isinstance((contributor := event.get("contributor")), str) and contributor
    }
    return sorted(contributors)


def _active_mapping_contributors(events: list[dict[str, Any]]) -> list[str]:
    """Return contributors with at least one currently active range pair."""
    active_pairs: dict[tuple[str, str], str] = {}
    for event in events:
        if not event.get("effective"):
            continue
        contributor = event.get("contributor")
        source_range = str(event["source_range"])
        target_range = str(event["target_range"])
        pair = (source_range, target_range)
        if event.get("action") == "add":
            if isinstance(contributor, str) and contributor:
                active_pairs[pair] = contributor
        elif event.get("action") == "remove":
            active_pairs.pop(pair, None)

    return sorted(set(active_pairs.values()))


def _index_of_value(
    value: str | None,
    values: list[str],
    index_map: dict[str, int],
) -> int:
    """Return a stable small-int index for a repeated string value."""
    if value is None or value == "":
        return -1
    existing = index_map.get(value)
    if existing is not None:
        return existing
    index = len(values)
    values.append(value)
    index_map[value] = index
    return index


def build_provenance_payload(
    episode_graph: EpisodeMappingGraph,
    *,
    schema_version: str | None = None,
    generated_on: datetime | None = None,
    include_details: bool = False,
) -> dict[str, Any]:
    """Build a compact provenance payload for browser inspection."""
    if schema_version is None:
        schema_version = importlib.metadata.version("anibridge-mappings")

    pair_events: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for source, target, events in episode_graph.provenance_items():
        src_provider, src_id, src_scope, src_range = source
        tgt_provider, tgt_id, tgt_scope, tgt_range = target
        src_descriptor = _descriptor(src_provider, src_id, src_scope)
        tgt_descriptor = _descriptor(tgt_provider, tgt_id, tgt_scope)
        left_descriptor, right_descriptor = sorted((src_descriptor, tgt_descriptor))
        flip = src_descriptor != left_descriptor

        for event in sorted(events, key=lambda item: item.seq):
            payload = _event_payload(
                event,
                source_range=tgt_range if flip else src_range,
                target_range=src_range if flip else tgt_range,
                include_details=include_details,
            )
            pair_events.setdefault((left_descriptor, right_descriptor), []).append(
                payload
            )

    descriptors: list[str] = []
    descriptor_index_map: dict[str, int] = {}
    actions: list[str] = []
    action_index_map: dict[str, int] = {}
    stages: list[str] = []
    stage_index_map: dict[str, int] = {}
    actors: list[str] = []
    actor_index_map: dict[str, int] = {}
    reasons: list[str] = []
    reason_index_map: dict[str, int] = {}
    ranges: list[dict[str, str]] = []
    range_index_map: dict[tuple[str, str], int] = {}

    mappings: list[dict[str, Any]] = []
    present_count = 0
    event_count = 0

    for left_descriptor, right_descriptor in sorted(pair_events):
        events = sorted(
            pair_events[(left_descriptor, right_descriptor)],
            key=lambda item: item["seq"],
        )
        active_ranges = _active_ranges(events)
        present = bool(active_ranges)
        if present:
            present_count += 1
        event_count += len(events)

        compact_events: list[dict[str, Any]] = []
        for event in events:
            range_key = (str(event["source_range"]), str(event["target_range"]))
            range_index = range_index_map.get(range_key)
            if range_index is None:
                range_index = len(ranges)
                ranges.append(
                    {
                        "s": range_key[0],
                        "t": range_key[1],
                    }
                )
                range_index_map[range_key] = range_index

            compact_events.append(
                {
                    "a": _index_of_value(
                        event.get("action"), actions, action_index_map
                    ),
                    "s": _index_of_value(event.get("stage"), stages, stage_index_map),
                    "ac": _index_of_value(event.get("actor"), actors, actor_index_map),
                    "rs": _index_of_value(
                        event.get("reason"), reasons, reason_index_map
                    ),
                    "r": range_index,
                    "e": bool(event.get("effective")),
                }
            )

        mappings.append(
            {
                "s": _index_of_value(
                    left_descriptor, descriptors, descriptor_index_map
                ),
                "t": _index_of_value(
                    right_descriptor, descriptors, descriptor_index_map
                ),
                "p": present,
                "n": len(compact_events),
                "ev": compact_events,
            }
        )

    mapping_count = len(mappings)
    summary = {
        "descriptors": len(descriptors),
        "mappings": mapping_count,
        "present_mappings": present_count,
        "missing_mappings": mapping_count - present_count,
        "events": event_count,
        "ranges": len(ranges),
        "files": 1,
    }

    return {
        "$meta": {
            "format": "anibridge.provenance.v2",
            "schema_version": schema_version,
            "generated_on": _normalize_timestamp(generated_on),
            "summary": summary,
        },
        "dict": {
            "descriptors": descriptors,
            "actions": actions,
            "stages": stages,
            "actors": actors,
            "reasons": reasons,
            "ranges": ranges,
        },
        "mappings": mappings,
    }


def validate_provenance_payload(payload: dict[str, Any]) -> None:
    """Validate payload integrity for compact provenance data."""
    meta = payload.get("$meta")
    payload_dict = payload.get("dict")
    mappings = payload.get("mappings")

    if not isinstance(meta, dict):
        raise ValueError("Provenance payload missing $meta object.")
    if not isinstance(payload_dict, dict):
        raise ValueError("Provenance payload missing dict object.")
    if not isinstance(mappings, list):
        raise ValueError("Provenance payload missing mappings list.")

    summary = meta.get("summary")
    if not isinstance(summary, dict):
        raise ValueError("Provenance payload metadata missing summary object.")

    descriptors = payload_dict.get("descriptors")
    actions = payload_dict.get("actions")
    stages = payload_dict.get("stages")
    actors = payload_dict.get("actors")
    reasons = payload_dict.get("reasons")
    ranges = payload_dict.get("ranges")

    if not isinstance(descriptors, list):
        raise ValueError("Provenance payload descriptors must be a list.")
    if not isinstance(actions, list):
        raise ValueError("Provenance payload actions must be a list.")
    if not isinstance(stages, list):
        raise ValueError("Provenance payload stages must be a list.")
    if not isinstance(actors, list):
        raise ValueError("Provenance payload actors must be a list.")
    if not isinstance(reasons, list):
        raise ValueError("Provenance payload reasons must be a list.")
    if not isinstance(ranges, list):
        raise ValueError("Provenance payload ranges must be a list.")

    expected_descriptors = int(summary.get("descriptors", -1))
    expected_mappings = int(summary.get("mappings", -1))
    expected_present = int(summary.get("present_mappings", -1))
    expected_missing = int(summary.get("missing_mappings", -1))
    expected_events = int(summary.get("events", -1))
    expected_ranges = int(summary.get("ranges", -1))

    actual_present = 0
    actual_events = 0

    for mapping in mappings:
        if not isinstance(mapping, dict):
            raise ValueError("Provenance mappings must contain objects.")
        source_index = int(mapping.get("s", -1))
        target_index = int(mapping.get("t", -1))
        if source_index < 0 or source_index >= len(descriptors):
            raise ValueError(f"Mapping source index out of bounds: {source_index}")
        if target_index < 0 or target_index >= len(descriptors):
            raise ValueError(f"Mapping target index out of bounds: {target_index}")

        events = mapping.get("ev")
        if not isinstance(events, list):
            raise ValueError("Provenance mapping events must be a list.")
        if int(mapping.get("n", -1)) != len(events):
            raise ValueError(
                f"Mapping event count mismatch: {mapping.get('n')} != {len(events)}"
            )

        actual_events += len(events)
        active_ranges: set[int] = set()

        for event in events:
            if not isinstance(event, dict):
                raise ValueError("Compact provenance events must be objects.")

            for key, values, allow_missing in (
                ("a", actions, False),
                ("s", stages, False),
                ("ac", actors, True),
                ("rs", reasons, True),
            ):
                event_index = int(event.get(key, -1))
                if allow_missing and event_index == -1:
                    continue
                if event_index < 0:
                    raise ValueError(
                        f"Event index out of bounds for {key}: {event_index}"
                    )
                if event_index >= len(values):
                    raise ValueError(
                        f"Event index out of bounds for {key}: {event_index}"
                    )

            range_index = int(event.get("r", -1))
            if range_index < 0 or range_index >= len(ranges):
                raise ValueError(f"Event range index out of bounds: {range_index}")

            if bool(event.get("e")):
                action = actions[int(event["a"])]
                if action == "add":
                    active_ranges.add(range_index)
                elif action == "remove":
                    active_ranges.discard(range_index)

        present = bool(active_ranges)
        if bool(mapping.get("p")) != present:
            raise ValueError(
                f"Mapping presence mismatch: {mapping.get('p')} != {present}"
            )
        if present:
            actual_present += 1

    actual_mappings = len(mappings)
    actual_missing = actual_mappings - actual_present

    if expected_descriptors != len(descriptors):
        raise ValueError(
            f"Manifest descriptors mismatch: {expected_descriptors} != "
            f"{len(descriptors)}"
        )
    if expected_mappings != actual_mappings:
        raise ValueError(
            f"Manifest mappings mismatch: {expected_mappings} != {actual_mappings}"
        )
    if expected_present != actual_present:
        raise ValueError(
            f"Manifest present mismatch: {expected_present} != {actual_present}"
        )
    if expected_missing != actual_missing:
        raise ValueError(
            f"Manifest missing mismatch: {expected_missing} != {actual_missing}"
        )
    if expected_events != actual_events:
        raise ValueError(
            f"Manifest events mismatch: {expected_events} != {actual_events}"
        )
    if expected_ranges != len(ranges):
        raise ValueError(
            f"Manifest ranges mismatch: {expected_ranges} != {len(ranges)}"
        )


def write_provenance_payload(path: Path, payload: dict[str, Any]) -> None:
    """Write a compact provenance payload into a zip file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(
        path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        archive.writestr(
            "provenance.json",
            orjson.dumps(payload),
        )
