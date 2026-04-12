import { useEffect, useMemo, useState } from "hono/jsx/dom";
import type {
  ExternalLink,
  MappingViewFormat,
  TimelineSlide,
} from "./ui-types";
import {
  buildFinalMappingObject,
  buildTimelineSlides,
  descriptorToExternal,
  formatMappingView,
} from "../utils/mapping-presentation";
import {
  fetchMetadata,
  getPosterUrl,
  resolveMetadata,
  type MetadataEnvelope,
  type ResolvedMetadata,
} from "../utils/metadata-api";

import type { Dict, Mapping } from "../utils/provenance";
import { getDictValue } from "../utils/provenance";

const MAPPING_VIEW_FORMAT_STORAGE_KEY = "anibridge:mapping-view-format";

const getStoredFormat = (): MappingViewFormat => {
  const v = window.localStorage.getItem(MAPPING_VIEW_FORMAT_STORAGE_KEY);
  return v === "yaml" ? "yaml" : "json";
};

const truncateText = (value: string | null | undefined, limit: number) => {
  if (!value) return null;
  if (value.length <= limit) return value;
  return `${value.slice(0, limit - 1).trimEnd()}...`;
};

const formatRelease = (
  startDate?: string | null,
  endDate?: string | null,
): string | null => {
  if (!startDate && !endDate) return null;
  if (startDate && endDate) return `${startDate} to ${endDate}`;
  return startDate ?? endDate ?? null;
};

const formatScopeLabel = (metadata: ResolvedMetadata) => {
  if (metadata.scopeKey) return `scope ${metadata.scopeKey}`;
  return null;
};

const MetadataValue = ({
  descriptor,
  metadata,
  loading,
}: {
  descriptor: string;
  metadata: ResolvedMetadata | null;
  loading: boolean;
}) => {
  if (loading) {
    return (
      <div class="text-xs text-slate-500 dark:text-slate-400">
        Loading metadata...
      </div>
    );
  }

  if (!metadata) {
    return (
      <div class="text-xs text-slate-500 dark:text-slate-400">
        No metadata found for {descriptor}.
      </div>
    );
  }

  const statusLabel =
    metadata.release?.status && metadata.release.status !== "unknown"
      ? metadata.release.status
      : null;
  const scopeLabel = formatScopeLabel(metadata);

  return (
    <div class="min-w-0 text-xs text-slate-700 dark:text-slate-200">
      <div class="font-mono text-[11px] text-slate-500 dark:text-slate-400">
        {metadata.id.descriptor}
      </div>
      <div class="mt-1 flex flex-wrap gap-1 text-[11px]">
        <span class="border border-slate-300 bg-slate-100 px-1.5 py-0.5 dark:border-slate-500 dark:bg-slate-700">
          {metadata.kind}
        </span>
        {scopeLabel ? (
          <span class="border border-slate-300 bg-slate-100 px-1.5 py-0.5 dark:border-slate-500 dark:bg-slate-700">
            {scopeLabel}
          </span>
        ) : null}
        {statusLabel ? (
          <span class="border border-slate-300 bg-slate-100 px-1.5 py-0.5 dark:border-slate-500 dark:bg-slate-700">
            {statusLabel}
          </span>
        ) : null}
        {metadata.classification.is_adult ? (
          <span class="border border-rose-300 bg-rose-50 px-1.5 py-0.5 text-rose-700 dark:border-rose-700 dark:bg-rose-950/30 dark:text-rose-300">
            adult
          </span>
        ) : null}
      </div>
    </div>
  );
};

const MetadataTitleValue = ({
  descriptor,
  metadata,
  loading,
}: {
  descriptor: string;
  metadata: ResolvedMetadata | null;
  loading: boolean;
}) => {
  const posterUrl = metadata ? getPosterUrl(metadata.images) : null;
  const originalTitle = metadata?.titles.original;

  return (
    <div class="grid min-h-[96px] grid-cols-[72px_minmax(0,1fr)] gap-2">
      {posterUrl ? (
        <img
          src={posterUrl}
          alt=""
          class="h-full w-full border border-slate-300 object-cover dark:border-slate-600"
        />
      ) : (
        <div class="flex h-full min-h-[96px] items-center justify-center border border-dashed border-slate-300 bg-slate-50 text-[11px] uppercase tracking-[0.03em] text-slate-400 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-500">
          No image
        </div>
      )}
      <div class="min-w-0">
        {loading ? (
          <div class="text-xs text-slate-500 dark:text-slate-400">
            Loading metadata...
          </div>
        ) : !metadata ? (
          <div class="text-xs text-slate-500 dark:text-slate-400">
            No metadata found for {descriptor}.
          </div>
        ) : null}
        {metadata ? (
          <>
            <div class="text-sm font-semibold text-slate-900 dark:text-slate-50">
              {metadata.titles.display}
            </div>
            {originalTitle ? (
              <div class="mt-0.5 text-xs text-slate-600 dark:text-slate-300">
                {originalTitle}
              </div>
            ) : null}
            <div class="mt-2">
              <MetadataValue
                descriptor={descriptor}
                metadata={metadata}
                loading={false}
              />
            </div>
          </>
        ) : null}
      </div>
    </div>
  );
};

const MetadataRow = ({
  label,
  source,
  target,
  multiline = false,
}: {
  label: string;
  source: string | null;
  target: string | null;
  multiline?: boolean;
}) => {
  const cellClass = multiline
    ? "px-2 py-2 text-xs leading-5 text-slate-700 dark:text-slate-200"
    : "px-2 py-1.5 text-xs text-slate-700 dark:text-slate-200";

  return (
    <div class="grid grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)] border-t border-slate-300 dark:border-slate-600">
      <div class="bg-slate-100 px-2 py-1.5 text-[10px] uppercase tracking-[0.03em] text-slate-500 dark:bg-slate-700 dark:text-slate-300">
        {label}
      </div>
      <div
        class={`border-l border-slate-300 dark:border-slate-600 ${cellClass}`}
      >
        {source ?? <span class="text-slate-400 dark:text-slate-500">-</span>}
      </div>
      <div
        class={`border-l border-slate-300 dark:border-slate-600 ${cellClass}`}
      >
        {target ?? <span class="text-slate-400 dark:text-slate-500">-</span>}
      </div>
    </div>
  );
};

const MetadataComparison = ({
  sourceDescriptor,
  targetDescriptor,
  sourceMetadata,
  targetMetadata,
  sourceEnvelope,
  targetEnvelope,
  metadataLoading,
}: {
  sourceDescriptor: string;
  targetDescriptor: string;
  sourceMetadata: ResolvedMetadata | null;
  targetMetadata: ResolvedMetadata | null;
  sourceEnvelope: MetadataEnvelope | null;
  targetEnvelope: MetadataEnvelope | null;
  metadataLoading: boolean;
}) => {
  const sourceRelease = sourceMetadata
    ? formatRelease(
        sourceMetadata.release?.start_date,
        sourceMetadata.release?.end_date,
      )
    : null;
  const targetRelease = targetMetadata
    ? formatRelease(
        targetMetadata.release?.start_date,
        targetMetadata.release?.end_date,
      )
    : null;
  const sourceRuntime = sourceMetadata?.runtime
    ? `${sourceMetadata.runtime.minutes} min (${sourceMetadata.runtime.basis})`
    : null;
  const targetRuntime = targetMetadata?.runtime
    ? `${targetMetadata.runtime.minutes} min (${targetMetadata.runtime.basis})`
    : null;
  const sourceGenres = sourceMetadata?.classification.genres.length
    ? sourceMetadata.classification.genres.join(" • ")
    : null;
  const targetGenres = targetMetadata?.classification.genres.length
    ? targetMetadata.classification.genres.join(" • ")
    : null;
  const sourceSynopsis = sourceMetadata
    ? truncateText(sourceMetadata.synopsis, 320)
    : null;
  const targetSynopsis = targetMetadata
    ? truncateText(targetMetadata.synopsis, 320)
    : null;

  return (
    <div class="overflow-auto">
      <div class="min-w-[720px]">
        <div class="grid grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)]">
          <div class="bg-slate-100 px-2 py-1.5 text-[10px] uppercase tracking-[0.03em] text-slate-500 dark:bg-slate-700 dark:text-slate-300">
            Field
          </div>
          <div class="border-l border-slate-300 bg-slate-100 px-2 py-1.5 text-[10px] uppercase tracking-[0.03em] text-slate-500 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-300">
            Source
          </div>
          <div class="border-l border-slate-300 bg-slate-100 px-2 py-1.5 text-[10px] uppercase tracking-[0.03em] text-slate-500 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-300">
            Target
          </div>
        </div>

        <div class="grid grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)] border-t border-slate-300 dark:border-slate-600">
          <div class="bg-slate-100 px-2 py-2 text-[10px] uppercase tracking-[0.03em] text-slate-500 dark:bg-slate-700 dark:text-slate-300">
            Title
          </div>
          <div class="border-l border-slate-300 px-2 py-2 dark:border-slate-600">
            <MetadataTitleValue
              descriptor={sourceDescriptor}
              metadata={sourceMetadata}
              loading={
                metadataLoading && !sourceEnvelope && Boolean(sourceDescriptor)
              }
            />
          </div>
          <div class="border-l border-slate-300 px-2 py-2 dark:border-slate-600">
            <MetadataTitleValue
              descriptor={targetDescriptor}
              metadata={targetMetadata}
              loading={
                metadataLoading && !targetEnvelope && Boolean(targetDescriptor)
              }
            />
          </div>
        </div>

        <MetadataRow
          label="Release"
          source={sourceRelease}
          target={targetRelease}
        />
        <MetadataRow
          label="Runtime"
          source={sourceRuntime}
          target={targetRuntime}
        />
        <MetadataRow
          label="Units"
          source={
            sourceMetadata?.units !== null &&
            sourceMetadata?.units !== undefined
              ? `${sourceMetadata.units}`
              : null
          }
          target={
            targetMetadata?.units !== null &&
            targetMetadata?.units !== undefined
              ? `${targetMetadata.units}`
              : null
          }
        />
        <MetadataRow
          label="Genres"
          source={sourceGenres}
          target={targetGenres}
          multiline
        />
        <MetadataRow
          label="Synopsis"
          source={sourceSynopsis}
          target={targetSynopsis}
          multiline
        />
      </div>
    </div>
  );
};

type MappingDetailsProps = {
  dict: Dict;
  selected: Mapping;
  inEdits: boolean | null;
  onJumpToEdits: () => void;
};

const DescriptorLink = ({
  value,
  link,
}: {
  value: string;
  link: ExternalLink | null;
}) => {
  if (!link) {
    return (
      <div class="mt-0.5 break-words font-mono text-xs">{value || "-"}</div>
    );
  }

  return (
    <a
      class="mt-0.5 inline-flex items-center gap-1 break-words font-mono text-xs text-sky-700 hover:underline dark:text-sky-300"
      href={link.url}
      target="_blank"
      rel="noopener noreferrer"
    >
      {value || "-"}
      <svg
        aria-hidden="true"
        viewBox="0 0 24 24"
        class="h-3 w-3 shrink-0"
        fill="none"
        stroke="currentColor"
        stroke-width="2"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <path d="M7 17 17 7" />
        <path d="M9 7h8v8" />
      </svg>
    </a>
  );
};

export const MappingDetails = ({
  dict,
  selected,
  inEdits,
  onJumpToEdits,
}: MappingDetailsProps) => {
  const [mappingViewFormat, setMappingViewFormat] =
    useState<MappingViewFormat>(getStoredFormat);
  const [metadataOpen, setMetadataOpen] = useState(false);
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [timelineStep, setTimelineStep] = useState(0);
  const [sourceEnvelope, setSourceEnvelope] = useState<MetadataEnvelope | null>(
    null,
  );
  const [targetEnvelope, setTargetEnvelope] = useState<MetadataEnvelope | null>(
    null,
  );
  const [metadataLoading, setMetadataLoading] = useState(false);

  const selectedSource = getDictValue(dict, "descriptors", selected.s);
  const selectedTarget = getDictValue(dict, "descriptors", selected.t);
  const selectedSourceExternal = descriptorToExternal(selectedSource);
  const selectedTargetExternal = descriptorToExternal(selectedTarget);
  const sourceMetadata = useMemo(
    () =>
      sourceEnvelope && selectedSource
        ? resolveMetadata(sourceEnvelope.metadata, selectedSource)
        : null,
    [selectedSource, sourceEnvelope],
  );
  const targetMetadata = useMemo(
    () =>
      targetEnvelope && selectedTarget
        ? resolveMetadata(targetEnvelope.metadata, selectedTarget)
        : null,
    [selectedTarget, targetEnvelope],
  );

  const finalMappingView = useMemo(() => {
    const obj = buildFinalMappingObject(dict, selected);
    return formatMappingView(obj, mappingViewFormat);
  }, [dict, selected, mappingViewFormat]);

  const timelineSlides = useMemo(() => {
    if (!timelineOpen) return [];
    return buildTimelineSlides(dict, selected);
  }, [timelineOpen, dict, selected]);

  useEffect(() => {
    setTimelineStep(timelineSlides.length ? timelineSlides.length - 1 : 0);
  }, [timelineSlides.length]);

  useEffect(() => {
    window.localStorage.setItem(
      MAPPING_VIEW_FORMAT_STORAGE_KEY,
      mappingViewFormat,
    );
  }, [mappingViewFormat]);

  useEffect(() => {
    if (!metadataOpen) return;

    let active = true;
    setMetadataLoading(Boolean(selectedSource || selectedTarget));
    setSourceEnvelope(null);
    setTargetEnvelope(null);

    Promise.all([
      selectedSource ? fetchMetadata(selectedSource) : Promise.resolve(null),
      selectedTarget ? fetchMetadata(selectedTarget) : Promise.resolve(null),
    ])
      .then(([sourceResult, targetResult]) => {
        if (!active) return;
        setSourceEnvelope(sourceResult);
        setTargetEnvelope(targetResult);
      })
      .finally(() => {
        if (active) setMetadataLoading(false);
      });

    return () => {
      active = false;
    };
  }, [metadataOpen, selectedSource, selectedTarget]);

  useEffect(() => {
    setMetadataOpen(false);
    setMetadataLoading(false);
    setSourceEnvelope(null);
    setTargetEnvelope(null);
  }, [selectedSource, selectedTarget]);

  const timelineCurrent = timelineSlides[timelineStep] ?? null;

  return (
    <main class="min-h-0 overflow-auto border border-slate-300 bg-slate-50 p-2 dark:border-slate-600 dark:bg-slate-800">
      <div class="grid gap-2 border border-slate-300 bg-slate-100 p-2 md:grid-cols-4 dark:border-slate-600 dark:bg-slate-700">
        <div>
          <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
            Source
          </div>
          <DescriptorLink
            value={selectedSource}
            link={selectedSourceExternal}
          />
        </div>
        <div>
          <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
            Target
          </div>
          <DescriptorLink
            value={selectedTarget}
            link={selectedTargetExternal}
          />
        </div>
        <div>
          <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
            State
          </div>
          <div class="mt-0.5 font-mono text-xs">
            {selected.p ? "present" : "missing"}
          </div>
        </div>
        <div>
          <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
            Edits
          </div>
          <div class="mt-0.5 flex items-center gap-1.5">
            {inEdits === null ? (
              <span class="font-mono text-xs text-slate-400">...</span>
            ) : inEdits ? (
              <>
                <span class="inline-flex items-center gap-1 font-mono text-xs text-emerald-700 dark:text-emerald-400">
                  <span class="inline-block h-2 w-2 rounded-full bg-emerald-500" />
                  in edits
                </span>
                <button
                  type="button"
                  class="inline-flex items-center gap-0.5 border border-slate-400 bg-white px-1.5 py-0.5 text-[10px] text-slate-600 hover:border-sky-600 hover:text-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-300 dark:hover:border-sky-400 dark:hover:text-sky-300"
                  onClick={onJumpToEdits}
                  title="Open YAML editor and jump to this mapping"
                >
                  Jump to
                  <svg
                    aria-hidden="true"
                    viewBox="0 0 24 24"
                    class="h-2.5 w-2.5"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    stroke-linecap="round"
                    stroke-linejoin="round"
                  >
                    <path d="M7 17 17 7" />
                    <path d="M9 7h8v8" />
                  </svg>
                </button>
              </>
            ) : (
              <span class="inline-flex items-center gap-1 font-mono text-xs text-slate-500 dark:text-slate-400">
                <span class="inline-block h-2 w-2 rounded-full bg-slate-300 dark:bg-slate-500" />
                not in edits
              </span>
            )}
          </div>
        </div>
      </div>

      <section class="mt-2 border border-slate-300 bg-white dark:border-slate-600 dark:bg-slate-800">
        <div class="border-b border-slate-300 bg-slate-100 px-2 py-1.5 dark:border-slate-600 dark:bg-slate-700">
          <div class="mb-1 flex items-center justify-between gap-2">
            <div class="text-xs font-semibold text-slate-700 dark:text-slate-100">
              Mapping
            </div>
            <div class="inline-flex items-center gap-1 text-xs">
              <button
                type="button"
                class={`border px-2 py-0.5 ${
                  mappingViewFormat === "json"
                    ? "border-sky-700 bg-sky-50 text-sky-800 dark:border-sky-300 dark:bg-sky-950/30 dark:text-sky-200"
                    : "border-slate-400 bg-white text-slate-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-300"
                }`}
                onClick={() => setMappingViewFormat("json")}
              >
                JSON
              </button>
              <button
                type="button"
                class={`border px-2 py-0.5 ${
                  mappingViewFormat === "yaml"
                    ? "border-sky-700 bg-sky-50 text-sky-800 dark:border-sky-300 dark:bg-sky-950/30 dark:text-sky-200"
                    : "border-slate-400 bg-white text-slate-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-300"
                }`}
                onClick={() => setMappingViewFormat("yaml")}
              >
                YAML
              </button>
            </div>
          </div>
          <pre class="max-h-[260px] overflow-auto border border-slate-300 bg-white p-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-800">
            {finalMappingView}
          </pre>
        </div>
      </section>

      <details
        open={metadataOpen}
        onToggle={(event: MouseEvent) =>
          setMetadataOpen((event.currentTarget as HTMLDetailsElement).open)
        }
        class="mt-2 border border-slate-300 bg-white dark:border-slate-600 dark:bg-slate-800"
      >
        <summary class="cursor-pointer border-b border-slate-300 bg-slate-100 px-2 py-1.5 text-xs font-semibold dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100">
          Metadata
        </summary>

        {!metadataOpen ? (
          <p class="m-0 px-2 py-2 text-xs text-slate-600 dark:text-slate-300">
            Expand to load metadata.
          </p>
        ) : (
          <MetadataComparison
            sourceDescriptor={selectedSource}
            targetDescriptor={selectedTarget}
            sourceMetadata={sourceMetadata}
            targetMetadata={targetMetadata}
            sourceEnvelope={sourceEnvelope}
            targetEnvelope={targetEnvelope}
            metadataLoading={metadataLoading}
          />
        )}
      </details>

      <details
        open={timelineOpen}
        onToggle={(event: MouseEvent) =>
          setTimelineOpen((event.currentTarget as HTMLDetailsElement).open)
        }
        class="mt-2 border border-slate-300 bg-white dark:border-slate-600 dark:bg-slate-800"
      >
        <summary class="cursor-pointer border-b border-slate-300 bg-slate-100 px-2 py-1.5 text-xs font-semibold dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100">
          Timeline ({selected.n ?? selected.ev?.length ?? 0} steps)
        </summary>

        {!timelineOpen ? (
          <p class="m-0 px-2 py-2 text-xs text-slate-600 dark:text-slate-300">
            Expand to load timeline.
          </p>
        ) : null}

        {timelineOpen && timelineSlides.length ? (
          <>
            <div class="flex items-center justify-between gap-2 border-b border-slate-300 bg-slate-100 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100">
              <button
                type="button"
                disabled={timelineStep <= 0}
                class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                onClick={() => setTimelineStep((prev) => Math.max(0, prev - 1))}
              >
                Older
              </button>
              <span class="inline-flex items-center gap-0.5">
                Step{" "}
                <input
                  type="number"
                  min={1}
                  max={timelineSlides.length}
                  value={timelineStep + 1}
                  onInput={(e) => {
                    const v = Number((e.target as HTMLInputElement).value);
                    if (v >= 1 && v <= timelineSlides.length)
                      setTimelineStep(v - 1);
                  }}
                  class="w-10 border border-slate-400 bg-white px-1 py-0.5 text-center text-xs dark:border-slate-500 dark:bg-slate-800"
                />
                <span>/ {timelineSlides.length}</span>
              </span>
              <button
                type="button"
                disabled={timelineStep >= timelineSlides.length - 1}
                class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                onClick={() =>
                  setTimelineStep((prev) =>
                    Math.min(timelineSlides.length - 1, prev + 1),
                  )
                }
              >
                Newer
              </button>
            </div>

            {timelineCurrent ? (
              <div class="p-2">
                <div class="mb-2 grid gap-1 text-xs text-slate-600 md:grid-cols-2 dark:text-slate-300">
                  <span>Action: {timelineCurrent.action}</span>
                  <span>Stage: {timelineCurrent.stage}</span>
                  <span>Actor: {timelineCurrent.actor}</span>
                  <span>Reason: {timelineCurrent.reason}</span>
                  <span>Range: {timelineCurrent.range}</span>
                  <span>Effect: {timelineCurrent.effect}</span>
                </div>

                <pre class="max-h-[360px] overflow-auto border border-slate-300 bg-white p-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-800">
                  {timelineCurrent.diff.map((line) => (
                    <div
                      key={line.key}
                      class={
                        line.type === "add"
                          ? "bg-emerald-50 text-emerald-800 dark:bg-emerald-900/20 dark:text-emerald-200"
                          : line.type === "remove"
                            ? "bg-rose-50 text-rose-800 dark:bg-rose-900/20 dark:text-rose-200"
                            : "text-slate-700 dark:text-slate-300"
                      }
                    >
                      {line.type === "add"
                        ? "+ "
                        : line.type === "remove"
                          ? "- "
                          : "  "}
                      {line.text || " "}
                    </div>
                  ))}
                </pre>
              </div>
            ) : null}
          </>
        ) : null}
      </details>
    </main>
  );
};
