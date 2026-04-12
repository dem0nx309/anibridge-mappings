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

import type { Dict, Mapping } from "../utils/provenance";
import { getDictValue } from "../utils/provenance";

const MAPPING_VIEW_FORMAT_STORAGE_KEY = "anibridge:mapping-view-format";

const getStoredFormat = (): MappingViewFormat => {
  const v = window.localStorage.getItem(MAPPING_VIEW_FORMAT_STORAGE_KEY);
  return v === "yaml" ? "yaml" : "json";
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
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [timelineStep, setTimelineStep] = useState(0);

  const selectedSource = getDictValue(dict, "descriptors", selected.s);
  const selectedTarget = getDictValue(dict, "descriptors", selected.t);
  const selectedSourceExternal = descriptorToExternal(selectedSource);
  const selectedTargetExternal = descriptorToExternal(selectedTarget);

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
