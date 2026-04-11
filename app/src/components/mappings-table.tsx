import type { Dict, MappingFilters } from "../utils/provenance";
import { getDictValue } from "../utils/provenance";
import type { MappingWithId, SortColumn } from "./ui-types";

type MappingsTableProps = {
  dict: Dict;
  rows: MappingWithId[];
  selectedKey: string | null;
  sortLabel: (column: SortColumn) => string;
  onSort: (column: SortColumn) => void;
  onSelect: (mappingKey: string) => void;
  paged: { page: number; pages: number; total: number };
  setFilters: (updater: (prev: MappingFilters) => MappingFilters) => void;
};

export const MappingsTable = ({
  dict,
  rows,
  selectedKey,
  sortLabel,
  onSort,
  onSelect,
  paged,
  setFilters,
}: MappingsTableProps) => {
  return (
    <aside class="flex min-h-0 flex-col border border-slate-300 bg-slate-50 dark:border-slate-600 dark:bg-slate-800">
      <div class="grid grid-cols-[48px_minmax(0,1fr)_minmax(0,1fr)_56px] border-b border-slate-300 bg-slate-200 px-2 py-1 text-[11px] uppercase tracking-[0.03em] text-slate-600 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-300">
        <button type="button" class="text-left" onClick={() => onSort("state")}>
          {sortLabel("state")}
        </button>
        <button
          type="button"
          class="text-left"
          onClick={() => onSort("source")}
        >
          {`SOURCE${sortLabel("source")}`}
        </button>
        <button
          type="button"
          class="text-left"
          onClick={() => onSort("target")}
        >
          {`TARGET${sortLabel("target")}`}
        </button>
        <button
          type="button"
          class="text-right"
          onClick={() => onSort("steps")}
        >
          {`STEPS${sortLabel("steps")}`}
        </button>
      </div>

      <div class="min-h-0 flex-1 overflow-auto">
        {rows.map((mapping) => {
          const source = getDictValue(dict, "descriptors", mapping.s) || "-";
          const target = getDictValue(dict, "descriptors", mapping.t) || "-";
          const steps = mapping.n ?? mapping.ev?.length ?? 0;
          const active = selectedKey === mapping.key;

          return (
            <button
              type="button"
              key={mapping.id}
              class={`grid w-full grid-cols-[48px_minmax(0,1fr)_minmax(0,1fr)_56px] border-b border-slate-200 px-2 py-1 text-left text-[12px] ${
                active
                  ? "bg-sky-100 dark:bg-sky-800/35"
                  : "bg-white hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700"
              }`}
              onClick={() => onSelect(mapping.key)}
            >
              <span class="flex items-center">
                <span
                  class={`inline-block h-2 w-2 rounded-full ${mapping.p ? "bg-emerald-500" : "bg-slate-300 dark:bg-slate-500"}`}
                  title={mapping.p ? "present" : "missing"}
                />
              </span>
              <span class="truncate">{source}</span>
              <span class="truncate">{target}</span>
              <span class="text-right">{steps}</span>
            </button>
          );
        })}
      </div>

      <div class="flex items-center justify-between gap-2 border-t border-slate-300 bg-slate-100 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-700 dark:text-slate-200">
        <button
          type="button"
          disabled={paged.page <= 1}
          class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
          onClick={() =>
            setFilters((prev) => ({
              ...prev,
              page: Math.max(1, prev.page - 1),
            }))
          }
        >
          Prev
        </button>
        <span class="inline-flex items-center gap-0.5">
          <input
            type="number"
            min={1}
            max={paged.pages}
            value={paged.page}
            onInput={(e) => {
              const v = Number((e.target as HTMLInputElement).value);
              if (v >= 1 && v <= paged.pages)
                setFilters((prev) => ({ ...prev, page: v }));
            }}
            class="w-10 border border-slate-400 bg-white px-1 py-0.5 text-center text-xs dark:border-slate-500 dark:bg-slate-800"
          />
          <span>
            /{paged.pages} ({paged.total.toLocaleString()})
          </span>
        </span>
        <button
          type="button"
          disabled={paged.page >= paged.pages}
          class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
          onClick={() =>
            setFilters((prev) => ({
              ...prev,
              page: Math.min(paged.pages, prev.page + 1),
            }))
          }
        >
          Next
        </button>
      </div>
    </aside>
  );
};
