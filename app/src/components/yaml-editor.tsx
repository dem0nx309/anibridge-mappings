import { useEffect, useRef, useState, useCallback } from "hono/jsx/dom";
import { EditorView, basicSetup } from "codemirror";
import { EditorState } from "@codemirror/state";
import { yaml } from "@codemirror/lang-yaml";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { setDiagnostics } from "@codemirror/lint";
import { tags } from "@lezer/highlight";
import { findMappingLine } from "../utils/edits-yaml";
import { validateEditsYaml } from "../utils/validate-edits";

type ValidationStatus =
  | { kind: "idle" }
  | { kind: "stale" }
  | { kind: "result"; count: number; positions: number[] };

const darkSyntax = HighlightStyle.define(
  [
    { tag: tags.keyword, color: "#c4b5fd" }, // purple-300
    { tag: tags.atom, color: "#fbbf24" }, // amber-400
    { tag: tags.bool, color: "#fbbf24" }, // amber-400
    { tag: tags.number, color: "#34d399" }, // emerald-400
    { tag: tags.string, color: "#86efac" }, // green-300
    { tag: tags.null, color: "#94a3b8" }, // slate-400
    { tag: tags.comment, color: "#64748b", fontStyle: "italic" },
    { tag: tags.variableName, color: "#e2e8f0" }, // slate-200
    { tag: tags.definitionKeyword, color: "#c4b5fd" }, // purple-300
    { tag: tags.propertyName, color: "#93c5fd" }, // blue-300
    { tag: tags.meta, color: "#94a3b8" }, // slate-400
    { tag: tags.tagName, color: "#f9a8d4" }, // pink-300
    { tag: tags.attributeName, color: "#fcd34d" }, // amber-300
    { tag: tags.typeName, color: "#67e8f9" }, // cyan-300
    { tag: tags.operator, color: "#e2e8f0" }, // slate-200
    { tag: tags.punctuation, color: "#94a3b8" }, // slate-400
    { tag: tags.bracket, color: "#94a3b8" }, // slate-400
    { tag: tags.separator, color: "#94a3b8" }, // slate-400
  ],
  { themeType: "dark" },
);

const darkTheme = EditorView.theme(
  {
    "&": { height: "100%", fontSize: "12px", backgroundColor: "#0f172a" },
    ".cm-scroller": { overflow: "auto" },
    ".cm-content": { caretColor: "#93c5fd" },
    "&.cm-focused .cm-cursor": { borderLeftColor: "#93c5fd" },
    "&.cm-focused .cm-selectionBackground, ::selection": {
      backgroundColor: "#1e3a5f",
    },
    ".cm-selectionBackground": { backgroundColor: "#1e3a5f !important" },
    ".cm-gutters": {
      backgroundColor: "#0f172a",
      color: "#475569",
      borderRight: "1px solid #1e293b",
    },
    ".cm-activeLineGutter": { backgroundColor: "#1e293b", color: "#94a3b8" },
    ".cm-activeLine": { backgroundColor: "#1e293b80" },
    ".cm-line": { color: "#cbd5e1" },
    ".cm-matchingBracket": {
      backgroundColor: "#334155",
      color: "#e2e8f0",
      outline: "none",
    },
    ".cm-foldGutter": { color: "#475569" },
    ".cm-tooltip": {
      backgroundColor: "#1e293b",
      border: "1px solid #334155",
      color: "#cbd5e1",
    },
    ".cm-panels": { backgroundColor: "#0f172a", color: "#cbd5e1" },
    ".cm-panels.cm-panels-top": { borderBottom: "1px solid #1e293b" },
    ".cm-searchMatch": {
      backgroundColor: "#854d0e40",
      outline: "1px solid #a16207",
    },
    ".cm-searchMatch.cm-searchMatch-selected": { backgroundColor: "#854d0e80" },
    ".cm-diagnostic": {
      borderLeft: "3px solid #f87171",
      backgroundColor: "#1e293b",
      color: "#fca5a5",
      padding: "4px 8px",
    },
    ".cm-diagnostic-error": { borderLeftColor: "#f87171" },
    ".cm-tooltip.cm-tooltip-lint": {
      backgroundColor: "#1e293b",
      border: "1px solid #334155",
    },
    ".cm-lintRange-error": {
      backgroundImage: "none",
      textDecoration: "wavy underline #f87171",
    },
  },
  { dark: true },
);

const lightTheme = EditorView.theme({
  "&": { height: "100%", fontSize: "12px", backgroundColor: "#ffffff" },
  ".cm-scroller": { overflow: "auto" },
  ".cm-gutters": {
    backgroundColor: "#f8fafc",
    color: "#94a3b8",
    borderRight: "1px solid #e2e8f0",
  },
  ".cm-activeLineGutter": { backgroundColor: "#e2e8f0", color: "#475569" },
  ".cm-activeLine": { backgroundColor: "#f1f5f980" },
  ".cm-selectionBackground": { backgroundColor: "#dbeafe !important" },
  ".cm-matchingBracket": { backgroundColor: "#e2e8f0", outline: "none" },
  ".cm-searchMatch": {
    backgroundColor: "#fef08a80",
    outline: "1px solid #eab308",
  },
  ".cm-searchMatch.cm-searchMatch-selected": { backgroundColor: "#fef08a" },
  ".cm-diagnostic": { borderLeft: "3px solid #ef4444", padding: "4px 8px" },
  ".cm-diagnostic-error": { borderLeftColor: "#ef4444" },
  ".cm-lintRange-error": {
    backgroundImage: "none",
    textDecoration: "wavy underline #ef4444",
  },
});

const MAPPING_KEY_SEPARATOR = "->";

type YamlEditorProps = {
  selectedKey: string | null;
  jumpKey: number;
  yamlContent: string;
  loading: boolean;
  error: string | null;
};

export const YamlEditor = ({
  selectedKey,
  jumpKey,
  yamlContent,
  loading,
  error,
}: YamlEditorProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const [validationStatus, setValidationStatus] = useState<ValidationStatus>({
    kind: "idle",
  });
  const [errorIndex, setErrorIndex] = useState(0);

  const jumpToErrorAt = useCallback(
    (idx: number) => {
      const view = viewRef.current;
      if (!view || validationStatus.kind !== "result") return;
      const pos = validationStatus.positions[idx];
      if (pos == null) return;
      view.dispatch({
        effects: EditorView.scrollIntoView(pos, { y: "center" }),
        selection: { anchor: pos },
      });
      view.focus();
    },
    [validationStatus],
  );

  const runValidation = useCallback(() => {
    const view = viewRef.current;
    if (!view) return;
    const text = view.state.doc.toString();
    const diags = validateEditsYaml(text);
    const positions = diags.map((d) => d.from);
    setValidationStatus({ kind: "result", count: diags.length, positions });
    setErrorIndex(0);
    view.dispatch(setDiagnostics(view.state, diags));
    // Jump to first error
    if (positions.length > 0) {
      view.dispatch({
        effects: EditorView.scrollIntoView(positions[0], { y: "center" }),
        selection: { anchor: positions[0] },
      });
      view.focus();
    }
  }, []);

  // Initialize CodeMirror
  useEffect(() => {
    const container = containerRef.current;
    if (!container || !yamlContent) return;

    const isDark = window.matchMedia("(prefers-color-scheme: dark)").matches;

    const onDocChange = EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        setValidationStatus((prev) =>
          prev.kind === "idle" ? prev : { kind: "stale" },
        );
      }
    });

    const state = EditorState.create({
      doc: yamlContent,
      extensions: [
        basicSetup,
        yaml(),
        isDark ? darkTheme : lightTheme,
        ...(isDark ? [syntaxHighlighting(darkSyntax)] : []),
        onDocChange,
      ],
    });

    const view = new EditorView({ state, parent: container });
    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
  }, [yamlContent]);

  // Jump to selected mapping
  useEffect(() => {
    if (!selectedKey || !viewRef.current || !yamlContent) return;

    const sepIdx = selectedKey.indexOf(MAPPING_KEY_SEPARATOR);
    if (sepIdx < 0) return;

    const source = selectedKey.slice(0, sepIdx);
    const target = selectedKey.slice(sepIdx + MAPPING_KEY_SEPARATOR.length);
    if (!source || !target) return;

    const lineNum = findMappingLine(yamlContent, source, target);
    if (lineNum < 0) return;

    const view = viewRef.current;
    const line = view.state.doc.line(lineNum + 1); // CodeMirror is 1-indexed

    view.dispatch({
      effects: EditorView.scrollIntoView(line.from, { y: "start" }),
      selection: { anchor: line.from },
    });
  }, [selectedKey, jumpKey, yamlContent]);

  return (
    <div class="flex min-h-0 flex-col border border-slate-300 bg-white dark:border-slate-600 dark:bg-[#0f172a]">
      <div class="flex items-center justify-between border-b border-slate-300 bg-slate-100 px-2 py-1.5 dark:border-slate-700 dark:bg-slate-800">
        <div class="flex items-center gap-2">
          <span class="text-[11px] font-semibold uppercase tracking-wide text-slate-600 dark:text-slate-300">
            mappings.edits.yaml
          </span>
          <button
            type="button"
            class="border border-slate-400 bg-white px-1.5 py-0.5 text-[10px] font-medium text-slate-600 hover:border-sky-600 hover:text-sky-700 dark:border-slate-500 dark:bg-slate-700 dark:text-slate-300 dark:hover:border-sky-400 dark:hover:text-sky-300"
            onClick={runValidation}
          >
            Validate
          </button>
          {validationStatus.kind === "result" && (
            <span
              class={`inline-flex items-center gap-1 rounded-sm px-1.5 py-0.5 text-[10px] font-medium ${
                validationStatus.count === 0
                  ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400"
                  : "bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-400"
              }`}
            >
              {validationStatus.count === 0
                ? "valid"
                : `${validationStatus.count} error${validationStatus.count !== 1 ? "s" : ""}`}
            </span>
          )}
          {validationStatus.kind === "result" && validationStatus.count > 0 && (
            <span class="inline-flex items-center gap-0.5">
              <button
                type="button"
                class="border border-slate-400 bg-white px-1 py-0.5 text-[10px] text-slate-600 hover:border-sky-600 hover:text-sky-700 dark:border-slate-500 dark:bg-slate-700 dark:text-slate-300 dark:hover:border-sky-400 dark:hover:text-sky-300"
                onClick={() => {
                  const prev =
                    (errorIndex - 1 + validationStatus.count) %
                    validationStatus.count;
                  setErrorIndex(prev);
                  jumpToErrorAt(prev);
                }}
                title="Previous error"
              >
                ▲
              </button>
              <span class="px-0.5 text-[10px] text-slate-500 dark:text-slate-400">
                {errorIndex + 1}/{validationStatus.count}
              </span>
              <button
                type="button"
                class="border border-slate-400 bg-white px-1 py-0.5 text-[10px] text-slate-600 hover:border-sky-600 hover:text-sky-700 dark:border-slate-500 dark:bg-slate-700 dark:text-slate-300 dark:hover:border-sky-400 dark:hover:text-sky-300"
                onClick={() => {
                  const next = (errorIndex + 1) % validationStatus.count;
                  setErrorIndex(next);
                  jumpToErrorAt(next);
                }}
                title="Next error"
              >
                ▼
              </button>
            </span>
          )}
          {validationStatus.kind === "stale" && (
            <span class="inline-flex items-center gap-1 rounded-sm bg-slate-200 px-1.5 py-0.5 text-[10px] font-medium text-slate-400 dark:bg-slate-700 dark:text-slate-500">
              unchecked changes
            </span>
          )}
        </div>
        <a
          href="https://github.com/anibridge/anibridge-mappings/blob/main/mappings.edits.yaml"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 text-[11px] text-slate-500 hover:text-sky-700 dark:text-slate-400 dark:hover:text-sky-300"
        >
          GitHub
          <svg
            aria-hidden="true"
            viewBox="0 0 24 24"
            class="h-2.5 w-2.5 shrink-0"
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
      </div>

      {loading ? (
        <div class="flex flex-1 items-center justify-center text-xs text-slate-500 dark:text-slate-400">
          Loading YAML...
        </div>
      ) : error ? (
        <div class="flex-1 p-2 text-xs text-rose-600 dark:text-rose-400">
          {error}
        </div>
      ) : (
        <div ref={containerRef} class="min-h-0 flex-1 overflow-hidden" />
      )}
    </div>
  );
};
