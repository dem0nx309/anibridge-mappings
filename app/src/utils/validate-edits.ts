import Ajv2020 from "ajv/dist/2020";
import { parseDocument } from "yaml";
import type { Diagnostic } from "@codemirror/lint";
import schema from "../../../mappings.schema.json";

const ajv = new Ajv2020({ allErrors: true, strict: false });
const validate = ajv.compile(schema);

export const validateEditsYaml = (text: string): Diagnostic[] => {
  const doc = parseDocument(text, { keepSourceTokens: true });
  const diagnostics: Diagnostic[] = [];

  // Surface YAML parse errors first
  for (const err of doc.errors) {
    const [from, to] = err.pos ?? [0, 0];
    diagnostics.push({
      from,
      to: to || from + 1,
      severity: "error",
      message: err.message,
      source: "yaml",
    });
  }

  if (diagnostics.length) return diagnostics;

  const data = doc.toJS();
  const valid = validate(data);
  if (valid) return [];


  const errors = validate.errors ?? [];
  const anyOfPaths = new Set(
    errors
      .filter((e) => e.keyword === "anyOf" || e.keyword === "oneOf")
      .map((e) => e.instancePath),
  );

  const deduped = errors.filter((e) => {
    if (e.keyword === "anyOf" || e.keyword === "oneOf") return true;
    return !anyOfPaths.has(e.instancePath);
  });

  for (const err of deduped) {
    const pathSegments = err.instancePath
      .split("/")
      .filter(Boolean)
      .map((s) => decodeURIComponent(s.replace(/~1/g, "/").replace(/~0/g, "~")));

    // Try to find the node at the error path for precise positioning
    let from = 0;
    let to = text.length;

    const node = pathSegments.length
      ? doc.getIn(pathSegments, true)
      : doc.contents;

    if (node && typeof node === "object" && "range" in node && node.range) {
      const range = node.range as [number, number, number];
      from = range[0];
      to = range[1];
    }

    const path = err.instancePath || "/";
    let msg = err.message ?? "validation error";
    if (err.keyword === "anyOf") {
      msg = "must be a valid range string (e.g. \"1-5\") or null";
    }
    diagnostics.push({
      from,
      to,
      severity: "error",
      message: `${path}: ${msg}`,
      source: "schema",
    });
  }

  return diagnostics;
};
