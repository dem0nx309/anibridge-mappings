const EDITS_URL =
  "https://raw.githubusercontent.com/anibridge/anibridge-mappings/refs/heads/main/mappings.edits.yaml";

const MAPPING_KEY_SEPARATOR = "->";

export const fetchEditsYaml = async (): Promise<string> => {
  const res = await fetch(EDITS_URL);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
};

export const findMappingLine = (
  text: string,
  sourceDescriptor: string,
  targetDescriptor: string,
): number => {
  const lines = text.split("\n");
  let inSource = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (line.length > 0 && line[0] !== " " && line[0] !== "#") {
      if (
        line.startsWith(sourceDescriptor + ":") &&
        (line.length === sourceDescriptor.length + 1 ||
          line[sourceDescriptor.length + 1] === " ")
      ) {
        inSource = true;
        continue;
      }
      inSource = false;
      continue;
    }

    if (inSource) {
      const trimmed = line.trimStart();
      if (
        trimmed.startsWith(targetDescriptor + ":") &&
        (trimmed.length === targetDescriptor.length + 1 ||
          trimmed[targetDescriptor.length + 1] === " ")
      ) {
        return i;
      }
    }
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (
      line[0] !== " " &&
      line.startsWith(sourceDescriptor + ":") &&
      (line.length === sourceDescriptor.length + 1 ||
        line[sourceDescriptor.length + 1] === " ")
    ) {
      return i;
    }
  }

  return -1;
};

export const mappingExistsInEdits = (
  yamlText: string,
  mappingKey: string,
): boolean => {
  const sepIdx = mappingKey.indexOf(MAPPING_KEY_SEPARATOR);
  if (sepIdx < 0) return false;
  const source = mappingKey.slice(0, sepIdx);
  const target = mappingKey.slice(sepIdx + MAPPING_KEY_SEPARATOR.length);
  if (!source || !target) return false;
  return findMappingLine(yamlText, source, target) >= 0;
};
