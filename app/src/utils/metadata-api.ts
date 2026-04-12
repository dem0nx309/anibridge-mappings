const API_BASE = "https://metadata.anibridge.eliasbenb.dev";

export type ImageType =
  | "banner"
  | "cover"
  | "poster"
  | "thumbnail"
  | "unknown";

export type EntityType = "movie" | "show";

export type TitleStatus =
  | "upcoming"
  | "ongoing"
  | "finished"
  | "cancelled"
  | "hiatus"
  | "unknown";

export type MetadataTitles = {
  display: string;
  main: string;
  original?: string | null;
  aliases: string[];
  franchise?: string | null;
};

export type MetadataRelease = {
  start_date?: string | null;
  end_date?: string | null;
  status: TitleStatus;
};

export type MetadataRuntime = {
  minutes: number;
  basis: "derived" | "provided";
};

export type MetadataClassification = {
  is_adult: boolean;
  genres: string[];
};

export type MetadataRatings = {
  average?: number | null;
  popularity?: number | null;
};

export type MetadataImageModel = {
  kind: ImageType;
  url: string;
};

export type MetadataId = {
  descriptor: string;
  provider: string;
  provider_id: string;
  scope?: string | null;
};

export type MetadataScope = {
  id: MetadataId;
  titles: MetadataTitles;
  release?: MetadataRelease | null;
  runtime?: MetadataRuntime | null;
  units?: number | null;
};

export type UnifiedMetadata = {
  kind: EntityType;
  id: MetadataId;
  titles: MetadataTitles;
  synopsis?: string | null;
  release?: MetadataRelease | null;
  runtime?: MetadataRuntime | null;
  units?: number | null;
  classification: MetadataClassification;
  ratings?: MetadataRatings | null;
  images: MetadataImageModel[];
  scopes?: Record<string, MetadataScope> | null;
  source?: string | null;
};

export type MetadataEnvelope = {
  metadata: UnifiedMetadata;
  cache: {
    updated_at: string;
    expires_at: string;
    stale: boolean;
    source: "cache" | "stale-cache" | "upstream";
    last_error?: string | null;
  };
};

export type DescriptorParts = {
  descriptor: string;
  provider: string;
  providerId: string;
  scope: string | null;
  baseDescriptor: string;
};

export type ResolvedMetadata = {
  requestedDescriptor: string;
  scope: MetadataScope | null;
  scopeKey: string | null;
  kind: EntityType;
  id: MetadataId;
  titles: MetadataTitles;
  synopsis?: string | null;
  release?: MetadataRelease | null;
  runtime?: MetadataRuntime | null;
  units?: number | null;
  classification: MetadataClassification;
  ratings?: MetadataRatings | null;
  images: MetadataImageModel[];
  source?: string | null;
  base: UnifiedMetadata;
};

const cache = new Map<string, Promise<MetadataEnvelope | null>>();

export const parseDescriptor = (
  descriptor: string,
): DescriptorParts | null => {
  const [provider, providerId, ...scopeParts] = descriptor.split(":");
  if (!provider || !providerId) return null;

  const scope = scopeParts.join(":") || null;
  return {
    descriptor,
    provider,
    providerId,
    scope,
    baseDescriptor: `${provider}:${providerId}`,
  };
};

export const getMetadataScope = (
  metadata: UnifiedMetadata,
  descriptor: string,
): MetadataScope | null => {
  const parsed = parseDescriptor(descriptor);
  if (!parsed?.scope || !metadata.scopes) return null;

  const direct = metadata.scopes[parsed.scope];
  if (direct) return direct;

  for (const scope of Object.values(metadata.scopes)) {
    if (
      scope.id.descriptor === descriptor ||
      scope.id.scope === parsed.scope
    ) {
      return scope;
    }
  }

  return null;
};

export const resolveMetadata = (
  metadata: UnifiedMetadata,
  descriptor: string,
): ResolvedMetadata => {
  const scope = getMetadataScope(metadata, descriptor);

  return {
    requestedDescriptor: descriptor,
    scope,
    scopeKey: scope?.id.scope ?? parseDescriptor(descriptor)?.scope ?? null,
    kind: metadata.kind,
    id: scope?.id ?? metadata.id,
    titles: scope?.titles ?? metadata.titles,
    synopsis: metadata.synopsis,
    release: scope?.release ?? metadata.release,
    runtime: scope?.runtime ?? metadata.runtime,
    units: scope?.units ?? metadata.units,
    classification: metadata.classification,
    ratings: metadata.ratings,
    images: metadata.images,
    source: metadata.source,
    base: metadata,
  };
};

export const fetchMetadata = (
  descriptor: string,
): Promise<MetadataEnvelope | null> => {
  const existing = cache.get(descriptor);
  if (existing) return existing;

  const promise = fetch(`${API_BASE}/api/metadata/${encodeURIComponent(descriptor)}`)
    .then((r) => {
      if (!r.ok) return null;
      return r.json() as Promise<MetadataEnvelope>;
    })
    .catch(() => null);

  cache.set(descriptor, promise);
  return promise;
};

export const getPosterUrl = (
  images: MetadataImageModel[],
): string | null => {
  const poster = images.find((i) => i.kind === "poster");
  return poster?.url ?? images[0]?.url ?? null;
};
