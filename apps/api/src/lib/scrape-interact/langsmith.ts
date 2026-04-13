import * as ai from "ai";
import { config } from "../../config";
import { logger as _logger } from "../logger";

const logger = _logger.child({ module: "scrape-interact/langsmith" });

export const isLangSmithEnabled = Boolean(
  config.LANGSMITH_API_KEY &&
    (config.LANGSMITH_TRACING === undefined
      ? true
      : config.LANGSMITH_TRACING === true),
);

if (isLangSmithEnabled) {
  // The langsmith SDK reads these env vars at runtime for auth + project routing.
  // We mirror our config into process.env so both the wrapAISDK path and any
  // traceable() calls pick up the same credentials without extra plumbing.
  process.env.LANGSMITH_TRACING = "true";
  process.env.LANGSMITH_API_KEY = config.LANGSMITH_API_KEY!;
  if (config.LANGSMITH_PROJECT) {
    process.env.LANGSMITH_PROJECT = config.LANGSMITH_PROJECT;
  }
  if (config.LANGSMITH_ENDPOINT) {
    process.env.LANGSMITH_ENDPOINT = config.LANGSMITH_ENDPOINT;
  }
}

export type InteractTraceMetadata = {
  thread_id: string;
  session_id: string;
  scrape_id: string;
  team_id: string;
  browser_id?: string;
  run_id?: string;
  mode: "prompt" | "code";
  // When true, the caller has determined the team/scrape is under
  // zero-data-retention and tracing must be skipped entirely so no prompt,
  // code, or tool I/O is shipped to LangSmith.
  zeroDataRetention?: boolean;
};

type WrappedAISDK = {
  generateText: typeof ai.generateText;
  streamText: typeof ai.streamText;
  generateObject: typeof ai.generateObject;
  streamObject: typeof ai.streamObject;
};

type LangSmithProviderOptions = {
  name?: string;
  metadata?: Record<string, unknown>;
  tags?: string[];
};

type TraceableOptions = {
  name?: string;
  run_type?:
    | "tool"
    | "chain"
    | "llm"
    | "retriever"
    | "embedding"
    | "prompt"
    | "parser";
  metadata?: Record<string, unknown>;
  tags?: string[];
};

let wrappedSDK: WrappedAISDK = ai;
let createLangSmithProviderOptionsFn:
  | ((opts: LangSmithProviderOptions) => unknown)
  | null = null;
let traceableFn:
  | (<F extends (...args: any[]) => any>(fn: F, opts?: TraceableOptions) => F)
  | null = null;

if (isLangSmithEnabled) {
  try {
    const vercelWrapper = require("langsmith/experimental/vercel");
    const traceableMod = require("langsmith/traceable");
    wrappedSDK = vercelWrapper.wrapAISDK(ai);
    createLangSmithProviderOptionsFn =
      vercelWrapper.createLangSmithProviderOptions;
    traceableFn = traceableMod.traceable;
    logger.info("LangSmith tracing enabled for interact agent", {
      project: config.LANGSMITH_PROJECT ?? "(default)",
    });
  } catch (err) {
    logger.error(
      "Failed to initialize LangSmith — falling back to raw ai SDK",
      {
        error: err,
      },
    );
  }
}

export const { generateText, streamText, generateObject, streamObject } =
  wrappedSDK;

// The LangSmith provider config is recognized by wrapAISDK but is not a
// first-class AI SDK provider, so it doesn't match SharedV3ProviderOptions.
// We return `any` deliberately so call sites can assign it to providerOptions
// without fighting the type.
export function buildLangSmithProviderOptions(
  meta: InteractTraceMetadata,
  opts: {
    name?: string;
    tags?: string[];
    extra?: Record<string, unknown>;
  } = {},
): any {
  if (
    !isLangSmithEnabled ||
    !createLangSmithProviderOptionsFn ||
    meta.zeroDataRetention
  ) {
    return undefined;
  }

  return createLangSmithProviderOptionsFn({
    name: opts.name,
    metadata: { ...meta, ...(opts.extra ?? {}) },
    tags: ["interact", `mode:${meta.mode}`, ...(opts.tags ?? [])],
  });
}

export function traceInteract<F extends (...args: any[]) => any>(
  fn: F,
  meta: InteractTraceMetadata,
  opts: { name?: string; runType?: TraceableOptions["run_type"] } = {},
): F {
  if (!isLangSmithEnabled || !traceableFn || meta.zeroDataRetention) return fn;

  return traceableFn(fn, {
    name: opts.name ?? `interact:${meta.mode}`,
    run_type: opts.runType ?? "chain",
    metadata: { ...meta },
    tags: ["interact", `mode:${meta.mode}`],
  });
}
