import { v7 as uuidv7 } from "uuid";
import { config } from "../../config";
import { logger as _logger } from "../../lib/logger";
import { getMapResults } from "../../lib/map-utils";
import { ScrapeJobData } from "../../types";
import { createWebhookSender, WebhookEvent } from "../webhook";
import { NuQJob } from "../worker/nuq";
import { processJobInternal } from "../worker/scrape-worker";
import { Document, ScrapeOptions, TeamFlags } from "../../controllers/v2/types";
import {
  MonitorChangedGroup,
  MonitorStatus,
  parseMonitorIntervalToMs,
} from "../../controllers/v2/monitor-types";

type ChangeStatus = "new" | "same" | "changed" | "removed";

type MonitorSnapshot = {
  markdown: string;
  statusCode: number | null;
  scrapedAt: string;
};

type MonitorSnapshots = Map<string, MonitorSnapshot>;

export type StoredMonitorJob = {
  id: string;
  teamId: string;
  status: MonitorStatus;
  urls: string[];
  resolvedUrls: string[];
  interval: string;
  intervalMs: number;
  scrapeOptions: ScrapeOptions;
  webhook?: {
    url: string;
    headers: Record<string, string>;
    metadata: Record<string, string>;
    events: string[];
  };
  hasBaseline: boolean;
  createdAt: string;
  updatedAt: string;
  nextRunAt: string | null;
  lastRunAt: string | null;
  latestData: MonitorChangedGroup[];
  latestDataAt: string | null;
  lastError: string | null;
  origin: string;
  integration: string | null;
  apiKeyId: number | null;
  zeroDataRetention: boolean;
  teamFlags: TeamFlags | null;
};

type CreateMonitorInput = {
  teamId: string;
  urls: string[];
  interval: string;
  scrapeOptions: ScrapeOptions;
  webhook?: StoredMonitorJob["webhook"];
  origin?: string;
  integration?: string | null;
  apiKeyId: number | null;
  zeroDataRetention: boolean;
  teamFlags: TeamFlags | null;
};

const monitorJobs = new Map<string, StoredMonitorJob>();
const monitorSnapshots = new Map<string, MonitorSnapshots>();
const activeRunLocks = new Set<string>();
let schedulerLoop: NodeJS.Timeout | null = null;
let schedulerSweepInProgress = false;

const MONITOR_SCHEDULER_TICK_MS = 5_000;
const MONITOR_MAP_LIMIT = 1_000;

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

function ensureSchedulerStarted() {
  if (schedulerLoop) {
    return;
  }

  schedulerLoop = setInterval(() => {
    void runDueMonitorChecks();
  }, MONITOR_SCHEDULER_TICK_MS);

  if (typeof schedulerLoop.unref === "function") {
    schedulerLoop.unref();
  }

  _logger.info("Started monitor scheduler loop", {
    module: "monitor",
    tickMs: MONITOR_SCHEDULER_TICK_MS,
  });
}

export function stopMonitorScheduler() {
  if (!schedulerLoop) {
    return;
  }

  clearInterval(schedulerLoop);
  schedulerLoop = null;
}

function deriveSourceHost(document: Document): string {
  const candidate =
    document.metadata?.sourceURL ?? document.metadata?.url ?? "unknown";
  try {
    return new URL(candidate).hostname;
  } catch {
    return "unknown";
  }
}

function deriveDocumentUrl(document: Document, fallbackUrl: string): string {
  return document.metadata?.sourceURL ?? document.metadata?.url ?? fallbackUrl;
}

function inferChangeStatus(
  document: Document,
  previous: MonitorSnapshot | undefined,
): ChangeStatus {
  const fromChangeTracking = (
    document.changeTracking as { changeStatus?: ChangeStatus } | undefined
  )?.changeStatus;
  if (
    fromChangeTracking &&
    ["new", "same", "changed", "removed"].includes(fromChangeTracking)
  ) {
    return fromChangeTracking;
  }

  const currentStatusCode = document.metadata?.statusCode ?? null;
  if (currentStatusCode === 404) {
    return previous ? "removed" : "new";
  }

  if (!previous) {
    return "new";
  }

  if (previous.markdown !== (document.markdown ?? "")) {
    return "changed";
  }

  if (previous.statusCode !== currentStatusCode) {
    return "changed";
  }

  return "same";
}

function withChangeTracking(
  document: Document,
  previous: MonitorSnapshot | undefined,
  changeStatus: ChangeStatus,
): Document {
  const existing =
    (document.changeTracking as Record<string, unknown> | undefined) ?? {};
  return {
    ...document,
    changeTracking: {
      ...existing,
      previousScrapeAt: previous?.scrapedAt ?? null,
      changeStatus,
      visibility:
        (existing.visibility as "visible" | "hidden" | undefined) ?? "visible",
    },
  };
}

function buildSingleScrapeJob(
  monitorJob: StoredMonitorJob,
  url: string,
): NuQJob<ScrapeJobData> {
  return {
    id: uuidv7(),
    status: "active",
    createdAt: new Date(),
    priority: 10,
    data: {
      mode: "single_urls",
      url,
      team_id: monitorJob.teamId,
      scrapeOptions: monitorJob.scrapeOptions,
      internalOptions: {
        teamId: monitorJob.teamId,
        disableSmartWaitCache: true,
        bypassBilling: true,
        saveScrapeResultToGCS: config.GCS_FIRE_ENGINE_BUCKET_NAME ? true : false,
        zeroDataRetention: monitorJob.zeroDataRetention,
        teamFlags: monitorJob.teamFlags ?? undefined,
      },
      origin: monitorJob.origin,
      integration: monitorJob.integration,
      skipNuq: true,
      startTime: Date.now(),
      zeroDataRetention: monitorJob.zeroDataRetention,
      apiKeyId: monitorJob.apiKeyId,
    },
  };
}

async function resolveMonitorUrls(job: StoredMonitorJob): Promise<string[]> {
  const resolved = new Set<string>();

  for (const sourceUrl of job.urls) {
    if (!sourceUrl.endsWith("/*")) {
      resolved.add(sourceUrl);
      continue;
    }

    const baseUrl = sourceUrl.slice(0, -2);
    try {
      const map = await getMapResults({
        url: baseUrl,
        limit: MONITOR_MAP_LIMIT,
        includeSubdomains: true,
        crawlerOptions: { sitemap: "include" },
        teamId: job.teamId,
        allowExternalLinks: false,
        filterByPath: true,
        flags: job.teamFlags,
        useIndex: true,
        ignoreCache: true,
      });

      for (const entry of map.mapResults) {
        resolved.add(entry.url);
      }

      if (map.mapResults.length === 0) {
        resolved.add(baseUrl);
      }
    } catch (error) {
      _logger.warn("Failed to resolve wildcard monitor URL", {
        module: "monitor",
        monitorId: job.id,
        sourceUrl,
        error,
      });
      resolved.add(baseUrl);
    }
  }

  return Array.from(resolved);
}

async function sendMonitorWebhook<T extends WebhookEvent>(
  job: StoredMonitorJob,
  event: T,
  payload: any,
) {
  const sender = await createWebhookSender({
    teamId: job.teamId,
    jobId: job.id,
    webhook: job.webhook as any,
    v0: false,
  });
  if (!sender) {
    return;
  }

  try {
    await sender.send(event, payload);
  } catch (error) {
    _logger.warn("Monitor webhook delivery failed", {
      module: "monitor",
      monitorId: job.id,
      event,
      error,
    });
  }
}

async function runMonitorCheck(jobId: string) {
  const monitorJob = monitorJobs.get(jobId);
  if (!monitorJob || monitorJob.status !== "active") {
    return;
  }

  const logger = _logger.child({
    module: "monitor",
    method: "runMonitorCheck",
    monitorId: jobId,
    teamId: monitorJob.teamId,
  });

  const now = new Date().toISOString();
  const snapshots = monitorSnapshots.get(jobId) ?? new Map<string, MonitorSnapshot>();

  let resolvedUrls: string[] = [];
  const changedDocuments: Document[] = [];
  const errors: string[] = [];

  try {
    resolvedUrls = await resolveMonitorUrls(monitorJob);

    for (const resolvedUrl of resolvedUrls) {
      try {
        const scrapeJob = buildSingleScrapeJob(monitorJob, resolvedUrl);
        const scraped = await processJobInternal(scrapeJob);
        if (!scraped) {
          continue;
        }

        const document = scraped as Document;
        const canonicalUrl = deriveDocumentUrl(document, resolvedUrl);
        const previous = snapshots.get(canonicalUrl);
        const changeStatus = inferChangeStatus(document, previous);
        const normalizedDocument = withChangeTracking(
          document,
          previous,
          changeStatus,
        );

        snapshots.set(canonicalUrl, {
          markdown: normalizedDocument.markdown ?? "",
          statusCode: normalizedDocument.metadata?.statusCode ?? null,
          scrapedAt: now,
        });

        if (monitorJob.hasBaseline && changeStatus !== "same") {
          changedDocuments.push(normalizedDocument);
        }
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Unknown monitor scrape error";
        errors.push(`${resolvedUrl}: ${message}`);
      }
    }
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown monitor cycle error";
    errors.push(message);
    logger.warn("Monitor check failed", { error });
  }

  const groupedBySource = new Map<string, Document[]>();
  for (const doc of changedDocuments) {
    const source = deriveSourceHost(doc);
    const group = groupedBySource.get(source) ?? [];
    group.push(doc);
    groupedBySource.set(source, group);
  }

  const groupedChanges: MonitorChangedGroup[] = Array.from(
    groupedBySource.entries(),
  ).map(([source, pages]) => ({
    source,
    pages,
  }));

  monitorJob.hasBaseline = true;
  monitorJob.lastRunAt = now;
  monitorJob.nextRunAt =
    monitorJob.status === "active"
      ? new Date(Date.now() + monitorJob.intervalMs).toISOString()
      : null;
  monitorJob.updatedAt = now;
  monitorJob.resolvedUrls = resolvedUrls;
  monitorJob.lastError = errors.length > 0 ? errors.join("; ") : null;

  if (groupedChanges.length > 0) {
    monitorJob.latestData = groupedChanges;
    monitorJob.latestDataAt = now;
  }

  monitorSnapshots.set(jobId, snapshots);
  monitorJobs.set(jobId, monitorJob);

  if (groupedChanges.length > 0) {
    for (const groupedChange of groupedChanges) {
      await sendMonitorWebhook(monitorJob, WebhookEvent.MONITOR_CHANGED, {
        success: true,
        data: [groupedChange],
      });
    }
  }

  if (errors.length > 0) {
    await sendMonitorWebhook(monitorJob, WebhookEvent.MONITOR_ERROR, {
      success: false,
      error: "One or more monitor URLs failed during this check",
      data: errors.map(error => ({ error })),
    });
  }
}

async function runDueMonitorChecks() {
  if (schedulerSweepInProgress) {
    return;
  }

  schedulerSweepInProgress = true;
  const now = Date.now();

  try {
    for (const monitorJob of monitorJobs.values()) {
      if (monitorJob.status !== "active" || !monitorJob.nextRunAt) {
        continue;
      }

      if (new Date(monitorJob.nextRunAt).getTime() > now) {
        continue;
      }

      if (activeRunLocks.has(monitorJob.id)) {
        continue;
      }

      activeRunLocks.add(monitorJob.id);
      void runMonitorCheck(monitorJob.id).finally(() => {
        activeRunLocks.delete(monitorJob.id);
      });
    }
  } finally {
    schedulerSweepInProgress = false;
  }
}

export async function createMonitorJob(
  input: CreateMonitorInput,
): Promise<StoredMonitorJob> {
  ensureSchedulerStarted();

  const now = new Date().toISOString();
  const id = uuidv7();
  const intervalMs = parseMonitorIntervalToMs(input.interval);

  const job: StoredMonitorJob = {
    id,
    teamId: input.teamId,
    status: "active",
    urls: input.urls,
    resolvedUrls: [],
    interval: input.interval,
    intervalMs,
    scrapeOptions: input.scrapeOptions,
    webhook: input.webhook,
    hasBaseline: false,
    createdAt: now,
    updatedAt: now,
    nextRunAt: now,
    lastRunAt: null,
    latestData: [],
    latestDataAt: null,
    lastError: null,
    origin: input.origin ?? "api",
    integration: input.integration ?? null,
    apiKeyId: input.apiKeyId,
    zeroDataRetention: input.zeroDataRetention,
    teamFlags: input.teamFlags,
  };

  monitorJobs.set(id, job);
  monitorSnapshots.set(id, new Map());

  await sendMonitorWebhook(job, WebhookEvent.MONITOR_STARTED, {
    success: true,
  });

  return clone(job);
}

export function getMonitorJob(
  id: string,
  teamId: string,
): StoredMonitorJob | null {
  const job = monitorJobs.get(id);
  if (!job || job.teamId !== teamId) {
    return null;
  }

  return clone(job);
}

export function cancelMonitorJob(id: string, teamId: string): boolean {
  const job = monitorJobs.get(id);
  if (!job || job.teamId !== teamId) {
    return false;
  }

  const now = new Date().toISOString();
  job.status = "cancelled";
  job.updatedAt = now;
  job.nextRunAt = null;
  monitorJobs.set(id, job);
  return true;
}
