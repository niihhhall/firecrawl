import type {
  MonitorJob,
  MonitorOptions,
  MonitorResponse,
  ScrapeOptions,
} from "../types";
import { HttpClient } from "../utils/httpClient";
import { ensureValidScrapeOptions } from "../utils/validation";
import { normalizeAxiosError, throwForBadResponse } from "../utils/errorHandler";

function hasChangeTrackingFormat(scrapeOptions: ScrapeOptions): boolean {
  if (!Array.isArray(scrapeOptions.formats)) {
    return false;
  }

  return scrapeOptions.formats.some(format => {
    if (typeof format === "string") {
      return format === "changeTracking";
    }

    return (format as { type?: string }).type === "changeTracking";
  });
}

function prepareMonitorPayload(request: MonitorOptions): Record<string, unknown> {
  if (!Array.isArray(request.urls) || request.urls.length === 0) {
    throw new Error("urls must be a non-empty array");
  }

  if (!request.scrapeOptions) {
    throw new Error("scrapeOptions is required");
  }

  ensureValidScrapeOptions(request.scrapeOptions);

  if (!hasChangeTrackingFormat(request.scrapeOptions)) {
    throw new Error("scrapeOptions.formats must include changeTracking");
  }

  const payload: Record<string, unknown> = {
    urls: request.urls,
    scrapeOptions: request.scrapeOptions,
  };

  if (request.interval != null) {
    payload.interval = request.interval;
  }
  if (request.webhook != null) {
    payload.webhook = request.webhook;
  }
  if (request.origin != null) {
    payload.origin = request.origin;
  }
  if (request.integration != null) {
    payload.integration = request.integration;
  }

  return payload;
}

export async function startMonitor(
  http: HttpClient,
  request: MonitorOptions,
): Promise<MonitorResponse> {
  try {
    const payload = prepareMonitorPayload(request);
    const res = await http.post<{ success: boolean; id: string; url: string }>(
      "/v2/monitor",
      payload,
    );
    if (res.status !== 200 || !res.data?.success) {
      throwForBadResponse(res, "start monitor");
    }

    return {
      id: res.data.id,
      url: res.data.url,
    };
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "start monitor");
    throw err;
  }
}

export async function getMonitorStatus(
  http: HttpClient,
  jobId: string,
): Promise<MonitorJob> {
  try {
    const res = await http.get<
      MonitorJob & {
        success: boolean;
      }
    >(`/v2/monitor/${jobId}`);
    if (res.status !== 200 || !res.data?.success) {
      throwForBadResponse(res, "get monitor status");
    }

    return {
      id: res.data.id,
      status: res.data.status,
      urls: res.data.urls || [],
      resolvedUrls: res.data.resolvedUrls || [],
      interval: res.data.interval,
      intervalMs: res.data.intervalMs,
      createdAt: res.data.createdAt,
      updatedAt: res.data.updatedAt,
      nextRunAt: res.data.nextRunAt,
      lastRunAt: res.data.lastRunAt,
      latestData: res.data.latestData || [],
      latestDataAt: res.data.latestDataAt,
      lastError: res.data.lastError,
    };
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "get monitor status");
    throw err;
  }
}

export async function cancelMonitor(
  http: HttpClient,
  jobId: string,
): Promise<boolean> {
  try {
    const res = await http.delete<{ success?: boolean; status?: string }>(
      `/v2/monitor/${jobId}`,
    );
    if (res.status !== 200) {
      throwForBadResponse(res, "cancel monitor");
    }

    return res.data?.status === "cancelled";
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "cancel monitor");
    throw err;
  }
}
