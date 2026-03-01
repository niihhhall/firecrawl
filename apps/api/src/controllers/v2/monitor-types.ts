import { z } from "zod";
import { scrapeOptions, Document } from "./types";
import { createWebhookSchema } from "../../services/webhook/schema";
import { checkUrl, protocolIncluded } from "../../lib/validateUrl";

export const monitorWebhookSchema = createWebhookSchema([
  "started",
  "changed",
  "error",
]);

const monitorIntervalRegex = /^(\d+)([mh])$/i;

function normalizeMonitorUrl(input: string): string {
  const value = input.trim();
  const withProtocol = protocolIncluded(value) ? value : `http://${value}`;

  if (withProtocol.includes("*")) {
    if (!withProtocol.endsWith("/*")) {
      throw new Error("Only trailing /* wildcard patterns are supported");
    }

    const baseUrl = withProtocol.slice(0, -2);
    checkUrl(baseUrl);

    return `${baseUrl.replace(/\/+$/, "")}/*`;
  }

  checkUrl(withProtocol);
  return withProtocol;
}

export function parseMonitorIntervalToMs(interval: string): number {
  const normalized = interval.trim().toLowerCase();
  const match = normalized.match(monitorIntervalRegex);
  if (!match) {
    throw new Error("Interval must match formats like 5m, 30m, 1h, or 24h");
  }

  const value = Number(match[1]);
  const unit = match[2];
  const multiplier = unit === "m" ? 60_000 : 3_600_000;
  const milliseconds = value * multiplier;
  if (milliseconds < 5 * 60_000) {
    throw new Error("Interval must be at least 5m");
  }

  return milliseconds;
}

const monitorIntervalSchema = z
  .string()
  .trim()
  .refine(value => {
    try {
      parseMonitorIntervalToMs(value);
      return true;
    } catch {
      return false;
    }
  }, "Interval must be at least 5m and match formats like 5m, 30m, 1h, or 24h");

function hasChangeTrackingFormat(formats?: unknown[]): boolean {
  if (!Array.isArray(formats)) {
    return false;
  }

  return formats.some(format => {
    if (typeof format === "string") {
      return format === "changeTracking";
    }

    if (format && typeof format === "object") {
      return (format as { type?: string }).type === "changeTracking";
    }

    return false;
  });
}

export const monitorRequestSchema = z.strictObject({
  urls: z
    .array(
      z
        .string()
        .min(1)
        .transform(value => normalizeMonitorUrl(value)),
    )
    .min(1),
  interval: monitorIntervalSchema.default("1h"),
  scrapeOptions: scrapeOptions.refine(
    value => hasChangeTrackingFormat(value.formats),
    "scrapeOptions.formats must include changeTracking",
  ),
  webhook: monitorWebhookSchema.optional(),
  origin: z.string().optional(),
  integration: z.string().nullable().optional(),
});

export type MonitorRequest = z.infer<typeof monitorRequestSchema>;

export type MonitorStatus = "active" | "cancelled";

export type MonitorChangedGroup = {
  source: string;
  pages: Document[];
};

export interface MonitorResponse {
  success: boolean;
  id: string;
  url: string;
}

export interface MonitorStatusResponse {
  success: boolean;
  id: string;
  status: MonitorStatus;
  urls: string[];
  resolvedUrls: string[];
  interval: string;
  intervalMs: number;
  createdAt: string;
  updatedAt: string;
  nextRunAt: string | null;
  lastRunAt: string | null;
  latestData: MonitorChangedGroup[];
  latestDataAt: string | null;
  lastError: string | null;
}

export interface MonitorCancelResponse {
  success: boolean;
  status: "cancelled";
}
