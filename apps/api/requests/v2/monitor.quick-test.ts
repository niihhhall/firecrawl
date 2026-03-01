import { config as loadEnv } from "dotenv";

type MonitorChangedPage = {
  metadata?: {
    sourceURL?: string;
    url?: string;
  };
  changeTracking?: {
    changeStatus?: string;
    previousScrapeAt?: string | null;
  };
};

type MonitorStatusResponse = {
  success: boolean;
  id: string;
  status: "active" | "cancelled";
  interval: string;
  lastRunAt: string | null;
  nextRunAt: string | null;
  latestData: Array<{
    source: string;
    pages: MonitorChangedPage[];
  }>;
  lastError: string | null;
};

type MonitorCreateResponse = {
  success: boolean;
  id: string;
  url: string;
};

type MonitorCancelResponse = {
  success: boolean;
  status: "cancelled";
};

type CliOptions = {
  baseUrl: string;
  targetUrl: string;
  interval: string;
  pollMs: number;
  token?: string;
  keep: boolean;
  waitSecondRun: boolean;
};

function parseIntervalToMs(interval: string): number {
  const normalized = interval.trim().toLowerCase();
  const match = normalized.match(/^(\d+)([mh])$/);
  if (!match) {
    throw new Error(
      "Interval must match formats like 5m, 30m, 1h, or 24h (example: --interval 5m)",
    );
  }

  const value = Number(match[1]);
  const unit = match[2];
  const multiplier = unit === "m" ? 60_000 : 3_600_000;
  return value * multiplier;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    baseUrl: "http://localhost:3102",
    targetUrl: "https://firecrawl.dev",
    interval: "5m",
    pollMs: 5_000,
    keep: false,
    waitSecondRun: true,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const next = argv[i + 1];

    if ((arg === "--base-url" || arg === "-b") && next) {
      options.baseUrl = next;
      i++;
      continue;
    }
    if ((arg === "--target" || arg === "-t") && next) {
      options.targetUrl = next;
      i++;
      continue;
    }
    if ((arg === "--interval" || arg === "-i") && next) {
      options.interval = next;
      i++;
      continue;
    }
    if (arg === "--poll-ms" && next) {
      options.pollMs = Number(next);
      i++;
      continue;
    }
    if (arg === "--token" && next) {
      options.token = next;
      i++;
      continue;
    }
    if (arg === "--keep") {
      options.keep = true;
      continue;
    }
    if (arg === "--no-wait-second-run") {
      options.waitSecondRun = false;
      continue;
    }
  }

  if (!Number.isFinite(options.pollMs) || options.pollMs <= 0) {
    throw new Error("--poll-ms must be a positive number");
  }

  // Validate early so bad intervals fail fast.
  parseIntervalToMs(options.interval);

  return options;
}

function maskToken(token: string): string {
  if (token.length <= 10) {
    return "***";
  }
  return `${token.slice(0, 6)}...${token.slice(-4)}`;
}

async function sleep(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms));
}

async function requestJson<T>(
  url: string,
  init?: RequestInit,
  retries = 3,
): Promise<{
  status: number;
  data: T;
}> {
  let lastError: unknown = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, init);
      const rawText = await res.text();
      let parsed: T;
      try {
        parsed = (rawText ? JSON.parse(rawText) : {}) as T;
      } catch {
        throw new Error(`Non-JSON response (${res.status}) from ${url}: ${rawText}`);
      }
      return { status: res.status, data: parsed };
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        await sleep(750);
      }
    }
  }

  const message =
    lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`Request failed after ${retries} attempts (${url}): ${message}`);
}

async function waitForStatus(
  opts: CliOptions,
  token: string,
  monitorId: string,
  predicate: (status: MonitorStatusResponse) => boolean,
  timeoutMs: number,
  label: string,
): Promise<MonitorStatusResponse> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const statusRes = await requestJson<MonitorStatusResponse>(
      `${opts.baseUrl}/v2/monitor/${monitorId}`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      },
    );

    if (statusRes.status !== 200 || !statusRes.data.success) {
      throw new Error(
        `Failed monitor status check (${statusRes.status}): ${JSON.stringify(statusRes.data)}`,
      );
    }

    if (predicate(statusRes.data)) {
      return statusRes.data;
    }

    await sleep(opts.pollMs);
  }

  throw new Error(`Timed out waiting for ${label}`);
}

function printChangeSummary(status: MonitorStatusResponse): void {
  if (!Array.isArray(status.latestData) || status.latestData.length === 0) {
    console.log("No changed pages in latestData.");
    return;
  }

  const firstGroup = status.latestData[0];
  const firstPage = firstGroup.pages?.[0];
  const pageUrl =
    firstPage?.metadata?.sourceURL ?? firstPage?.metadata?.url ?? "unknown";
  const changeStatus = firstPage?.changeTracking?.changeStatus ?? "unknown";
  const previousScrapeAt = firstPage?.changeTracking?.previousScrapeAt ?? null;

  console.log(`Changed source: ${firstGroup.source}`);
  console.log(`Changed page:   ${pageUrl}`);
  console.log(`Change status:  ${changeStatus}`);
  console.log(`Previous run:   ${previousScrapeAt}`);
}

async function main() {
  loadEnv({ path: ".env.local" });
  loadEnv({ path: ".env" });

  const opts = parseArgs(process.argv.slice(2));
  const token =
    opts.token ||
    process.env.MONITOR_TEST_TOKEN ||
    process.env.PREVIEW_TOKEN ||
    process.env.TEST_API_KEY;

  if (!token) {
    throw new Error(
      "Missing token. Set PREVIEW_TOKEN/TEST_API_KEY in env, or pass --token.",
    );
  }

  const intervalMs = parseIntervalToMs(opts.interval);
  const firstRunTimeoutMs = 3 * 60_000;
  const secondRunTimeoutMs = intervalMs + 3 * 60_000;

  console.log("Monitor quick test");
  console.log("------------------");
  console.log(`Base URL:    ${opts.baseUrl}`);
  console.log(`Target URL:  ${opts.targetUrl}`);
  console.log(`Interval:    ${opts.interval}`);
  console.log(`Auth token:  ${maskToken(token)}`);
  console.log("");

  let monitorId: string | null = null;

  try {
    console.log("[1/5] Creating monitor...");
    const createRes = await requestJson<MonitorCreateResponse>(
      `${opts.baseUrl}/v2/monitor`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          urls: [opts.targetUrl],
          interval: opts.interval,
          scrapeOptions: {
            formats: [
              "markdown",
              {
                type: "changeTracking",
                modes: ["git-diff"],
              },
            ],
            onlyMainContent: true,
          },
        }),
      },
    );

    if (createRes.status !== 200 || !createRes.data.success) {
      throw new Error(
        `Create monitor failed (${createRes.status}): ${JSON.stringify(createRes.data)}`,
      );
    }

    monitorId = createRes.data.id;
    console.log(`✓ Monitor created: ${monitorId}`);
    console.log(`  URL: ${createRes.data.url}`);
    console.log("");

    console.log("[2/5] Waiting for first run (baseline)...");
    const firstRun = await waitForStatus(
      opts,
      token,
      monitorId,
      status => Boolean(status.lastRunAt),
      firstRunTimeoutMs,
      "first run",
    );
    console.log(`✓ First run completed at: ${firstRun.lastRunAt}`);
    console.log(`  latestData groups: ${firstRun.latestData.length}`);
    console.log(`  lastError: ${firstRun.lastError ?? "null"}`);
    console.log("");

    if (!opts.waitSecondRun) {
      console.log(
        "[3/5] Skipping second-run wait (--no-wait-second-run was set).",
      );
      console.log("");
    } else {
      console.log(
        `[3/5] Waiting for second run (~${opts.interval}) to check for changes...`,
      );
      const firstRunAt = firstRun.lastRunAt;
      const secondRun = await waitForStatus(
        opts,
        token,
        monitorId,
        status => Boolean(status.lastRunAt && status.lastRunAt !== firstRunAt),
        secondRunTimeoutMs,
        "second run",
      );
      console.log(`✓ Second run completed at: ${secondRun.lastRunAt}`);
      console.log(`  latestData groups: ${secondRun.latestData.length}`);
      console.log(`  lastError: ${secondRun.lastError ?? "null"}`);
      printChangeSummary(secondRun);
      console.log("");
    }

    if (opts.keep) {
      console.log("[4/5] Keeping monitor active (--keep set).");
      console.log(`Done. Monitor ID: ${monitorId}`);
      return;
    }

    console.log("[4/5] Cancelling monitor...");
    const cancelRes = await requestJson<MonitorCancelResponse>(
      `${opts.baseUrl}/v2/monitor/${monitorId}`,
      {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      },
    );

    if (cancelRes.status !== 200 || cancelRes.data.status !== "cancelled") {
      throw new Error(
        `Cancel monitor failed (${cancelRes.status}): ${JSON.stringify(cancelRes.data)}`,
      );
    }
    console.log("✓ Monitor cancelled");
    console.log("");
    console.log("[5/5] Done.");
  } catch (error) {
    console.error("");
    console.error("Monitor quick test failed.");
    if (error instanceof Error) {
      console.error(error.message);
    } else {
      console.error(error);
    }

    if (monitorId && !opts.keep) {
      try {
        await requestJson<MonitorCancelResponse>(
          `${opts.baseUrl}/v2/monitor/${monitorId}`,
          {
            method: "DELETE",
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );
        console.error(`Cleanup: cancelled monitor ${monitorId}`);
      } catch {
        console.error(`Cleanup: failed to cancel monitor ${monitorId}`);
      }
    }

    process.exitCode = 1;
  }
}

void main();
