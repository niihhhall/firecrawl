import {
  type Document,
  type ScrapeBrowserDeleteResponse,
  type ScrapeExecuteRequest,
  type ScrapeExecuteResponse,
  type ScrapeOptions,
} from "../types";
import { HttpClient } from "../utils/httpClient";
import { ensureValidScrapeOptions } from "../utils/validation";
import { throwForBadResponse, normalizeAxiosError } from "../utils/errorHandler";

export async function scrape(http: HttpClient, url: string, options?: ScrapeOptions): Promise<Document> {
  if (!url || !url.trim()) {
    throw new Error("URL cannot be empty");
  }
  if (options) ensureValidScrapeOptions(options);

  const payload: Record<string, unknown> = { url: url.trim() };
  if (options) Object.assign(payload, options);

  try {
    const res = await http.post<{ success: boolean; data?: Document; error?: string }>("/v2/scrape", payload);
    if (res.status !== 200 || !res.data?.success) {
      throwForBadResponse(res, "scrape");
    }
    return (res.data.data || {}) as Document;
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "scrape");
    throw err;
  }
}

export async function scrapeExecute(
  http: HttpClient,
  jobId: string,
  args: ScrapeExecuteRequest
): Promise<ScrapeExecuteResponse> {
  if (!jobId || !jobId.trim()) {
    throw new Error("Job ID cannot be empty");
  }
  if (!args?.code || !args.code.trim()) {
    throw new Error("Code cannot be empty");
  }

  const body: Record<string, unknown> = {
    code: args.code,
    language: args.language ?? "node",
  };
  if (args.timeout != null) body.timeout = args.timeout;
  if (args.origin) body.origin = args.origin;

  try {
    const res = await http.post<ScrapeExecuteResponse>(
      `/v2/scrape/${jobId}/execute`,
      body
    );
    if (res.status !== 200) throwForBadResponse(res, "execute scrape browser code");
    return res.data;
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "execute scrape browser code");
    throw err;
  }
}

export async function deleteScrapeBrowser(
  http: HttpClient,
  jobId: string
): Promise<ScrapeBrowserDeleteResponse> {
  if (!jobId || !jobId.trim()) {
    throw new Error("Job ID cannot be empty");
  }

  try {
    const res = await http.delete<ScrapeBrowserDeleteResponse>(
      `/v2/scrape/${jobId}/browser`
    );
    if (res.status !== 200) throwForBadResponse(res, "delete scrape browser session");
    return res.data;
  } catch (err: any) {
    if (err?.isAxiosError) return normalizeAxiosError(err, "delete scrape browser session");
    throw err;
  }
}

