import { config } from "../../config";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BrowserServiceCreateResponse {
  id: string;
  token: string;
  mode: string;
  cdp_url: string;
  selkies_url: string;
  screencast_url: string;
  ttl_seconds: number;
  activity_ttl_seconds: number;
  created_at: string;
  status: string;
}

export interface BrowserServiceExecResponse {
  stdout: string;
  result: string;
  stderr: string;
  exit_code: number;
  killed: boolean;
}

export interface BrowserServiceDeleteResponse {
  status: string;
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

export class BrowserServiceError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

function browserServiceHeaders(
  extra?: Record<string, string>,
): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(extra ?? {}),
  };
  return headers;
}

/**
 * Call the browser service and return parsed JSON.
 * Throws `BrowserServiceError` on non-2xx responses.
 */
export async function browserServiceRequest<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const url = `${config.BROWSER_SERVICE_URL}${path}`;
  const res = await fetch(url, {
    method,
    headers: browserServiceHeaders(),
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new BrowserServiceError(
      res.status,
      `Browser service ${method} ${path} failed (${res.status}): ${text}`,
    );
  }

  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}
