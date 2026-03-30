import { config } from "../../../config";
import {
  TEST_SELF_HOST,
  itIf,
} from "../lib";
import {
  Identity,
  idmux,
  browserCreateRaw,
  browserListRaw,
  browserExecuteRaw,
  browserDeleteRaw,
  scrapeTimeout,
} from "./lib";

const HAS_BROWSER_SERVICE = !!config.BROWSER_SERVICE_URL;

describe("Browser API (firebox-controller)", () => {
  let identity: Identity;

  beforeAll(async () => {
    identity = await idmux({
      name: "browser-api",
      concurrency: 20,
      credits: 1_000_000,
    });
  }, 10000 + scrapeTimeout);

  const canRunBrowserTests = HAS_BROWSER_SERVICE && !TEST_SELF_HOST;

  itIf(canRunBrowserTests)(
    "creates a browser session, executes code, and destroys it",
    async () => {
      const createRes = await browserCreateRaw(
        { ttl: 120, activityTtl: 60 },
        identity,
      );

      expect(createRes.statusCode).toBe(200);
      expect(createRes.body.success).toBe(true);
      expect(typeof createRes.body.id).toBe("string");
      expect(typeof createRes.body.cdpUrl).toBe("string");
      expect(typeof createRes.body.liveViewUrl).toBe("string");
      expect(typeof createRes.body.interactiveLiveViewUrl).toBe("string");
      expect(typeof createRes.body.expiresAt).toBe("string");

      const sessionId = createRes.body.id;

      try {
        const execRes = await browserExecuteRaw(
          sessionId,
          { code: "console.log('hello from firebox')", language: "node", timeout: 30 },
          identity,
        );

        expect(execRes.statusCode).toBe(200);
        expect(execRes.body.success).toBe(true);
        expect(execRes.body.stdout).toContain("hello from firebox");
        expect(execRes.body.exitCode).toBe(0);
        expect(execRes.body.killed).toBe(false);
      } finally {
        const deleteRes = await browserDeleteRaw(sessionId, identity);
        expect(deleteRes.statusCode).toBe(200);
        expect(deleteRes.body.success).toBe(true);
      }
    },
    scrapeTimeout,
  );

  itIf(canRunBrowserTests)(
    "lists browser sessions",
    async () => {
      const createRes = await browserCreateRaw(
        { ttl: 120, activityTtl: 60 },
        identity,
      );
      expect(createRes.statusCode).toBe(200);
      const sessionId = createRes.body.id;

      try {
        const listRes = await browserListRaw(identity);
        expect(listRes.statusCode).toBe(200);
        expect(listRes.body.success).toBe(true);
        expect(Array.isArray(listRes.body.sessions)).toBe(true);

        const found = listRes.body.sessions.find(
          (s: any) => s.id === sessionId,
        );
        expect(found).toBeDefined();
        expect(found.status).toBe("active");
        expect(typeof found.cdpUrl).toBe("string");
        expect(typeof found.liveViewUrl).toBe("string");
        expect(typeof found.interactiveLiveViewUrl).toBe("string");
      } finally {
        await browserDeleteRaw(sessionId, identity);
      }
    },
    scrapeTimeout,
  );

  itIf(!TEST_SELF_HOST)(
    "returns 404 for non-existent session execute",
    async () => {
      const execRes = await browserExecuteRaw(
        "00000000-0000-0000-0000-000000000000",
        { code: "console.log('nope')", language: "node" },
        identity,
      );
      expect(execRes.statusCode).toBe(404);
      expect(execRes.body.success).toBe(false);
    },
  );

  itIf(!TEST_SELF_HOST)(
    "returns 404 for non-existent session delete",
    async () => {
      const deleteRes = await browserDeleteRaw(
        "00000000-0000-0000-0000-000000000000",
        identity,
      );
      expect(deleteRes.statusCode).toBe(404);
      expect(deleteRes.body.success).toBe(false);
    },
  );

  itIf(!TEST_SELF_HOST)(
    "returns 501 when profile parameter is provided",
    async () => {
      const createRes = await browserCreateRaw(
        {
          ttl: 120,
          activityTtl: 60,
          ...({ profile: { name: "test-profile", saveChanges: true } } as any),
        },
        identity,
      );
      expect(createRes.statusCode).toBe(501);
      expect(createRes.body.success).toBe(false);
      expect(createRes.body.error).toContain("not yet supported");
    },
  );
});
