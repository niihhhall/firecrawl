#!/usr/bin/env node

const http = require("node:http");
const { URL } = require("node:url");

const PORT = Number(process.env.MONITOR_DEMO_PORT || 8788);

let content = "# Monitor demo\n\nCurrent value: v1";
const webhookEvents = [];

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function sendHtml(res, statusCode, body) {
  res.writeHead(statusCode, {
    "Content-Type": "text/html; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", chunk => chunks.push(chunk));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(new Error("Body must be valid JSON"));
      }
    });
    req.on("error", reject);
  });
}

function buildContentPage() {
  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Monitor local demo</title>
  </head>
  <body>
    <main>
      <h1>Monitor local demo page</h1>
      <pre id="content">${content}</pre>
    </main>
  </body>
</html>`;
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url || "/", `http://${req.headers.host}`);
  const method = req.method || "GET";

  try {
    if (method === "GET" && requestUrl.pathname === "/health") {
      sendJson(res, 200, { ok: true });
      return;
    }

    if (method === "GET" && requestUrl.pathname === "/content") {
      sendHtml(res, 200, buildContentPage());
      return;
    }

    if (method === "POST" && requestUrl.pathname === "/content") {
      const body = await readRequestBody(req);
      if (typeof body.content !== "string" || body.content.length === 0) {
        sendJson(res, 400, {
          ok: false,
          error: "Expected JSON body with non-empty string field `content`",
        });
        return;
      }

      content = body.content;
      sendJson(res, 200, { ok: true, content });
      return;
    }

    if (method === "POST" && requestUrl.pathname === "/content/toggle") {
      content = content.includes("v1")
        ? "# Monitor demo\n\nCurrent value: v2"
        : "# Monitor demo\n\nCurrent value: v1";
      sendJson(res, 200, { ok: true, content });
      return;
    }

    if (method === "POST" && requestUrl.pathname === "/webhook") {
      const body = await readRequestBody(req);
      webhookEvents.push({
        receivedAt: new Date().toISOString(),
        body,
      });
      sendJson(res, 200, { ok: true, received: webhookEvents.length });
      return;
    }

    if (method === "GET" && requestUrl.pathname === "/webhooks") {
      sendJson(res, 200, {
        count: webhookEvents.length,
        events: webhookEvents,
      });
      return;
    }

    if (method === "DELETE" && requestUrl.pathname === "/webhooks") {
      webhookEvents.length = 0;
      sendJson(res, 200, { ok: true });
      return;
    }

    sendJson(res, 404, { ok: false, error: "Not found" });
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`monitor-local-test-server listening on http://127.0.0.1:${PORT}`);
  console.log("Endpoints:");
  console.log("  GET    /health");
  console.log("  GET    /content");
  console.log("  POST   /content         {\"content\":\"...\"}");
  console.log("  POST   /content/toggle");
  console.log("  POST   /webhook");
  console.log("  GET    /webhooks");
  console.log("  DELETE /webhooks");
});
