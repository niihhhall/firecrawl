import { Response } from "express";
import { ErrorResponse, RequestWithAuth } from "./types";
import {
  MonitorRequest,
  MonitorResponse,
  monitorRequestSchema,
} from "./monitor-types";
import { checkPermissions } from "../../lib/permissions";
import { createMonitorJob } from "../../services/monitor/service";

export async function monitorController(
  req: RequestWithAuth<{}, MonitorResponse | ErrorResponse, MonitorRequest>,
  res: Response<MonitorResponse | ErrorResponse>,
) {
  req.body = monitorRequestSchema.parse(req.body);

  const permissions = checkPermissions(
    { scrapeOptions: req.body.scrapeOptions },
    req.acuc?.flags,
  );
  if (permissions.error) {
    return res.status(403).json({
      success: false,
      error: permissions.error,
    });
  }

  const monitor = await createMonitorJob({
    teamId: req.auth.team_id,
    urls: req.body.urls,
    interval: req.body.interval,
    scrapeOptions: req.body.scrapeOptions,
    webhook: req.body.webhook,
    origin: req.body.origin,
    integration: req.body.integration,
    apiKeyId: req.acuc?.api_key_id ?? null,
    zeroDataRetention: req.acuc?.flags?.forceZDR || false,
    teamFlags: req.acuc?.flags ?? null,
  });

  return res.status(200).json({
    success: true,
    id: monitor.id,
    url: `${req.protocol}://${req.get("host")}/v2/monitor/${monitor.id}`,
  });
}
