import { Response } from "express";
import { ErrorResponse, RequestWithAuth } from "./types";
import { MonitorStatusResponse } from "./monitor-types";
import { getMonitorJob } from "../../services/monitor/service";

export async function monitorStatusController(
  req: RequestWithAuth<
    { jobId: string },
    MonitorStatusResponse | ErrorResponse,
    undefined
  >,
  res: Response<MonitorStatusResponse | ErrorResponse>,
) {
  const monitor = getMonitorJob(req.params.jobId, req.auth.team_id);
  if (!monitor) {
    return res.status(404).json({
      success: false,
      error: "Monitor job not found",
    });
  }

  return res.status(200).json({
    success: true,
    id: monitor.id,
    status: monitor.status,
    urls: monitor.urls,
    resolvedUrls: monitor.resolvedUrls,
    interval: monitor.interval,
    intervalMs: monitor.intervalMs,
    createdAt: monitor.createdAt,
    updatedAt: monitor.updatedAt,
    nextRunAt: monitor.nextRunAt,
    lastRunAt: monitor.lastRunAt,
    latestData: monitor.latestData,
    latestDataAt: monitor.latestDataAt,
    lastError: monitor.lastError,
  });
}
