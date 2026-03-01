import { Response } from "express";
import { ErrorResponse, RequestWithAuth } from "./types";
import { MonitorCancelResponse } from "./monitor-types";
import { cancelMonitorJob } from "../../services/monitor/service";

export async function monitorCancelController(
  req: RequestWithAuth<
    { jobId: string },
    MonitorCancelResponse | ErrorResponse,
    undefined
  >,
  res: Response<MonitorCancelResponse | ErrorResponse>,
) {
  const cancelled = cancelMonitorJob(req.params.jobId, req.auth.team_id);
  if (!cancelled) {
    return res.status(404).json({
      success: false,
      error: "Monitor job not found",
    });
  }

  return res.status(200).json({
    success: true,
    status: "cancelled",
  });
}
