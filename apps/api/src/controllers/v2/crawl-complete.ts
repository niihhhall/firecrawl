import { Response } from "express";
import { logger } from "../../lib/logger";
import { getCrawl } from "../../lib/crawl-redis";
import * as Sentry from "@sentry/node";
import { configDotenv } from "dotenv";
import { RequestWithAuth } from "./types";
import { crawlFinishedQueue, crawlGroup } from "../../services/worker/nuq";
configDotenv();

export async function crawlCompleteController(
  req: RequestWithAuth<{ jobId: string }>,
  res: Response,
) {
  try {
    const sc = await getCrawl(req.params.jobId);
    if (!sc) {
      return res.status(404).json({ error: "Job not found" });
    }

    if (sc.team_id !== req.auth.team_id) {
      return res.status(403).json({ error: "Unauthorized" });
    }

    const group = await crawlGroup.getGroup(req.params.jobId);
    if (!group) {
      return res.status(404).json({ error: "Job not found" });
    }

    if (group.status === "completed") {
      return res.status(409).json({ error: "Crawl is already completed" });
    }

    // Use the crawl id as the finish job id so repeated calls dedupe at the
    // queue_crawl_finished PK and don't enqueue duplicate finish work.
    await crawlFinishedQueue.addJobIfNotExists(
      req.params.jobId,
      {},
      {
        priority: 0,
        ownerId: req.auth.team_id,
        groupId: req.params.jobId,
      },
    );

    res.json({
      status: "completing",
    });
  } catch (error) {
    Sentry.captureException(error);
    logger.error(error);
    return res.status(500).json({ error: error.message });
  }
}
