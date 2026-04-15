import { TeamFlags } from "../controllers/v2/types";
import { getScrapeZDR, getIgnoreRobots } from "./zdr-helpers";

type LocationOptions = { country?: string };

interface APIRequest {
  zeroDataRetention?: boolean;
  location?: LocationOptions;
  scrapeOptions?: {
    location?: LocationOptions;
  };
  // crawl-specific fields (flattened from crawlerOptions)
  ignoreRobotsTxt?: boolean;
  robotsUserAgent?: string;
}

const SUPPORT_EMAIL = "support@firecrawl.com";

export function checkPermissions(
  request: APIRequest,
  flags?: TeamFlags,
): { error?: string } {
  // zdr perms — scrapeZDR must be 'allowed' or 'forced' for request-scoped ZDR
  const scrapeMode = getScrapeZDR(flags);
  if (
    request.zeroDataRetention &&
    scrapeMode !== "allowed" &&
    scrapeMode !== "forced"
  ) {
    return {
      error: `Zero Data Retention (ZDR) is not enabled for your team. Contact ${SUPPORT_EMAIL} to enable this feature.`,
    };
  }

  // robots perms — ignoreRobots must be 'allowed' or 'forced' for ignoreRobotsTxt and robotsUserAgent
  const robotsMode = getIgnoreRobots(flags);
  if (
    (request.ignoreRobotsTxt || request.robotsUserAgent) &&
    robotsMode !== "allowed" &&
    robotsMode !== "forced"
  ) {
    return {
      error: `Ignoring robots.txt is not enabled for your team. Contact ${SUPPORT_EMAIL} to enable this feature.`,
    };
  }

  // ip whitelist perms
  const needsWhitelist =
    request.location?.country === "us-whitelist" ||
    request.scrapeOptions?.location?.country === "us-whitelist";

  if (needsWhitelist && !flags?.ipWhitelist) {
    return {
      error: `Static IP addresses are not enabled for your team. Contact ${SUPPORT_EMAIL} to get a dedicated set of IP addresses you can whitelist.`,
    };
  }

  return {};
}
