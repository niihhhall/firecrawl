import { describe, test, expect, jest } from "@jest/globals";
import { deleteScrapeBrowser, scrapeExecute } from "../../../v2/methods/scrape";
import { SdkError } from "../../../v2/types";

describe("JS SDK v2 scrape-browser methods", () => {
  test("scrapeExecute posts to scrape execute endpoint", async () => {
    const post = jest.fn(async () => ({
      status: 200,
      data: {
        success: true,
        stdout: "ok",
        exitCode: 0,
      },
    }));

    const http = { post } as any;
    const response = await scrapeExecute(http, "job-123", { code: "console.log('ok')" });

    expect(post).toHaveBeenCalledWith("/v2/scrape/job-123/execute", {
      code: "console.log('ok')",
      language: "node",
    });
    expect(response.success).toBe(true);
    expect(response.exitCode).toBe(0);
  });

  test("scrapeExecute throws on non-200 response", async () => {
    const post = jest.fn(async () => ({
      status: 400,
      data: {
        success: false,
        error: "Invalid job ID format",
      },
    }));

    const http = { post } as any;
    await expect(
      scrapeExecute(http, "bad-id", { code: "console.log('ok')" })
    ).rejects.toBeInstanceOf(SdkError);
  });

  test("deleteScrapeBrowser calls delete endpoint", async () => {
    const del = jest.fn(async () => ({
      status: 200,
      data: {
        success: true,
      },
    }));

    const http = { delete: del } as any;
    const response = await deleteScrapeBrowser(http, "job-123");

    expect(del).toHaveBeenCalledWith("/v2/scrape/job-123/browser");
    expect(response.success).toBe(true);
  });

  test("deleteScrapeBrowser throws on non-200 response", async () => {
    const del = jest.fn(async () => ({
      status: 404,
      data: {
        success: false,
        error: "Browser session not found.",
      },
    }));

    const http = { delete: del } as any;
    await expect(deleteScrapeBrowser(http, "job-123")).rejects.toBeInstanceOf(
      SdkError
    );
  });
});
