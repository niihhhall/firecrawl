/**
 * Sanity tests for the LangSmith wiring. These don't hit the LangSmith API —
 * they only verify the gating and passthrough semantics so the module is safe
 * to import in environments without a LANGSMITH_API_KEY set.
 */
import * as ai from "ai";
import {
  generateText,
  streamText,
  generateObject,
  streamObject,
  isLangSmithEnabled,
  buildLangSmithProviderOptions,
  traceInteract,
} from "./langsmith";

describe("scrape-interact/langsmith (disabled — no API key)", () => {
  // These tests run in the normal test environment, where LANGSMITH_API_KEY is
  // not set. That means isLangSmithEnabled should be false and the module
  // should fall back cleanly to the raw ai SDK.

  it("reports disabled when LANGSMITH_API_KEY is unset", () => {
    expect(isLangSmithEnabled).toBe(false);
  });

  it("re-exports raw ai SDK functions when disabled", () => {
    expect(generateText).toBe(ai.generateText);
    expect(streamText).toBe(ai.streamText);
    expect(generateObject).toBe(ai.generateObject);
    expect(streamObject).toBe(ai.streamObject);
  });

  it("returns undefined providerOptions when disabled", () => {
    const opts = buildLangSmithProviderOptions(
      {
        thread_id: "t1",
        session_id: "t1",
        scrape_id: "s1",
        team_id: "team1",
        mode: "prompt",
      },
      { name: "test" },
    );
    expect(opts).toBeUndefined();
  });

  it("traceInteract returns the original function unchanged when disabled", async () => {
    const original = async (x: number) => x * 2;
    const wrapped = traceInteract(
      original,
      {
        thread_id: "t1",
        session_id: "t1",
        scrape_id: "s1",
        team_id: "team1",
        mode: "code",
      },
      { name: "test" },
    );
    expect(wrapped).toBe(original);
    await expect(wrapped(3)).resolves.toBe(6);
  });
});
