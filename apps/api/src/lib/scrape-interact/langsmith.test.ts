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

describe("scrape-interact/langsmith (enabled — mocked SDK)", () => {
  // These tests reset module state and provide fake langsmith modules so we
  // can exercise the wrap path without network calls or a real API key.

  const ORIGINAL_ENV = { ...process.env };
  const fakeWrappedFns = {
    generateText: jest.fn(),
    streamText: jest.fn(),
    generateObject: jest.fn(),
    streamObject: jest.fn(),
  };
  const createProviderOptionsSpy = jest.fn((opts: unknown) => ({
    __fake_langsmith_options__: true,
    payload: opts,
  }));
  const traceableSpy = jest.fn(
    (fn: (...args: unknown[]) => unknown, _opts: unknown) => {
      const wrapper = (...args: unknown[]) => fn(...args);
      (
        wrapper as unknown as { __fake_traceable__: boolean }
      ).__fake_traceable__ = true;
      return wrapper;
    },
  );

  beforeEach(() => {
    jest.resetModules();
    process.env.LANGSMITH_API_KEY = "test-fake-key";
    process.env.LANGSMITH_PROJECT = "test-project";
    createProviderOptionsSpy.mockClear();
    traceableSpy.mockClear();
    jest.doMock("langsmith/experimental/vercel", () => ({
      wrapAISDK: () => fakeWrappedFns,
      createLangSmithProviderOptions: createProviderOptionsSpy,
    }));
    jest.doMock("langsmith/traceable", () => ({
      traceable: traceableSpy,
    }));
  });

  afterEach(() => {
    process.env = { ...ORIGINAL_ENV };
    jest.dontMock("langsmith/experimental/vercel");
    jest.dontMock("langsmith/traceable");
  });

  it("reports enabled and swaps generateText for the wrapped fn", () => {
    const mod = require("./langsmith");
    expect(mod.isLangSmithEnabled).toBe(true);
    expect(mod.generateText).toBe(fakeWrappedFns.generateText);
    expect(mod.generateText).not.toBe(ai.generateText);
  });

  it("builds provider options with thread_id + scrape_id metadata", () => {
    const mod = require("./langsmith");
    const result = mod.buildLangSmithProviderOptions(
      {
        thread_id: "sess-abc",
        session_id: "sess-abc",
        scrape_id: "scrape-xyz",
        team_id: "team-42",
        browser_id: "browser-1",
        mode: "prompt",
      },
      { name: "interact:prompt", extra: { prompt_length: 123 } },
    );

    expect(createProviderOptionsSpy).toHaveBeenCalledTimes(1);
    const callArg = createProviderOptionsSpy.mock.calls[0][0] as {
      name: string;
      metadata: Record<string, unknown>;
      tags: string[];
    };
    expect(callArg.name).toBe("interact:prompt");
    expect(callArg.metadata).toMatchObject({
      thread_id: "sess-abc",
      session_id: "sess-abc",
      scrape_id: "scrape-xyz",
      team_id: "team-42",
      browser_id: "browser-1",
      mode: "prompt",
      prompt_length: 123,
    });
    expect(callArg.tags).toEqual(["interact", "mode:prompt"]);
    expect(result).toMatchObject({ __fake_langsmith_options__: true });
  });

  it("skips tracing when meta.zeroDataRetention is true", () => {
    const mod = require("./langsmith");
    const result = mod.buildLangSmithProviderOptions({
      thread_id: "sess-abc",
      session_id: "sess-abc",
      scrape_id: "scrape-xyz",
      team_id: "team-42",
      mode: "prompt",
      zeroDataRetention: true,
    });
    expect(result).toBeUndefined();
    expect(createProviderOptionsSpy).not.toHaveBeenCalled();

    const fn = async () => "should-still-run";
    const wrapped = mod.traceInteract(fn, {
      thread_id: "sess-abc",
      session_id: "sess-abc",
      scrape_id: "scrape-xyz",
      team_id: "team-42",
      mode: "code",
      zeroDataRetention: true,
    });
    expect(wrapped).toBe(fn);
    expect(traceableSpy).not.toHaveBeenCalled();
  });

  it("wraps functions via traceable when zeroDataRetention is not set", async () => {
    const mod = require("./langsmith");
    const fn = jest.fn(async (x: number) => x + 1);
    const wrapped = mod.traceInteract(
      fn,
      {
        thread_id: "sess-abc",
        session_id: "sess-abc",
        scrape_id: "scrape-xyz",
        team_id: "team-42",
        mode: "code",
      },
      { name: "interact:code" },
    );
    expect(traceableSpy).toHaveBeenCalledTimes(1);
    const traceableOpts = traceableSpy.mock.calls[0][1] as {
      name: string;
      run_type: string;
      metadata: Record<string, unknown>;
      tags: string[];
    };
    expect(traceableOpts.name).toBe("interact:code");
    expect(traceableOpts.run_type).toBe("chain");
    expect(traceableOpts.tags).toEqual(["interact", "mode:code"]);
    expect(traceableOpts.metadata).toMatchObject({
      thread_id: "sess-abc",
      mode: "code",
    });
    await expect(wrapped(5)).resolves.toBe(6);
    expect(fn).toHaveBeenCalledWith(5);
  });

  it("falls back to raw ai SDK when langsmith require() throws", () => {
    jest.resetModules();
    jest.doMock("langsmith/experimental/vercel", () => {
      throw new Error("simulated install breakage");
    });
    jest.doMock("langsmith/traceable", () => ({ traceable: traceableSpy }));
    // Re-require ai from the fresh module graph so the identity check lines up
    // with the module instance the langsmith helper imported.
    const freshAi = require("ai");
    const mod = require("./langsmith");
    expect(mod.generateText).toBe(freshAi.generateText);
    expect(
      mod.buildLangSmithProviderOptions({
        thread_id: "t",
        session_id: "t",
        scrape_id: "s",
        team_id: "x",
        mode: "prompt",
      }),
    ).toBeUndefined();
  });
});
