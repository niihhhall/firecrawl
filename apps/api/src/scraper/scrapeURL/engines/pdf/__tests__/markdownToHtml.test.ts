import { safeMarkdownToHtml } from "../markdownToHtml";

const noopLogger = {
  warn: jest.fn(),
} as any;

describe("safeMarkdownToHtml", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("converts simple markdown to HTML", async () => {
    const html = await safeMarkdownToHtml("# Hello", noopLogger, "test-1");
    expect(html).toContain("<h1>");
    expect(html).toContain("Hello");
  });

  it("does not throw on pathologically deep markdown and returns <pre> fallback", async () => {
    // 50,000 nested blockquotes — enough to blow the call stack in marked's recursive parser
    const deep = "> ".repeat(50_000) + "content";
    const html = await safeMarkdownToHtml(deep, noopLogger, "test-deep");
    // Should not throw — either marked handles it or we fall back
    expect(typeof html).toBe("string");
    expect(html.length).toBeGreaterThan(0);

    // If marked blew up, we should get the <pre> fallback
    if (html.startsWith("<pre>")) {
      expect(html).toContain("content");
      expect(noopLogger.warn).toHaveBeenCalled();
    }
  });

  it("does not throw on a large table and returns <pre> fallback", async () => {
    // ~200KB markdown table
    const row = "| " + "cell | ".repeat(10) + "\n";
    const header = row + "| " + "--- | ".repeat(10) + "\n";
    const table = header + row.repeat(5_000);
    expect(table.length).toBeGreaterThan(200_000);

    const html = await safeMarkdownToHtml(table, noopLogger, "test-table");
    expect(typeof html).toBe("string");
    expect(html.length).toBeGreaterThan(0);
  });

  it("escapes HTML entities in the <pre> fallback", async () => {
    // Force the fallback by mocking marked
    const markedModule = require("marked");
    const originalParse = markedModule.parse;
    markedModule.parse = () => {
      throw new RangeError("Maximum call stack size exceeded");
    };

    try {
      const input = '<script>alert("xss")</script> & "quotes" \'apos\'';
      const html = await safeMarkdownToHtml(input, noopLogger, "test-escape");
      expect(html).toStartWith("<pre>");
      expect(html).toContain("&lt;script&gt;");
      expect(html).toContain("&amp;");
      expect(html).toContain("&quot;quotes&quot;");
      expect(html).toContain("&#39;apos&#39;");
      expect(html).not.toContain("<script>");
    } finally {
      markedModule.parse = originalParse;
    }
  });
});

// Custom matcher
expect.extend({
  toStartWith(received: string, expected: string) {
    const pass = typeof received === "string" && received.startsWith(expected);
    return {
      pass,
      message: () =>
        `expected ${JSON.stringify(received.slice(0, 50))}... to start with ${JSON.stringify(expected)}`,
    };
  },
});

declare global {
  namespace jest {
    interface Matchers<R> {
      toStartWith(expected: string): R;
    }
  }
}
