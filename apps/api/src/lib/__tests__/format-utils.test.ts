import {
  hasFormatOfType,
  includesFormat,
  needsMarkdownContent,
} from "../format-utils";

describe("format-utils", () => {
  describe("needsMarkdownContent", () => {
    it("returns true for markdown-dependent formats, including query", () => {
      expect(needsMarkdownContent([{ type: "markdown" }])).toBe(true);
      expect(
        needsMarkdownContent([
          {
            type: "changeTracking",
            modes: ["git-diff"],
            schema: null,
            tag: null,
          },
        ]),
      ).toBe(true);
      expect(
        needsMarkdownContent([
          { type: "json", prompt: "Extract title", schema: null },
        ]),
      ).toBe(true);
      expect(needsMarkdownContent([{ type: "summary" }])).toBe(true);
      expect(
        needsMarkdownContent([{ type: "query", prompt: "What is this?" }]),
      ).toBe(true);
    });

    it("returns false for formats that do not require markdown", () => {
      expect(needsMarkdownContent(undefined)).toBe(false);
      expect(needsMarkdownContent([{ type: "html" }])).toBe(false);
      expect(needsMarkdownContent([{ type: "rawHtml" }])).toBe(false);
      expect(needsMarkdownContent([{ type: "links" }])).toBe(false);
      expect(needsMarkdownContent([{ type: "images" }])).toBe(false);
      expect(needsMarkdownContent([{ type: "branding" }])).toBe(false);
      expect(
        needsMarkdownContent([{ type: "screenshot", fullPage: false }]),
      ).toBe(false);
    });
  });

  describe("existing helpers", () => {
    it("still finds object formats by type", () => {
      expect(
        hasFormatOfType([{ type: "query", prompt: "What is this?" }], "query"),
      ).toEqual({ type: "query", prompt: "What is this?" });
    });

    it("still supports mixed v1/v2 format checks", () => {
      expect(includesFormat(["markdown", "html"], "markdown")).toBe(true);
      expect(includesFormat([{ type: "query", prompt: "What is this?" }], "query")).toBe(true);
      expect(includesFormat([{ type: "html" }], "query")).toBe(false);
    });
  });
});
