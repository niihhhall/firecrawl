import { generateObject } from "ai";
import { z } from "zod";
import { getModel } from "../lib/generic-ai";
import { config } from "../config";
import type { Logger } from "winston";

const decomposeSchema = z.object({
  queries: z
    .array(
      z.object({
        query: z.string().describe("A SERP-optimized search query"),
        intent: z.string().describe("What this sub-query aims to find"),
      }),
    )
    .min(2)
    .max(10),
});

export async function decomposeQuery(
  query: string,
  numQueries: number | "auto",
  logger: Logger,
): Promise<{ query: string; intent: string }[]> {
  if (!config.OPENAI_API_KEY && !config.OLLAMA_BASE_URL) {
    throw new Error(
      "Query decomposition requires an AI provider. Set OPENAI_API_KEY or OLLAMA_BASE_URL.",
    );
  }

  const isAuto = numQueries === "auto";
  const countInstruction = isAuto
    ? "the optimal number of"
    : String(numQueries);

  const autoGuidance = isAuto
    ? `
Decide how many sub-queries to generate (2-10) based on the query:
- Simple, focused query (e.g. "best web frameworks") → 2-3 sub-queries covering different angles
- Multi-faceted query (e.g. "pros and cons of React vs Vue for enterprise apps") → 3-4 sub-queries
- Multi-entity query (e.g. "stock price of Apple, Nvidia, Tesla") → one sub-query per entity
- The number of sub-queries should match the natural structure of the request`
    : "";

  const result = await generateObject({
    model: getModel("gpt-4o-mini"),
    schema: decomposeSchema,
    messages: [
      {
        role: "system",
        content: `You are a search query optimizer. Given a user's search query, decompose it into ${countInstruction} distinct, SERP-optimized search queries that together provide comprehensive coverage of the topic.
${autoGuidance}
Rules:
- Each query should target a different facet or angle, or a distinct entity from the original query
- Keep queries concise and optimized for search engines
- Do not repeat the same query with minor variations
- The first query should be a concise, direct version of the original
- Today's date is ${new Date().toISOString().split("T")[0]}`,
      },
      {
        role: "user",
        content: query,
      },
    ],
  });

  logger.info("Query decomposition complete", {
    originalQuery: query,
    decomposedCount: result.object.queries.length,
  });

  const maxQueries = typeof numQueries === "number" ? numQueries : 10;
  return result.object.queries.slice(0, maxQueries);
}
