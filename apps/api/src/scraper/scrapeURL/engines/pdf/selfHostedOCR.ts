import { Meta } from "../..";
import { config } from "../../../../config";
import { robustFetch } from "../../lib/fetch";
import { z } from "zod";
import * as marked from "marked";
import type { PDFProcessorResult } from "./types";

const ocrResponseSchema = z.object({
  markdown: z.string(),
  failed_pages: z.array(z.number()).nullable(),
  pages_processed: z.number().optional(),
});

export async function scrapePDFWithSelfHostedOCR(
  meta: Meta,
  base64Content: string,
  maxPages?: number,
): Promise<PDFProcessorResult> {
  const logger = meta.logger.child({ method: "scrapePDF/selfHostedOCR" });

  logger.debug("Processing PDF document with self-hosted OCR");

  const resp = await robustFetch({
    url: `${config.PDF_OCR_BASE_URL}/ocr`,
    method: "POST",
    headers: config.PDF_OCR_API_KEY
      ? { Authorization: `Bearer ${config.PDF_OCR_API_KEY}` }
      : undefined,
    body: {
      pdf: base64Content,
      scrape_id: meta.id,
      ...(maxPages !== undefined && { max_pages: maxPages }),
    },
    logger,
    schema: ocrResponseSchema,
    mock: meta.mock,
    abort: meta.abort.asSignal(),
  });

  return {
    markdown: resp.markdown,
    html: await marked.parse(resp.markdown, { async: true }),
  };
}
