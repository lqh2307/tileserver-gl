"use strict";

import { gzipAsync, inflateAsync, unzipAsync } from "./file.js";

/**
 * Send a plain text response for messages that may include request-controlled
 * values. This prevents browsers from parsing reflected errors as HTML.
 * @param {Response} res Express response
 * @param {number} status HTTP status code
 * @param {string} message Response message
 * @returns {Response}
 */
export function sendTextResponse(res, status, message) {
  return res.type("text/plain").status(status).send(message);
}

/**
 * Check whether a response should be gzip-compressed.
 *
 * The compression query parameter is retained as a per-request override. If
 * it is omitted, the normalized COMPRESS_RESPONSE environment variable is
 * used as the default.
 * @param {Request} req Express request
 * @returns {boolean}
 */
export function shouldCompressResponse(req) {
  if (req.query?.compression === "true") {
    return true;
  }

  if (req.query?.compression === "false") {
    return false;
  }

  return process.env.COMPRESS_RESPONSE === "true";
}

/**
 * Normalize an already encoded response for the client's negotiated encoding.
 * Unencoded responses are left untouched and are compressed by the response
 * middleware when enabled, avoiding eager buffering in individual handlers.
 * @param {Buffer|string} data Response body
 * @param {Record<string, string>} headers Response headers, mutated in place
 * @param {Request} req Express request
 * @returns {Promise<Buffer|string>} Normalized response body
 */
export async function normalizeResponseEncoding(data, headers, req) {
  const contentEncoding = headers["content-encoding"]?.toLowerCase();

  if (!contentEncoding) {
    return data;
  }

  const compress = shouldCompressResponse(req);

  if (contentEncoding !== "gzip" && contentEncoding !== "deflate") {
    return data;
  }

  const acceptedEncoding = compress
    ? req.acceptsEncodings("gzip", contentEncoding)
    : false;

  if (acceptedEncoding === contentEncoding) {
    return data;
  }

  if (acceptedEncoding === "gzip" && contentEncoding === "deflate") {
    headers["content-encoding"] = "gzip";

    delete headers["content-length"];

    return gzipAsync(await inflateAsync(data));
  }

  delete headers["content-encoding"];
  delete headers["content-length"];

  return contentEncoding === "gzip"
    ? await unzipAsync(data)
    : await inflateAsync(data);
}
