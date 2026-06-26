"use strict";

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
