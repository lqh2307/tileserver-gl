import { Registry, Histogram } from "prom-client";

const register = new Registry();

const httpRequestDuration = new Histogram({
  name: "http_request_duration",
  help: "Duration of HTTP requests in ms",
  labelNames: [
    "method",
    "protocol",
    "path",
    "status_code",
    "origin",
    "ip",
    "user_id",
    "user_agent",
  ],
  buckets: [100, 300, 500, 1000],
});

register.setDefaultLabels({
  service_name: process.env.SERVICE_NAME,
});

register.registerMetric(httpRequestDuration);

/**
 * Set metrics
 * @param {string} method HTTP method
 * @param {string} protocol HTTP protocol
 * @param {string} path HTTP path
 * @param {number} statusCode HTTP status code
 * @param {string} origin Origin
 * @param {string} ip IP
 * @param {string} userID User ID
 * @param {string} userAgent User agent
 * @param {number} duration Duration
 * @returns {void}
 */
export function setMetrics(
  method,
  protocol,
  path,
  statusCode,
  origin,
  ip,
  userID,
  userAgent,
  duration
) {
  httpRequestDuration
    .labels(method, protocol, path, statusCode, origin, ip, userID, userAgent)
    .observe(duration);
}

/**
 * Get metrics
 * @returns {Promise<object>}
 */
export async function getMetrics() {
  return {
    contentType: register.contentType,
    metrics: await register.metrics(),
  };
}
