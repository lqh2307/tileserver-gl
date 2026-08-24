import { collectDefaultMetrics, Registry, Histogram } from "prom-client";
import { getEnvironmentString } from "../configs/index.js";

const register = new Registry();

const httpRequestDuration = new Histogram({
  name: "http_request_duration",
  help: "Duration of HTTP requests in ms",
  labelNames: ["method", "route", "status_code"],
  buckets: [100, 300, 500, 1000],
});

register.setDefaultLabels({
  service_name: getEnvironmentString(process.env.SERVICE_NAME, "tile-server"),
});

register.registerMetric(httpRequestDuration);
collectDefaultMetrics({
  register,
});

/**
 * Set metrics
 * @param {string} method HTTP method
 * @param {string} route Normalized Express route
 * @param {number} statusCode HTTP status code
 * @param {number} duration Duration
 * @returns {void}
 */
export function setMetrics(method, route, statusCode, duration) {
  httpRequestDuration.labels(method, route, statusCode).observe(duration);
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
