"use strict";

import { StatusCodes } from "http-status-codes";
import { getMetrics } from "./prometheus.js";
import { printLog } from "./logger.js";
import express from "express";

/**
 * Get metrics handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function serveMetricsHandler() {
  return async (req, res, next) => {
    try {
      const data = getMetrics();

      res.header("content-type", data.contentType);

      return res.status(StatusCodes.OK).send(data.metrics);
    } catch (error) {
      printLog("error", `Failed to get metrics: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_prometheus = {
  init: () => {
    const app = express().disable("x-powered-by");

    /**
     * @swagger
     * tags:
     *   - name: Prometheus
     *     description: Prometheus related endpoints
     * /prometheus:
     *   get:
     *     tags:
     *       - Prometheus
     *     summary: Get metrics
     *     responses:
     *       200:
     *         description: Metrics
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: OK
     *       404:
     *         description: Not found
     *       503:
     *         description: Server is starting up
     *         content:
     *           text/plain:
     *             schema:
     *               type: string
     *               example: Starting...
     *       500:
     *         description: Internal server error
     */
    app.get("/", serveMetricsHandler());

    return app;
  },
};
