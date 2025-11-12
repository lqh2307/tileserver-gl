"use strict";

import { StatusCodes } from "http-status-codes";
import { printLog } from "../utils/index.js";

/**
 * Start task handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function startTaskHandler() {
  return async (req, res) => {
    try {
      setTimeout(
        () =>
          process.send({
            action: "startTask",
            cleanUpSprites: req.query.cleanUpSprites === "true",
            cleanUpFonts: req.query.cleanUpFonts === "true",
            cleanUpStyles: req.query.cleanUpStyles === "true",
            cleanUpGeoJSONs: req.query.cleanUpGeoJSONs === "true",
            cleanUpDatas: req.query.cleanUpDatas === "true",
            seedSprites: req.query.seedSprites === "true",
            seedFonts: req.query.seedFonts === "true",
            seedStyles: req.query.seedStyles === "true",
            seedGeoJSONs: req.query.seedGeoJSONs === "true",
            seedDatas: req.query.seedDatas === "true",
            restart: req.query.restart === "true",
          }),
        0,
      );

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to start task": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Cancel task handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function cancelTaskHandler() {
  return async (_, res) => {
    try {
      setTimeout(
        () =>
          process.send({
            action: "cancelTask",
          }),
        0,
      );

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      printLog("error", `Failed to cancel task": ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_task = {
  /**
   * Register task handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Task
     *     description: Task related endpoints
     * /tasks/start:
     *   get:
     *     tags:
     *       - Task
     *     summary: Start task
     *     parameters:
     *       - in: query
     *         name: cleanUpSprites
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run cleanup sprites
     *       - in: query
     *         name: cleanUpFonts
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run cleanup fonts
     *       - in: query
     *         name: cleanUpStyles
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run cleanup styles
     *       - in: query
     *         name: cleanUpGeoJSONs
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run cleanup geojsons
     *       - in: query
     *         name: cleanUpDatas
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run cleanup datas
     *       - in: query
     *         name: seedSprites
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run seed sprites
     *       - in: query
     *         name: seedFonts
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run seed fonts
     *       - in: query
     *         name: seedStyles
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run seed styles
     *       - in: query
     *         name: seedGeoJSONs
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run seed geojsons
     *       - in: query
     *         name: seedDatas
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Run seed datas
     *       - in: query
     *         name: restart
     *         schema:
     *           type: boolean
     *         required: false
     *         description: Restart server after run task
     *     responses:
     *       200:
     *         description: Task started successfully
     *       400:
     *         description: Bad request
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
    app.get("/tasks/start", startTaskHandler());

    /**
     * @swagger
     * tags:
     *   - name: Task
     *     description: Task related endpoints
     * /tasks/cancel:
     *   get:
     *     tags:
     *       - Task
     *     summary: Cancel the running task
     *     responses:
     *       200:
     *         description: Task cancelled successfully
     *       400:
     *         description: Bad request
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
    app.get("/tasks/cancel", cancelTaskHandler());
  },
};
