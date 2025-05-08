"use strict";

import { getJSONSchema, validateJSON } from "./utils.js";
import { StatusCodes } from "http-status-codes";
import { exportAll } from "./export_all.js";
import { printLog } from "./logger.js";

/**
 * Export all handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function exportAllHandler() {
  return async (req, res, next) => {
    const id = req.params.id;

    try {
      /* Export all */
      try {
        validateJSON(await getJSONSchema("export_all"), req.body);
      } catch (error) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options is invalid: ${error}`);
      }

      exportAll(
        `${process.env.DATA_DIR}/exports/alls/${req.body.id}`,
        req.body
      );

      return res.status(StatusCodes.CREATED).send("OK");
    } catch (error) {
      printLog("error", `Failed to export all: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send("Options parameter is invalid");
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

export const serve_export = {
  /**
   * Register export handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Export
     *     description: Export related endpoints
     * /exports:
     *   post:
     *     tags:
     *       - Export
     *     summary: Export all
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Export all options
     *     responses:
     *       201:
     *         description: Export all is started
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
    app.post("/exports", exportAllHandler());
  },
};
