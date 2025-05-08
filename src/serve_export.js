"use strict";

import { getJSONSchema, validateJSON } from "./utils.js";
import { StatusCodes } from "http-status-codes";
import { exportAll } from "./export_all.js";
import { printLog } from "./logger.js";
import { config } from "./config.js";

/**
 * Export all handler
 * @returns {(req: any, res: any, next: any) => Promise<any>}
 */
function exportAllHandler() {
  return async (req, res, next) => {
    try {
      try {
        validateJSON(await getJSONSchema("export_all"), req.body);
      } catch (error) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options is invalid: ${error}`);
      }

      if (req.body.styles !== undefined) {
        for (const styleID of req.body.styles) {
          if (config.styles[styleID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Style id "${styleID}" does not exist`);
          }
        }
      }

      if (req.body.datas !== undefined) {
        for (const dataID of req.body.datas) {
          if (config.datas[dataID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Data id "${dataID}" does not exist`);
          }
        }
      }

      if (req.body.geojsons !== undefined) {
        for (const group of req.body.geojsons) {
          if (config.geojsons[group] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`GeoJSON group id "${group}" does not exist`);
          }
        }
      }

      if (req.body.sprites !== undefined) {
        for (const spriteID of req.body.sprites) {
          if (config.sprites[spriteID] === undefined) {
            return res
              .status(StatusCodes.NOT_FOUND)
              .send(`Sprite id "${spriteID}" does not exist`);
          }
        }
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
