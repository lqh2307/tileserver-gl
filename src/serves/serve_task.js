"use strict";

import { cleanUp, seed } from "../configs/index.js";
import { StatusCodes } from "http-status-codes";
import {
  sendTextResponse,
  getTaskTargets,
  TASK_TYPES,
  printLog,
} from "../utils/index.js";

/**
 * Parse and validate a task selector.
 * @param {Request} req Express request
 * @param {boolean} allowRestart Whether the restart query is allowed
 * @returns {{ type?: string, id?: string }} Task selector
 */
function getTaskSelector(req, allowRestart = false) {
  const allowedQueries = new Set(
    allowRestart ? ["type", "id", "restart"] : ["type", "id"],
  );
  const unsupportedQueries = Object.keys(req.query).filter((query) => {
    return !allowedQueries.has(query);
  });

  if (unsupportedQueries.length) {
    throw new SyntaxError(
      `Unsupported task query: "${unsupportedQueries.join('", "')}"`,
    );
  }

  const { type, id } = req.query;

  if (
    (type !== undefined && typeof type !== "string") ||
    (id !== undefined && typeof id !== "string")
  ) {
    throw new SyntaxError("Task type and id must be single string values");
  }

  if (id && !type) {
    throw new SyntaxError('Task "type" is required when "id" is specified');
  }

  if (type !== undefined && !TASK_TYPES.has(type)) {
    throw new SyntaxError(`Invalid task type "${type}"`);
  }

  if (
    allowRestart &&
    req.query.restart !== undefined &&
    req.query.restart !== "true" &&
    req.query.restart !== "false"
  ) {
    throw new SyntaxError('Task "restart" must be "true" or "false"');
  }

  return {
    type,
    id,
  };
}

/**
 * Start task handler.
 * @returns {(req: Request, res: Response) => Promise<any>}
 */
function startTaskHandler() {
  return async (req, res) => {
    try {
      const selector = getTaskSelector(req, true);
      const targets = getTaskTargets(selector, seed, cleanUp);

      if (selector.id && !targets.length) {
        return sendTextResponse(
          res,
          StatusCodes.NOT_FOUND,
          "No configured sync task matched",
        );
      }

      process.send?.({
        action: "startTask",
        ...selector,
        restart: req.query.restart === "true",
      });

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      if (error instanceof SyntaxError) {
        return sendTextResponse(res, StatusCodes.BAD_REQUEST, error.message);
      }

      printLog("error", `Failed to start task: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

/**
 * Cancel task handler.
 * @returns {(req: Request, res: Response) => Promise<any>}
 */
function cancelTaskHandler() {
  return async (req, res) => {
    try {
      process.send?.({
        action: "cancelTask",
        ...getTaskSelector(req),
      });

      return res.status(StatusCodes.OK).send("OK");
    } catch (error) {
      if (error instanceof SyntaxError) {
        return sendTextResponse(res, StatusCodes.BAD_REQUEST, error.message);
      }

      printLog("error", `Failed to cancel task: ${error}`);

      return res
        .status(StatusCodes.INTERNAL_SERVER_ERROR)
        .send("Internal server error");
    }
  };
}

export const serve_task = {
  /**
   * Register task handlers.
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Task
     *     description: Resource-level cleanup and seed synchronization
     * /tasks/start:
     *   get:
     *     tags:
     *       - Task
     *     summary: Start synchronization tasks
     *     description: Runs cleanup and then seed for one resource, all resources of a type, or every configured resource.
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [data, style, geojson, sprite, font]
     *         required: false
     *         description: Resource type. Omit to synchronize every type.
     *       - in: query
     *         name: id
     *         schema:
     *           type: string
     *         required: false
     *         description: Exact resource ID. Requires type; omit to synchronize every ID of the selected type.
     *       - in: query
     *         name: restart
     *         schema:
     *           type: boolean
     *           default: false
     *         required: false
     *         description: Restart after all accepted synchronization tasks finish.
     *     responses:
     *       200:
     *         description: Matching tasks were accepted
     *       400:
     *         description: Invalid type or id without type
     *       404:
     *         description: The exact type and ID selector did not match a configured sync resource
     *       503:
     *         description: Server is starting up
     *       500:
     *         description: Internal server error
     */
    app.get("/tasks/start", startTaskHandler());

    /**
     * @swagger
     * /tasks/cancel:
     *   get:
     *     tags:
     *       - Task
     *     summary: Cancel synchronization tasks
     *     description: Cancels one resource, all queued/running resources of a type, or every synchronization task without affecting unmatched tasks.
     *     parameters:
     *       - in: query
     *         name: type
     *         schema:
     *           type: string
     *           enum: [data, style, geojson, sprite, font]
     *         required: false
     *         description: Resource type. Omit to cancel every type.
     *       - in: query
     *         name: id
     *         schema:
     *           type: string
     *         required: false
     *         description: Exact resource ID. Requires type; omit to cancel every ID of the selected type.
     *     responses:
     *       200:
     *         description: Cancellation request was accepted
     *       400:
     *         description: Invalid type or id without type
     *       503:
     *         description: Server is starting up
     *       500:
     *         description: Internal server error
     */
    app.get("/tasks/cancel", cancelTaskHandler());
  },
};
