"use strict";

import { StatusCodes } from "http-status-codes";
import {
  detectContentTypeFromFormat,
  getJSONSchema,
  validateJSON,
  printLog,
} from "../utils/index.js";
import {
  renderHighQualityPDF,
  renderSVGToImage,
  renderStyleJSON,
  renderPDF,
  addFrame,
} from "../render_style.js";

/**
 * Render styleJSON handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderStyleJSONHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_stylejson"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Render style */
      const image = await renderStyleJSON(req.body.input, req.body.output);

      res.set({
        "content-type": detectContentTypeFromFormat(req.body.format || "png"),
      });

      return res.status(StatusCodes.CREATED).send(image);
    } catch (error) {
      printLog("error", `Failed to render styleJSON: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Add frame handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function addFrameHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("add_frame"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      /* Add frame */
      const image = await addFrame(
        req.body.input,
        req.body.overlays,
        req.body.frame,
        req.body.grid,
        req.body.output
      );

      res.set({
        "content-type": detectContentTypeFromFormat(
          req.body.output.format || "png"
        ),
      });

      return res.status(StatusCodes.CREATED).send(image);
    } catch (error) {
      printLog("error", `Failed to add frame: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Render SVG handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderSVGHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_svg"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const result = await renderSVGToImage(req.body);

      res.set({
        "content-type": "application/json",
      });

      return res.status(StatusCodes.CREATED).send(result);
    } catch (error) {
      printLog("error", `Failed to render SVG: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Render PDF handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderPDFHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_pdf"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const result = await renderPDF(
        req.body.input,
        req.body.preview,
        req.body.output
      );

      res.set({
        "content-type": "application/json",
      });

      return res.status(StatusCodes.CREATED).send(result);
    } catch (error) {
      printLog("error", `Failed to render PDF: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

/**
 * Render PDF high quality handler
 * @returns {(req: Request, res: Response, next: NextFunction) => Promise<any>}
 */
function renderHighQualityPDFHandler() {
  return async (req, res) => {
    try {
      /* Validate options */
      try {
        validateJSON(await getJSONSchema("render_high_quality_pdf"), req.body);
      } catch (error) {
        throw new SyntaxError(error);
      }

      const result = await renderHighQualityPDF(
        req.body.input,
        req.body.preview,
        req.body.output
      );

      res.set({
        "content-type": "application/json",
      });

      return res.status(StatusCodes.CREATED).send(result);
    } catch (error) {
      printLog("error", `Failed to render high quality PDF: ${error}`);

      if (error instanceof SyntaxError) {
        return res
          .status(StatusCodes.BAD_REQUEST)
          .send(`Options parameter is invalid: ${error}`);
      } else {
        return res
          .status(StatusCodes.INTERNAL_SERVER_ERROR)
          .send("Internal server error");
      }
    }
  };
}

export const serve_render = {
  /**
   * Register render handlers
   * @param {Express} app Express object
   * @returns {void}
   */
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/add-frame:
     *   post:
     *     tags:
     *       - Render
     *     summary: Add frame to image
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Add frame options
     *     responses:
     *       201:
     *         description: Frame added
     *         content:
     *           application/json:
     *             schema:
     *               type: object
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
    app.post("/renders/add-frame", addFrameHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/stylejson:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render styleJSON
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render styleJSON options
     *     responses:
     *       201:
     *         description: Style JSON rendered
     *         content:
     *           application/json:
     *             schema:
     *               type: object
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
    app.post("/renders/stylejson", renderStyleJSONHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/svg:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render SVG
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: array
     *               items:
     *                 type: object
     *                 properties:
     *                   content:
     *                     type: string
     *                   width:
     *                     type: number
     *                   height:
     *                     type: number
     *                   format:
     *                    type: string
     *                    enum: [jpeg, jpg, png, webp, gif]
     *                   base64:
     *                    type: boolean
     *               example: []
     *       description: Render SVG options
     *     responses:
     *       201:
     *         description: SVG rendered
     *         content:
     *           application/json:
     *             schema:
     *               type: array
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
    app.post("/renders/svg", renderSVGHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/high-quality-pdf:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render high quality PDF
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render high quality PDF options
     *     responses:
     *       201:
     *         description: High quality PDF rendered
     *         content:
     *           application/json:
     *             schema:
     *               type: object
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
    app.post("/renders/high-quality-pdf", renderHighQualityPDFHandler());

    /**
     * @swagger
     * tags:
     *   - name: Render
     *     description: Render related endpoints
     * /renders/pdf:
     *   post:
     *     tags:
     *       - Render
     *     summary: Render PDF
     *     requestBody:
     *       required: true
     *       content:
     *         application/json:
     *             schema:
     *               type: object
     *               example: {}
     *       description: Render PDF options
     *     responses:
     *       201:
     *         description: PDF rendered
     *         content:
     *           application/json:
     *             schema:
     *               type: object
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
    app.post("/renders/pdf", renderPDFHandler());
  },
};
