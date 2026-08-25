"use strict";

import { getTileRendererPool, renderImageTileData } from "../render_style.js";
import { StatusCodes } from "http-status-codes";
import { config } from "../configs/index.js";
import {
  getAndCachePostgreSQLTileData,
  getAndCacheMBTilesTileData,
  getAndCacheXYZTileData,
  getRenderedStyleJSON,
  getPMTilesTile,
} from "../resources/index.js";
import {
  compileHandleBarsTemplate,
  normalizeResponseEncoding,
  isFileNotModified,
  getRequestHost,
  getParameter,
  isValidBBox,
  xmlEscape,
  printLog,
  min,
} from "../utils/index.js";

const WMTS_VERSION = "1.0.0";
const WEB_MERCATOR_HALF_WORLD = 20037508.342789244;
const WEB_MERCATOR_SCALE_256 = 559082264.0287178;
const DEFAULT_MAX_ZOOM = 22;
const DEFAULT_BBOX = [-180, -85.051129, 180, 85.051129];
const CAPABILITIES_UPDATE_SEQUENCE = String(Date.now());
const DEFAULT_STYLE_FORMATS = ["image/png", "image/jpeg", "image/webp"];
const TILE_MATRIX_ALIASES = {
  256: "GoogleMapsCompatible_256",
  512: "GoogleMapsCompatible_512",
  GoogleMapsCompatible: "GoogleMapsCompatible_256",
  WebMercatorQuad: "GoogleMapsCompatible_256",
};

const TILE_FORMATS = new Map([
  [
    "image/png",
    {
      mime: "image/png",
      extension: "png",
      tileFormat: "png",
    },
  ],
  [
    "image/jpeg",
    {
      mime: "image/jpeg",
      extension: "jpg",
      tileFormat: "jpeg",
    },
  ],
  [
    "image/jpg",
    {
      mime: "image/jpeg",
      extension: "jpg",
      tileFormat: "jpeg",
    },
  ],
  [
    "image/webp",
    {
      mime: "image/webp",
      extension: "webp",
      tileFormat: "webp",
    },
  ],
  [
    "application/x-protobuf",
    {
      mime: "application/x-protobuf",
      extension: "pbf",
      tileFormat: "pbf",
    },
  ],
  [
    "application/vnd.mapbox-vector-tile",
    {
      mime: "application/vnd.mapbox-vector-tile",
      extension: "pbf",
      tileFormat: "pbf",
    },
  ],
]);

const TILE_FORMATS_BY_EXTENSION = new Map([
  ["png", TILE_FORMATS.get("image/png")],
  ["jpg", TILE_FORMATS.get("image/jpeg")],
  ["jpeg", TILE_FORMATS.get("image/jpeg")],
  ["webp", TILE_FORMATS.get("image/webp")],
  ["pbf", TILE_FORMATS.get("application/x-protobuf")],
]);

class WMTSError extends Error {
  constructor(code, message, status = StatusCodes.BAD_REQUEST) {
    super(message);
    this.name = "WMTSError";
    this.code = code;
    this.status = status;
  }
}

function normalizeVersion(value) {
  const version = String(value ?? WMTS_VERSION);

  if (version !== WMTS_VERSION) {
    throw new WMTSError(
      "InvalidParameterValue",
      `Unsupported WMTS version "${version}".`,
    );
  }

  return version;
}

function normalizeOperation(value) {
  const operation = String(value ?? "GetCapabilities").toLowerCase();

  if (operation === "getcapabilities") {
    return "GetCapabilities";
  }

  if (operation === "gettile") {
    return "GetTile";
  }

  throw new WMTSError(
    "OperationNotSupported",
    `Operation "${value}" is not supported.`,
  );
}

function normalizeFormat(value) {
  const format = String(value ?? "")
    .trim()
    .toLowerCase();
  const definition = TILE_FORMATS.get(format);

  if (!definition) {
    throw new WMTSError(
      "FormatNotSupported",
      `Tile format "${value}" is not supported.`,
    );
  }

  return definition;
}

function formatFromExtension(value) {
  const extension = String(value ?? "")
    .replace(/^\./, "")
    .toLowerCase();

  const definition = TILE_FORMATS_BY_EXTENSION.get(extension);

  if (!definition) {
    throw new WMTSError(
      "FormatNotSupported",
      `Tile format extension "${value}" is not supported.`,
    );
  }

  return definition;
}

function normalizeBBox(value) {
  if (!Array.isArray(value) || value.length !== 4) {
    return DEFAULT_BBOX;
  }

  const bbox = value.map(Number);
  return isValidBBox(bbox, true) ? bbox : DEFAULT_BBOX;
}

function clampZoom(value, fallback) {
  const zoom = Number(value);
  return Number.isInteger(zoom) && zoom >= 0
    ? min(zoom, DEFAULT_MAX_ZOOM)
    : fallback;
}

function createTileMatrixSet(identifier, tileSize) {
  const scale =
    tileSize === 512 ? WEB_MERCATOR_SCALE_256 / 2 : WEB_MERCATOR_SCALE_256;

  return {
    identifier,
    supportedCRS: "urn:ogc:def:crs:EPSG::3857",
    tileSize,
    topLeft: [-WEB_MERCATOR_HALF_WORLD, WEB_MERCATOR_HALF_WORLD],
    matrices: Array.from(
      {
        length: DEFAULT_MAX_ZOOM + 1,
      },
      (_, zoom) => {
        return {
          identifier: String(zoom),
          scaleDenominator: scale / 2 ** zoom,
          matrixWidth: 2 ** zoom,
          matrixHeight: 2 ** zoom,
        };
      },
    ),
  };
}

const TILE_MATRIX_SETS = new Map([
  [
    "GoogleMapsCompatible_256",
    createTileMatrixSet("GoogleMapsCompatible_256", 256),
  ],
  [
    "GoogleMapsCompatible_512",
    createTileMatrixSet("GoogleMapsCompatible_512", 512),
  ],
]);

function getTileMatrixSet(id) {
  const identifier = TILE_MATRIX_ALIASES[id] ?? id;

  const matrixSet = TILE_MATRIX_SETS.get(identifier);
  if (!matrixSet) {
    throw new WMTSError(
      "InvalidParameterValue",
      `TileMatrixSet "${id}" is not supported.`,
    );
  }

  return matrixSet;
}

const STYLE_MATRIX_SETS = [
  TILE_MATRIX_SETS.get("GoogleMapsCompatible_256"),
  TILE_MATRIX_SETS.get("GoogleMapsCompatible_512"),
];

function getLayerFormats(layer) {
  if (layer.kind === "style") {
    const configured = layer.item.wmts?.formats;
    const formats = Array.isArray(configured)
      ? configured
      : DEFAULT_STYLE_FORMATS;

    return formats.map(normalizeFormat);
  }

  const format = layer.tileJSON?.format;
  let mime;

  switch (format) {
    case "png":
      mime = "image/png";
      break;
    case "jpg":
    case "jpeg":
      mime = "image/jpeg";
      break;
    case "webp":
      mime = "image/webp";
      break;
    case "pbf":
      mime = "application/x-protobuf";
      break;
  }

  return mime ? [normalizeFormat(mime)] : [];
}

function getLayerMatrixSets(layer) {
  if (layer.kind === "style") {
    return STYLE_MATRIX_SETS;
  }

  const tileSize = layer.tileJSON?.tileSize === 512 ? 512 : 256;
  return [TILE_MATRIX_SETS.get(`GoogleMapsCompatible_${tileSize}`)];
}

function createLayer(kind, id, item) {
  const tileJSON = item.tileJSON ?? {};
  const metadata = item.wmts ?? {};
  const layer = {
    id,
    kind,
    item,
    tileJSON,
    title: metadata.title ?? tileJSON.name ?? item.name ?? id,
    abstract: metadata.abstract ?? tileJSON.description ?? id,
    bbox: normalizeBBox(metadata.bbox ?? item.bbox ?? tileJSON.bounds),
    minZoom: clampZoom(tileJSON.minzoom, 0),
    maxZoom: clampZoom(tileJSON.maxzoom, DEFAULT_MAX_ZOOM),
  };

  layer.formats = getLayerFormats(layer);
  layer.matrixSets = getLayerMatrixSets(layer);
  return layer;
}

let wmtsRegistry;

function createWMTSRegistry(styles, datas) {
  const layers = [];

  for (const [id, item] of Object.entries(styles)) {
    if (item.tileJSON) {
      layers.push(createLayer("style", id, item));
    }
  }

  for (const [id, item] of Object.entries(datas)) {
    const layer = createLayer("data", id, item);
    if (layer.formats.length) {
      layers.push(layer);
    }
  }

  return {
    layers,
    byId: new Map(
      layers.map((layer) => {
        return [layer.id, layer];
      }),
    ),
  };
}

function getWMTSRegistry() {
  const styles = config.styles ?? {};
  const datas = config.datas ?? {};

  if (
    !wmtsRegistry ||
    wmtsRegistry.styles !== styles ||
    wmtsRegistry.datas !== datas
  ) {
    wmtsRegistry = {
      ...createWMTSRegistry(styles, datas),
      styles,
      datas,
    };
  }

  return wmtsRegistry;
}

export function getWMTSLayers() {
  const registry = getWMTSRegistry();
  return registry.layers;
}

function getRegistryLayer(id) {
  const layer = getWMTSRegistry().byId.get(id);
  if (!layer) {
    throw new WMTSError(
      "LayerNotDefined",
      `WMTS layer "${id}" does not exist.`,
      StatusCodes.NOT_FOUND,
    );
  }

  return layer;
}

function getMatrixForLayer(layer, matrixSetId, matrixId) {
  const requestedMatrixSet = getTileMatrixSet(matrixSetId).identifier;
  const matrixSet = layer.matrixSets.find((item) => {
    return item.identifier === requestedMatrixSet;
  });

  if (!matrixSet) {
    throw new WMTSError(
      "InvalidParameterValue",
      `TileMatrixSet "${matrixSetId}" is not available for layer "${layer.id}".`,
    );
  }

  const zoom = Number(matrixId);
  const matrix =
    Number.isInteger(zoom) && String(zoom) === String(matrixId)
      ? matrixSet.matrices[zoom]
      : undefined;

  if (!matrix) {
    throw new WMTSError(
      "TileOutOfRange",
      `TileMatrix "${matrixId}" is not available.`,
    );
  }

  if (zoom < layer.minZoom || zoom > layer.maxZoom) {
    throw new WMTSError(
      "TileOutOfRange",
      `TileMatrix "${matrixId}" is outside the layer zoom range.`,
    );
  }

  return {
    matrixSet,
    matrix,
  };
}

function validateTileCoordinates(matrix, row, col) {
  const tileRow = Number(row);
  const tileCol = Number(col);

  if (
    !Number.isInteger(tileRow) ||
    !Number.isInteger(tileCol) ||
    tileRow < 0 ||
    tileCol < 0 ||
    tileRow >= matrix.matrixHeight ||
    tileCol >= matrix.matrixWidth
  ) {
    throw new WMTSError(
      "TileOutOfRange",
      `Tile coordinates "${col},${row}" are outside the TileMatrix.`,
    );
  }

  return {
    tileRow,
    tileCol,
  };
}

export async function buildCapabilities({
  baseURL,
  layers = getWMTSLayers(),
  title = config.options?.wmts?.title ?? "Tile Server",
  abstract = config.options?.wmts?.abstract ?? "OGC Web Map Tile Service",
}) {
  const matrixSets = [];
  const matrixSetIds = new Set();
  for (const layer of layers) {
    for (const matrixSet of layer.matrixSets) {
      if (!matrixSetIds.has(matrixSet.identifier)) {
        matrixSetIds.add(matrixSet.identifier);
        matrixSets.push(matrixSet);
      }
    }
  }

  const capabilitiesURL = `${baseURL}/wmts`;
  const layerData = layers.map((layer) => {
    const restBase = `${baseURL}/wmts/${encodeURIComponent(layer.id)}/default`;
    const formats = layer.formats.map((format) => {
      return {
        mime: xmlEscape(format.mime),
        template: xmlEscape(
          `${restBase}/{TileMatrixSet}/{TileMatrix}/{TileRow}/{TileCol}.${format.extension}`,
        ),
      };
    });

    return {
      title: xmlEscape(layer.title),
      abstract: xmlEscape(layer.abstract),
      identifier: xmlEscape(layer.id),
      bbox: layer.bbox,
      formats,
      matrixSetLinks: layer.matrixSets.map((matrixSet) => {
        return {
          identifier: xmlEscape(matrixSet.identifier),
          limits: matrixSet.matrices
            .slice(layer.minZoom, layer.maxZoom + 1)
            .map((matrix) => {
              return {
                identifier: xmlEscape(matrix.identifier),
                maxTileRow: matrix.matrixHeight - 1,
                maxTileCol: matrix.matrixWidth - 1,
              };
            }),
        };
      }),
      resources: formats,
    };
  });

  const matrixData = matrixSets.map((matrixSet) => {
    return {
      title: xmlEscape(matrixSet.identifier),
      identifier: xmlEscape(matrixSet.identifier),
      supportedCRS: xmlEscape(matrixSet.supportedCRS),
      matrices: matrixSet.matrices,
    };
  });

  return compileHandleBarsTemplate("wmts", {
    version: WMTS_VERSION,
    updateSequence: CAPABILITIES_UPDATE_SEQUENCE,
    title: xmlEscape(title),
    abstract: xmlEscape(abstract),
    capabilitiesURL: xmlEscape(capabilitiesURL),
    layers: layerData,
    matrixSets: matrixData,
  });
}

let capabilitiesCache;

async function getCapabilitiesXML(baseURL) {
  const registry = getWMTSRegistry();
  const options = config.options?.wmts;
  if (
    !capabilitiesCache ||
    capabilitiesCache.registry !== registry ||
    capabilitiesCache.options !== options
  ) {
    capabilitiesCache = {
      registry,
      options,
      entries: new Map(),
    };
  }

  let promise = capabilitiesCache.entries.get(baseURL);
  if (!promise) {
    promise = buildCapabilities({
      baseURL,
      layers: registry.layers,
    });
    const entries = capabilitiesCache.entries;
    entries.set(baseURL, promise);

    if (entries.size > 32) {
      entries.delete(entries.keys().next().value);
    }

    promise.catch(() => {
      entries.delete(baseURL);
    });
  }

  return promise;
}

async function sendException(res, error) {
  const status =
    error instanceof WMTSError
      ? error.status
      : StatusCodes.INTERNAL_SERVER_ERROR;
  const code = error instanceof WMTSError ? error.code : "NoApplicableCode";
  const message =
    error instanceof WMTSError ? error.message : "Internal server error";
  const body = await compileHandleBarsTemplate("ows_exception", {
    version: "1.1.0",
    code: xmlEscape(code),
    message: xmlEscape(message),
  });

  return res.status(status).set("content-type", "application/xml").send(body);
}

async function getRawTile(layer, z, x, y) {
  const id = layer.id;
  const item = layer.item;

  switch (item.sourceType) {
    case "mbtiles":
      return getAndCacheMBTilesTileData(id, z, x, y);
    case "pmtiles":
      return getPMTilesTile(item.source, z, x, y);
    case "xyz":
      return getAndCacheXYZTileData(id, z, x, y);
    case "pg":
      return getAndCachePostgreSQLTileData(id, z, x, y);
    default:
      throw new WMTSError(
        "NoApplicableCode",
        `Source type "${item.sourceType}" cannot serve WMTS tiles.`,
        StatusCodes.INTERNAL_SERVER_ERROR,
      );
  }
}

async function sendTile(req, res, layer, parameters) {
  const formatValue =
    parameters.FORMAT ??
    parameters.format ??
    getParameter(parameters, "FORMAT");
  const format = parameters.__extension
    ? formatFromExtension(parameters.__extension)
    : normalizeFormat(formatValue);
  const matrixSetValue =
    parameters.TILEMATRIXSET ?? getParameter(parameters, "TILEMATRIXSET");
  const matrixId =
    parameters.TILEMATRIX ?? getParameter(parameters, "TILEMATRIX");
  const matrixSetAlias =
    matrixSetValue === "WebMercatorQuad"
      ? "GoogleMapsCompatible_256"
      : matrixSetValue;
  const { matrixSet, matrix } = getMatrixForLayer(
    layer,
    matrixSetAlias,
    matrixId,
  );
  const { tileRow, tileCol } = validateTileCoordinates(
    matrix,
    parameters.TILEROW ?? getParameter(parameters, "TILEROW"),
    parameters.TILECOL ?? getParameter(parameters, "TILECOL"),
  );

  if (
    !layer.formats.some((item) => {
      return item.mime === format.mime || item.tileFormat === format.tileFormat;
    })
  ) {
    throw new WMTSError(
      "FormatNotSupported",
      `Format "${format.mime}" is not available for layer "${layer.id}".`,
    );
  }

  if (layer.kind === "style") {
    if (await isFileNotModified(req, res, layer.item.path)) {
      return res.status(StatusCodes.NOT_MODIFIED).end();
    }

    const styleJSON = await getRenderedStyleJSON(layer.item.path);
    const image = await renderImageTileData({
      z: Number(matrix.identifier),
      x: tileCol,
      y: tileRow,
      pool: getTileRendererPool({
        key: `wmts:${layer.id}:${matrixSet.tileSize}`,
        styleJSON,
        tileScale: 1,
      }),
      styleJSON,
      tileScale: 1,
      tileSize: matrixSet.tileSize,
      format: format.tileFormat,
    });

    return res
      .status(StatusCodes.OK)
      .set("content-type", format.mime)
      .send(image);
  }

  const tile = await getRawTile(
    layer,
    Number(matrix.identifier),
    tileCol,
    tileRow,
  );
  let data = tile.data;
  const headers = {
    ...(tile.headers ?? {}),
    "content-type": format.mime,
  };

  if (format.tileFormat === "pbf" && headers["content-encoding"]) {
    res.vary("Accept-Encoding");
    data = await normalizeResponseEncoding(data, headers, req);
  }

  return res.status(StatusCodes.OK).set(headers).send(data);
}

async function sendCapabilities(req, res) {
  try {
    normalizeVersion(getParameter(req.query, "VERSION", WMTS_VERSION));
    const baseURL = getRequestHost(req);
    const xml = await getCapabilitiesXML(baseURL);

    return res.status(StatusCodes.OK).set("content-type", "text/xml").send(xml);
  } catch (error) {
    printLog("error", `Failed to build WMTS capabilities: ${error}`);
    return sendException(res, error);
  }
}

function kvpHandler() {
  return async (req, res) => {
    try {
      const parameters = req.query ?? {};
      const operation = normalizeOperation(getParameter(parameters, "REQUEST"));

      if (operation === "GetCapabilities") {
        return await sendCapabilities(req, res);
      }

      normalizeVersion(getParameter(parameters, "VERSION", WMTS_VERSION));

      const layerId = getParameter(parameters, "LAYER");
      if (!layerId) {
        throw new WMTSError("MissingParameterValue", "LAYER is required.");
      }

      const layer = getRegistryLayer(layerId);
      const style = getParameter(parameters, "STYLE", "default");
      if (style !== "default") {
        throw new WMTSError(
          "StyleNotDefined",
          `Style "${style}" does not exist.`,
        );
      }

      return await sendTile(req, res, layer, parameters);
    } catch (error) {
      printLog("error", `Failed to serve WMTS KVP request: ${error}`);
      return sendException(res, error);
    }
  };
}

function restTileHandler({ compact = false } = {}) {
  return async (req, res) => {
    try {
      const layerId = req.params.layer;
      const layer = getRegistryLayer(layerId);
      const style = compact ? "default" : req.params.style;
      if (style !== "default") {
        throw new WMTSError(
          "StyleNotDefined",
          `Style "${style}" does not exist.`,
        );
      }

      return await sendTile(req, res, layer, {
        TILEMATRIXSET: req.params.tileMatrixSet,
        TILEMATRIX: req.params.tileMatrix,
        TILEROW: req.params.tileRow,
        TILECOL: req.params.tileCol,
        __extension: req.params.format,
      });
    } catch (error) {
      printLog("error", `Failed to serve WMTS REST tile: ${error}`);
      return sendException(res, error);
    }
  };
}

export const serve_wmts = {
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: WMTS
     *     description: OGC Web Map Tile Service endpoints
     * /wmts:
     *   get:
     *     tags: [WMTS]
     *     summary: Execute a WMTS KVP request
     *     parameters:
     *       - in: query
     *         name: REQUEST
     *         required: true
     *         schema: { type: string, enum: [GetCapabilities, GetTile] }
     *       - in: query
     *         name: VERSION
     *         schema: { type: string, default: '1.0.0' }
     *       - in: query
     *         name: LAYER
     *         schema: { type: string }
     *       - in: query
     *         name: FORMAT
     *         schema: { type: string, example: image/png }
     *       - in: query
     *         name: TILEMATRIXSET
     *         schema: { type: string, example: GoogleMapsCompatible_256 }
     *       - in: query
     *         name: TILEMATRIX
     *         schema: { type: string, example: '0' }
     *       - in: query
     *         name: TILEROW
     *         schema: { type: integer, minimum: 0 }
     *       - in: query
     *         name: TILECOL
     *         schema: { type: integer, minimum: 0 }
     *     responses:
     *       200:
     *         description: WMTS capabilities XML or tile bytes.
     *         content:
     *           application/xml: { schema: { type: string } }
     *           image/png: { schema: { type: string, format: binary } }
     *           application/x-protobuf: { schema: { type: string, format: binary } }
     *       400:
     *         description: OGC exception report.
     */
    /**
     * @swagger
     * /wmts/1.0.0/WMTSCapabilities.xml:
     *   get:
     *     tags: [WMTS]
     *     summary: Get WMTS capabilities XML
     *     parameters:
     *       - in: query
     *         name: VERSION
     *         schema: { type: string, enum: ['1.0.0'], default: '1.0.0' }
     *     responses:
     *       200:
     *         description: WMTS 1.0.0 capabilities document.
     *         content:
     *           application/xml: { schema: { type: string } }
     *       400:
     *         description: OGC exception report.
     */
    /**
     * @swagger
     * /wmts/{layer}/{style}/{tileMatrixSet}/{tileMatrix}/{tileRow}/{tileCol}.{format}:
     *   get:
     *     tags: [WMTS]
     *     summary: Get a WMTS REST tile
     *     parameters:
     *       - { in: path, name: layer, required: true, schema: { type: string } }
     *       - { in: path, name: style, required: true, schema: { type: string, default: default } }
     *       - { in: path, name: tileMatrixSet, required: true, schema: { type: string } }
     *       - { in: path, name: tileMatrix, required: true, schema: { type: string } }
     *       - { in: path, name: tileRow, required: true, schema: { type: integer, minimum: 0 } }
     *       - { in: path, name: tileCol, required: true, schema: { type: integer, minimum: 0 } }
     *       - { in: path, name: format, required: true, schema: { type: string, enum: [png, jpg, jpeg, webp, pbf] } }
     *     responses:
     *       200:
     *         description: Tile bytes.
     *         content:
     *           image/png: { schema: { type: string, format: binary } }
     *           application/x-protobuf: { schema: { type: string, format: binary } }
     *       400:
     *         description: OGC exception report.
     */
    /**
     * @swagger
     * /wmts/{layer}/{tileMatrixSet}/{tileMatrix}/{tileRow}/{tileCol}.{format}:
     *   get:
     *     tags: [WMTS]
     *     summary: Get a WMTS REST tile using the default style
     *     parameters:
     *       - { in: path, name: layer, required: true, schema: { type: string } }
     *       - { in: path, name: tileMatrixSet, required: true, schema: { type: string } }
     *       - { in: path, name: tileMatrix, required: true, schema: { type: string } }
     *       - { in: path, name: tileRow, required: true, schema: { type: integer, minimum: 0 } }
     *       - { in: path, name: tileCol, required: true, schema: { type: integer, minimum: 0 } }
     *       - { in: path, name: format, required: true, schema: { type: string, enum: [png, jpg, jpeg, webp, pbf] } }
     *     responses:
     *       200:
     *         description: Tile bytes.
     *       400:
     *         description: OGC exception report.
     */
    app.get("/wmts", kvpHandler());
    app.get("/wmts/1.0.0/WMTSCapabilities.xml", sendCapabilities);
    app.get(
      "/wmts/:layer/:style/:tileMatrixSet/:tileMatrix/:tileRow/:tileCol.:format",
      restTileHandler(),
    );
    app.get(
      "/wmts/:layer/:tileMatrixSet/:tileMatrix/:tileRow/:tileCol.:format",
      restTileHandler({
        compact: true,
      }),
    );
  },
};

export { getTileMatrixSet, normalizeFormat, normalizeVersion };
