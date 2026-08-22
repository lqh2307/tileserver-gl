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
  isFileNotModified,
  getRequestHost,
  getParameter,
  xmlEscape,
  printLog,
} from "../utils/index.js";

const WMTS_VERSION = "1.0.0";
const WMTS_NAMESPACE = "http://www.opengis.net/wmts/1.0";
const OWS_NAMESPACE = "http://www.opengis.net/ows/1.1";
const XLINK_NAMESPACE = "http://www.w3.org/1999/xlink";
const XML_SCHEMA_INSTANCE_NAMESPACE =
  "http://www.w3.org/2001/XMLSchema-instance";
const WEB_MERCATOR_HALF_WORLD = 20037508.342789244;
const WEB_MERCATOR_SCALE_256 = 559082264.0287178;
const DEFAULT_MAX_ZOOM = 22;
const DEFAULT_BBOX = [-180, -85.051129, 180, 85.051129];
const CAPABILITIES_UPDATE_SEQUENCE = String(Date.now());

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
  return bbox.every(Number.isFinite) ? bbox : DEFAULT_BBOX;
}

function clampZoom(value, fallback) {
  const zoom = Number(value);
  return Number.isInteger(zoom) && zoom >= 0
    ? Math.min(zoom, DEFAULT_MAX_ZOOM)
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
  const aliases = {
    256: "GoogleMapsCompatible_256",
    512: "GoogleMapsCompatible_512",
    GoogleMapsCompatible: "GoogleMapsCompatible_256",
    WebMercatorQuad: "GoogleMapsCompatible_256",
  };
  const identifier = aliases[id] ?? id;

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
      : ["image/png", "image/jpeg", "image/webp"];

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

export function getWMTSLayers(pathLayerId) {
  const registry = getWMTSRegistry();

  if (pathLayerId) {
    const layer = registry.byId.get(pathLayerId);
    if (!layer || layer.kind !== "style") {
      throw new WMTSError(
        "LayerNotDefined",
        `WMTS layer "${pathLayerId}" does not exist.`,
        StatusCodes.NOT_FOUND,
      );
    }

    return [layer];
  }

  return registry.layers;
}

function getLayer(layers, id) {
  const registry = getWMTSRegistry();
  if (layers === registry.layers) {
    const layer = registry.byId.get(id);
    if (layer) {
      return layer;
    }
  }

  const layer = layers.find((item) => {
    return item.id === id;
  });

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

function operationXML(name, href, encodings) {
  return `<ows:Operation name="${name}"><ows:DCP><ows:HTTP><ows:Get xlink:href="${xmlEscape(href)}"><ows:Constraint name="GetEncoding"><ows:AllowedValues>${encodings
    .map((encoding) => {
      return `<ows:Value>${encoding}</ows:Value>`;
    })
    .join(
      "",
    )}</ows:AllowedValues></ows:Constraint></ows:Get></ows:HTTP></ows:DCP></ows:Operation>`;
}

function tileMatrixXML(matrixSet) {
  return matrixSet.matrices
    .map((matrix) => {
      return `<TileMatrix><ows:Identifier>${matrix.identifier}</ows:Identifier><ScaleDenominator>${matrix.scaleDenominator}</ScaleDenominator><TopLeftCorner>${matrixSet.topLeft[0]} ${matrixSet.topLeft[1]}</TopLeftCorner><TileWidth>${matrixSet.tileSize}</TileWidth><TileHeight>${matrixSet.tileSize}</TileHeight><MatrixWidth>${matrix.matrixWidth}</MatrixWidth><MatrixHeight>${matrix.matrixHeight}</MatrixHeight></TileMatrix>`;
    })
    .join("");
}

function layerXML(layer, baseURL, pathLayerId) {
  const restBase = pathLayerId
    ? `${baseURL}/styles/${encodeURIComponent(layer.id)}/wmts`
    : `${baseURL}/wmts/${encodeURIComponent(layer.id)}/default`;
  const bbox = layer.bbox;
  const formats = layer.formats
    .map((format) => {
      return `<Format>${xmlEscape(format.mime)}</Format>`;
    })
    .join("");
  const links = layer.matrixSets
    .map((matrixSet) => {
      const limits = matrixSet.matrices
        .slice(layer.minZoom, layer.maxZoom + 1)
        .map((matrix) => {
          return `<TileMatrixLimits><TileMatrix>${matrix.identifier}</TileMatrix><MinTileRow>0</MinTileRow><MaxTileRow>${matrix.matrixHeight - 1}</MaxTileRow><MinTileCol>0</MinTileCol><MaxTileCol>${matrix.matrixWidth - 1}</MaxTileCol></TileMatrixLimits>`;
        })
        .join("");

      return `<TileMatrixSetLink><TileMatrixSet>${matrixSet.identifier}</TileMatrixSet><TileMatrixSetLimits>${limits}</TileMatrixSetLimits></TileMatrixSetLink>`;
    })
    .join("");
  const resources = layer.formats
    .map((format) => {
      return `<ResourceURL format="${xmlEscape(format.mime)}" resourceType="tile" template="${xmlEscape(`${restBase}/{TileMatrixSet}/{TileMatrix}/{TileRow}/{TileCol}.${format.extension}`)}"/>`;
    })
    .join("");

  return `<Layer><ows:Title>${xmlEscape(layer.title)}</ows:Title><ows:Abstract>${xmlEscape(layer.abstract)}</ows:Abstract><ows:Identifier>${xmlEscape(layer.id)}</ows:Identifier><ows:WGS84BoundingBox crs="urn:ogc:def:crs:OGC:2:84"><ows:LowerCorner>${bbox[0]} ${bbox[1]}</ows:LowerCorner><ows:UpperCorner>${bbox[2]} ${bbox[3]}</ows:UpperCorner></ows:WGS84BoundingBox><Style isDefault="true"><ows:Identifier>default</ows:Identifier><ows:Title>Default</ows:Title></Style>${formats}${links}${resources}</Layer>`;
}

export function buildCapabilities({
  baseURL,
  layers = getWMTSLayers(),
  title = config.options?.wmts?.title ?? "Tile Server",
  abstract = config.options?.wmts?.abstract ?? "OGC Web Map Tile Service",
  pathLayerId,
}) {
  const matrixSets = [];
  for (const layer of layers) {
    for (const matrixSet of layer.matrixSets) {
      if (
        !matrixSets.some((item) => {
          return item.identifier === matrixSet.identifier;
        })
      ) {
        matrixSets.push(matrixSet);
      }
    }
  }

  const capabilitiesURL = `${baseURL}${pathLayerId ? `/styles/${encodeURIComponent(pathLayerId)}/wmts.xml` : "/wmts"}`;
  const tileServiceURL = `${baseURL}/wmts`;
  const layerContent = layers
    .map((layer) => {
      return layerXML(layer, baseURL, pathLayerId);
    })
    .join("");
  const matrixContent = matrixSets
    .map((matrixSet) => {
      return `<TileMatrixSet><ows:Title>${matrixSet.identifier}</ows:Title><ows:Identifier>${matrixSet.identifier}</ows:Identifier><ows:SupportedCRS>${matrixSet.supportedCRS}</ows:SupportedCRS>${tileMatrixXML(matrixSet)}</TileMatrixSet>`;
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?><Capabilities xmlns="${WMTS_NAMESPACE}" xmlns:ows="${OWS_NAMESPACE}" xmlns:xlink="${XLINK_NAMESPACE}" xmlns:xsi="${XML_SCHEMA_INSTANCE_NAMESPACE}" version="${WMTS_VERSION}" updateSequence="${CAPABILITIES_UPDATE_SEQUENCE}"><ows:ServiceIdentification><ows:Title>${xmlEscape(title)}</ows:Title><ows:Abstract>${xmlEscape(abstract)}</ows:Abstract><ows:ServiceType codeSpace="OGC">WMTS</ows:ServiceType><ows:ServiceTypeVersion>${WMTS_VERSION}</ows:ServiceTypeVersion></ows:ServiceIdentification><ows:OperationsMetadata>${operationXML("GetCapabilities", capabilitiesURL, ["KVP", "RESTful"])}${operationXML("GetTile", tileServiceURL, ["KVP", "RESTful"])}</ows:OperationsMetadata><Contents>${layerContent}${matrixContent}</Contents><ServiceMetadataURL xlink:href="${xmlEscape(capabilitiesURL)}"/></Capabilities>`;
}

function sendException(res, error) {
  const status =
    error instanceof WMTSError
      ? error.status
      : StatusCodes.INTERNAL_SERVER_ERROR;
  const code = error instanceof WMTSError ? error.code : "NoApplicableCode";
  const message =
    error instanceof WMTSError ? error.message : "Internal server error";
  const body = `<?xml version="1.0" encoding="UTF-8"?><ows:ExceptionReport xmlns:ows="${OWS_NAMESPACE}" version="1.1.0"><ows:Exception exceptionCode="${xmlEscape(code)}"><ows:ExceptionText>${xmlEscape(message)}</ows:ExceptionText></ows:Exception></ows:ExceptionReport>`;

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
  const headers = {
    ...(tile.headers ?? {}),
    "content-type": format.mime,
  };

  return res.status(StatusCodes.OK).set(headers).send(tile.data);
}

function capabilitiesHandler(pathLayerId) {
  return (req, res) => {
    try {
      normalizeVersion(getParameter(req.query, "VERSION", WMTS_VERSION));
      const baseURL = getRequestHost(req);
      const actualLayerId = pathLayerId ?? req.params.id;
      const layers = getWMTSLayers(actualLayerId);
      const xml = buildCapabilities({
        baseURL,
        layers,
        pathLayerId: actualLayerId,
      });

      return res
        .status(StatusCodes.OK)
        .set("content-type", "text/xml")
        .send(xml);
    } catch (error) {
      printLog("error", `Failed to build WMTS capabilities: ${error}`);
      return sendException(res, error);
    }
  };
}

function kvpHandler() {
  return async (req, res) => {
    try {
      const parameters = req.query ?? {};
      const operation = normalizeOperation(getParameter(parameters, "REQUEST"));
      normalizeVersion(getParameter(parameters, "VERSION", WMTS_VERSION));

      if (operation === "GetCapabilities") {
        return capabilitiesHandler()(req, res);
      }

      const layerId = getParameter(parameters, "LAYER");
      if (!layerId) {
        throw new WMTSError("MissingParameterValue", "LAYER is required.");
      }

      const layer = getLayer(getWMTSLayers(), layerId);
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

function restTileHandler({ pathStyle = false, compact = false } = {}) {
  return async (req, res) => {
    try {
      const layerId = pathStyle ? req.params.id : req.params.layer;
      const layer = getLayer(getWMTSLayers(), layerId);
      const style = pathStyle
        ? "default"
        : compact
          ? "default"
          : req.params.style;
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
    app.get("/wmts", kvpHandler());
    app.get("/wmts/1.0.0/WMTSCapabilities.xml", capabilitiesHandler());
    app.get("/styles/:id/wmts.xml", capabilitiesHandler());
    app.get(
      "/styles/:id/wmts/:tileMatrixSet/:tileMatrix/:tileRow/:tileCol.:format",
      restTileHandler({
        pathStyle: true,
      }),
    );
    app.get(
      "/styles/:id/:tileMatrixSet/:tileMatrix/:tileRow/:tileCol.:format",
      restTileHandler({
        pathStyle: true,
      }),
    );
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
