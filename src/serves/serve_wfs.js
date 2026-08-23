"use strict";

import { config } from "../configs/index.js";
import { nanoid } from "nanoid";
import express from "express";
import {
  getAndCacheDataGeoJSON,
  storeGeoJSONFile,
} from "../resources/index.js";
import {
  transformPointSRS,
  transformBBoxSRS,
  getGeometryBBox,
  splitParameter,
  getRequestHost,
  getParameter,
  xmlEscape,
  mins,
  maxs,
  min,
  max,
} from "../utils/index.js";

const WFS_1_0_0 = "1.0.0";
const WFS_1_1_0 = "1.1.0";
const WFS_2_0_0 = "2.0.0";
const WFS_NAMESPACE = "http://www.opengis.net/wfs";
const WFS_FEATURE_NAMESPACE = "http://www.example.com/wfs";
const GML_NAMESPACE = "http://www.opengis.net/gml";
const GML32_NAMESPACE = "http://www.opengis.net/gml/3.2";
const OWS_NAMESPACE = "http://www.opengis.net/ows/1.1";
const FES_NAMESPACE = "http://www.opengis.net/fes/2.0";
const MAX_FEATURES = 10000;
const DEFAULT_COUNT = 1000;

class WFSException extends Error {
  constructor(code, message) {
    super(message);
    this.name = "WFSException";
    this.code = code;
  }
}

const locks = new Map();
const storedQueries = new Map();

function xmlName(value) {
  const name = String(value ?? "value").replace(/[^a-zA-Z0-9_.-]/g, "_");
  return /^[a-zA-Z_]/.test(name) ? name : `_${name}`;
}

function localName(value) {
  return String(value ?? "")
    .split(":")
    .pop();
}

function normalizeVersion(value) {
  const version = String(value ?? WFS_2_0_0);
  if (![WFS_1_0_0, WFS_1_1_0, WFS_2_0_0].includes(version)) {
    throw new WFSException(
      "VersionNegotiationFailed",
      `Unsupported WFS version "${version}".`,
    );
  }
  return version;
}

function normalizeCRS(value) {
  const crs = String(value ?? "EPSG:4326")
    .trim()
    .toUpperCase();
  if (crs === "CRS84" || crs === "CRS:84" || crs.includes("CRS84")) {
    return "EPSG:4326";
  }
  const urn = crs.match(/^URN:OGC:DEF:CRS:EPSG:(?::|\d+:)(\d+)$/);
  return urn ? `EPSG:${urn[1]}` : crs;
}

let featureTypesCache;

function getFeatureTypes() {
  const geojsons = config.geojsons ?? {};
  if (featureTypesCache?.source === geojsons) {
    return featureTypesCache.types;
  }

  const result = [];
  for (const [group, layers] of Object.entries(geojsons)) {
    for (const [layer, item] of Object.entries(layers ?? {})) {
      result.push({
        name: `${group}:${layer}`,
        group,
        layer,
        item,
        title: item.title ?? `${group} - ${layer}`,
      });
    }
  }
  const byName = new Map();
  const byPath = new Map();
  const byLayer = new Map();
  for (const type of result) {
    byName.set(type.name, type);
    byPath.set(`${type.group}/${type.layer}`, type);
    const aliases = byLayer.get(type.layer) ?? [];
    aliases.push(type);
    byLayer.set(type.layer, aliases);
  }
  featureTypesCache = {
    source: geojsons,
    types: result,
    byName,
    byPath,
    byLayer,
  };
  return result;
}

function resolveFeatureType(name) {
  const value = String(name ?? "");
  getFeatureTypes();
  const type =
    featureTypesCache.byName.get(value) ?? featureTypesCache.byPath.get(value);
  if (type) {
    return type;
  }

  const aliases = featureTypesCache.byLayer.get(value) ?? [];
  if (aliases.length === 1) {
    return aliases[0];
  }
  throw new WFSException(
    "InvalidParameterValue",
    `Feature type "${value}" does not exist.`,
  );
}

function resolveFeatureTypes(parameters, pathName) {
  const values = pathName
    ? [pathName]
    : splitParameter(
        getParameter(
          parameters,
          "TYPENAMES",
          getParameter(parameters, "TYPENAME"),
        ),
      );
  if (!values.length) {
    throw new WFSException("MissingParameterValue", "TYPENAMES is required.");
  }
  return values.map(resolveFeatureType);
}

async function readFeatures(type) {
  const data = JSON.parse(await getAndCacheDataGeoJSON(type.group, type.layer));
  if (data.type === "FeatureCollection") {
    return data.features ?? [];
  }
  if (data.type === "Feature") {
    return [data];
  }
  return [
    {
      type: "Feature",
      properties: {},
      geometry: data,
    },
  ];
}

function featureId(feature, type, index) {
  return String(feature.id ?? `${type.group}.${type.layer}.${index + 1}`);
}

function bboxIntersects(first, second) {
  return !(
    first[2] < second[0] ||
    first[0] > second[2] ||
    first[3] < second[1] ||
    first[1] > second[3]
  );
}

function parseBBox(value) {
  if (!value) {
    return;
  }
  const parts = String(value).split(",");
  const values = parts.slice(0, 4).map(Number);
  if (
    parts.length < 4 ||
    values.some((item) => {
      return !Number.isFinite(item);
    })
  ) {
    throw new WFSException(
      "InvalidParameterValue",
      "BBOX must contain four numbers.",
    );
  }
  const crs = normalizeCRS(parts[4] ?? "EPSG:4326");
  try {
    return transformBBoxSRS({
      srcSRS: crs,
      dstSRS: "EPSG:4326",
      bounds: values.slice(0, 4),
    });
  } catch (error) {
    throw new WFSException(
      "InvalidParameterValue",
      `BBOX CRS is not supported: ${error.message}`,
    );
  }
}

function propertyValue(feature, name) {
  if (name === "id" || name === "fid") {
    return feature.id;
  }
  return feature.properties?.[name];
}

function compareValue(actual, expected, operator) {
  const left = actual instanceof Date ? actual.getTime() : actual;
  const right = expected instanceof Date ? expected.getTime() : expected;
  switch (operator) {
    case "eq":
      return String(left ?? "") === String(right ?? "");
    case "neq":
      return String(left ?? "") !== String(right ?? "");
    case "lt":
      return left < right;
    case "lte":
      return left <= right;
    case "gt":
      return left > right;
    case "gte":
      return left >= right;
    default:
      return false;
  }
}

function tagValue(xml, tag) {
  const match = String(xml ?? "").match(
    new RegExp(`<[^>]*${tag}[^>]*>([\\s\\S]*?)</[^>]*${tag}>`, "i"),
  );
  return match ? match[1].replace(/<[^>]+>/g, "").trim() : undefined;
}

function filterFromXML(xml) {
  if (!xml) {
    return;
  }
  const source = String(xml).replace(/\s+/g, " ");
  const resource = source.match(/ResourceId[^>]+(?:rid|fid)=["']([^"']+)["']/i);
  if (resource) {
    return {
      type: "id",
      value: resource[1],
    };
  }

  const bbox = source.match(
    /<(?:[^:>]+:)?BBOX[\s\S]*?>([\s\S]*?)<\/(?:[^:>]+:)?BBOX>/i,
  );
  if (bbox) {
    const lower =
      tagValue(bbox[1], "lowerCorner") ?? tagValue(bbox[1], "lowercorner");
    const upper =
      tagValue(bbox[1], "upperCorner") ?? tagValue(bbox[1], "uppercorner");
    if (lower && upper) {
      return {
        type: "bbox",
        value: parseBBox(
          `${lower.split(/\s+/).join(",")},${upper.split(/\s+/).join(",")}`,
        ),
      };
    }
  }

  const logical = source.match(
    /<(?:[^:>]+:)?(And|Or|Not)[^>]*>([\s\S]*?)<\/(?:[^:>]+:)?\1>/i,
  );
  if (logical) {
    const children = [
      ...logical[2].matchAll(
        /<(?:[^:>]+:)?(?:PropertyIs\w+|BBOX|ResourceId|And|Or|Not)[^>]*>[\s\S]*?<\/(?:[^:>]+:)?(?:PropertyIs\w+|BBOX|ResourceId|And|Or|Not)>/gi,
      ),
    ]
      .map((item) => {
        return filterFromXML(item[0]);
      })
      .filter(Boolean);
    return {
      type: logical[1].toLowerCase(),
      children,
    };
  }

  const comparison = source.match(
    /<(?:[^:>]+:)?(PropertyIsEqualTo|PropertyIsNotEqualTo|PropertyIsLessThan|PropertyIsLessThanOrEqualTo|PropertyIsGreaterThan|PropertyIsGreaterThanOrEqualTo)[^>]*>([\s\S]*?)<\/(?:[^:>]+:)?\1>/i,
  );
  if (comparison) {
    const property =
      tagValue(comparison[2], "PropertyName") ??
      tagValue(comparison[2], "ValueReference");
    const literal = tagValue(comparison[2], "Literal");
    const operator = {
      PropertyIsEqualTo: "eq",
      PropertyIsNotEqualTo: "neq",
      PropertyIsLessThan: "lt",
      PropertyIsLessThanOrEqualTo: "lte",
      PropertyIsGreaterThan: "gt",
      PropertyIsGreaterThanOrEqualTo: "gte",
    }[comparison[1]];
    return {
      type: "comparison",
      property,
      value: literal,
      operator,
    };
  }

  const like = source.match(
    /<(?:[^:>]+:)?PropertyIsLike[^>]*wildCard=["']([^"']+)["'][^>]*>([\s\S]*?)<\/(?:[^:>]+:)?PropertyIsLike>/i,
  );
  if (like) {
    return {
      type: "like",
      property: tagValue(like[2], "PropertyName"),
      value: tagValue(like[2], "Literal"),
      wildcard: like[1],
    };
  }
  return;
}

function filterFromCQL(value) {
  if (!value) {
    return;
  }
  const match = String(value).match(
    /^\s*([\w.-]+)\s*(=|<>|<=|>=|<|>)\s*['"]?([^'"]+)['"]?\s*$/i,
  );
  if (!match) {
    return;
  }
  return {
    type: "comparison",
    property: match[1],
    value: match[3],
    operator: {
      "=": "eq",
      "<>": "neq",
      "<": "lt",
      "<=": "lte",
      ">": "gt",
      ">=": "gte",
    }[match[2]],
  };
}

function matchesFilter(feature, filter, type, index) {
  if (!filter) {
    return true;
  }
  switch (filter.type) {
    case "id":
      return featureId(feature, type, index) === filter.value;
    case "bbox":
      return Boolean(
        getGeometryBBox(feature.geometry) &&
        bboxIntersects(getGeometryBBox(feature.geometry), filter.value),
      );
    case "comparison":
      return compareValue(
        propertyValue(feature, filter.property),
        filter.value,
        filter.operator,
      );
    case "like": {
      const expression = String(filter.value)
        .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
        .replaceAll(filter.wildcard, ".*");
      return new RegExp(`^${expression}$`, "i").test(
        String(propertyValue(feature, filter.property) ?? ""),
      );
    }
    case "and":
      return filter.children.every((child) => {
        return matchesFilter(feature, child, type, index);
      });
    case "or":
      return filter.children.some((child) => {
        return matchesFilter(feature, child, type, index);
      });
    case "not":
      return !filter.children.some((child) => {
        return matchesFilter(feature, child, type, index);
      });
    default:
      return true;
  }
}

function geometryTransform(geometry, srcCRS, dstCRS) {
  if (!geometry || srcCRS === dstCRS) {
    return geometry;
  }
  const coordinates = (value) => {
    if (Array.isArray(value) && typeof value[0] === "number") {
      return transformPointSRS({
        srcSRS: srcCRS,
        dstSRS: dstCRS,
        point: value.slice(0, 2),
      }).concat(value.slice(2));
    }
    return value?.map(coordinates);
  };
  if (geometry.type === "GeometryCollection") {
    return {
      ...geometry,
      geometries: geometry.geometries.map((item) => {
        return geometryTransform(item, srcCRS, dstCRS);
      }),
    };
  }
  return {
    ...geometry,
    coordinates: coordinates(geometry.coordinates),
  };
}

function projectFeature(feature, type, index, srsName) {
  return {
    ...feature,
    id: featureId(feature, type, index),
    geometry: geometryTransform(feature.geometry, "EPSG:4326", srsName),
  };
}

function fieldSchema(features) {
  const fields = new Map();
  for (const feature of features) {
    for (const [name, value] of Object.entries(feature.properties ?? {})) {
      const type =
        typeof value === "number"
          ? Number.isInteger(value)
            ? "integer"
            : "double"
          : typeof value === "boolean"
            ? "boolean"
            : "string";
      fields.set(
        name,
        fields.get(name) === "string" ? type : (fields.get(name) ?? type),
      );
    }
  }
  return fields;
}

function positionText(coordinate) {
  return coordinate.slice(0, 2).join(" ");
}

function gmlGeometry(geometry, gmlNS, srsName) {
  if (!geometry) {
    return "";
  }
  const attr = ` srsName="${xmlEscape(srsName)}"`;
  const line = (coordinates) => {
    return `<gml:LineString${attr}><gml:posList>${coordinates.map(positionText).join(" ")}</gml:posList></gml:LineString>`;
  };
  switch (geometry.type) {
    case "Point":
      return `<gml:Point${attr}><gml:pos>${positionText(geometry.coordinates)}</gml:pos></gml:Point>`;
    case "LineString":
      return line(geometry.coordinates);
    case "MultiLineString":
      return `<gml:MultiCurve${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:curveMember>${line(item)}</gml:curveMember>`;
        })
        .join("")}</gml:MultiCurve>`;
    case "Polygon":
      return `<gml:Polygon${attr}><gml:exterior><gml:LinearRing><gml:posList>${geometry.coordinates[0].map(positionText).join(" ")}</gml:posList></gml:LinearRing></gml:exterior>${geometry.coordinates
        .slice(1)
        .map((ring) => {
          return `<gml:interior><gml:LinearRing><gml:posList>${ring.map(positionText).join(" ")}</gml:posList></gml:LinearRing></gml:interior>`;
        })
        .join("")}</gml:Polygon>`;
    case "MultiPolygon":
      return `<gml:MultiSurface${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:surfaceMember>${gmlGeometry(
            {
              type: "Polygon",
              coordinates: item,
            },
            gmlNS,
            srsName,
          )}</gml:surfaceMember>`;
        })
        .join("")}</gml:MultiSurface>`;
    case "MultiPoint":
      return `<gml:MultiPoint${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:pointMember><gml:Point${attr}><gml:pos>${positionText(item)}</gml:pos></gml:Point></gml:pointMember>`;
        })
        .join("")}</gml:MultiPoint>`;
    case "GeometryCollection":
      return `<gml:MultiGeometry${attr}>${geometry.geometries
        .map((item) => {
          return `<gml:geometryMember>${gmlGeometry(item, gmlNS, srsName)}</gml:geometryMember>`;
        })
        .join("")}</gml:MultiGeometry>`;
    default:
      return "";
  }
}

function featureToGML(feature, type, index, version, srsName) {
  const gmlNS = version === WFS_2_0_0 ? GML32_NAMESPACE : GML_NAMESPACE;
  const featureName = xmlName(type.layer);
  const properties = Object.entries(feature.properties ?? {})
    .map(([name, value]) => {
      return `<${xmlName(name)}>${xmlEscape(value)}</${xmlName(name)}>`;
    })
    .join("");
  const geometry = feature.geometry
    ? `<geometry>${gmlGeometry(geometryTransform(feature.geometry, "EPSG:4326", srsName), gmlNS, srsName)}</geometry>`
    : "";
  return `<feature:${featureName} gml:id="${xmlEscape(featureId(feature, type, index))}" xmlns:feature="${WFS_FEATURE_NAMESPACE}">${properties}${geometry}</feature:${featureName}>`;
}

function featureCollectionGML(
  items,
  version,
  srsName,
  matched,
  lockId,
  output,
) {
  const gmlNS =
    output === "gml2"
      ? GML_NAMESPACE
      : version === WFS_2_0_0
        ? GML32_NAMESPACE
        : GML_NAMESPACE;
  const memberTag = version === WFS_2_0_0 ? "wfs:member" : "gml:featureMember";
  const members = items
    .map(({ feature, type, index }) => {
      return `<${memberTag}>${featureToGML(feature, type, index, version, srsName)}</${memberTag}>`;
    })
    .join("");
  const count =
    version === WFS_2_0_0
      ? `numberMatched="${matched}" numberReturned="${items.length}"`
      : `numberOfFeatures="${items.length}"`;
  return `<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection xmlns:wfs="${WFS_NAMESPACE}" xmlns:gml="${gmlNS}" xmlns:feature="${WFS_FEATURE_NAMESPACE}" timeStamp="${new Date().toISOString()}" ${count}${lockId ? ` lockId="${xmlEscape(lockId)}"` : ""}>${members}</wfs:FeatureCollection>`;
}

function featureCollectionJSON(features, matched, startIndex) {
  return JSON.stringify({
    type: "FeatureCollection",
    numberMatched: matched,
    numberReturned: features.length,
    totalFeatures: matched,
    startIndex,
    features,
  });
}

async function descriptor(type) {
  const features = await readFeatures(type);
  const points = features
    .map((feature) => {
      return getGeometryBBox(feature.geometry);
    })
    .filter(Boolean);
  const bbox = points.length
    ? [
        mins(
          points.map((item) => {
            return item[0];
          }),
        ),
        mins(
          points.map((item) => {
            return item[1];
          }),
        ),
        maxs(
          points.map((item) => {
            return item[2];
          }),
        ),
        maxs(
          points.map((item) => {
            return item[3];
          }),
        ),
      ]
    : [-180, -90, 180, 90];
  return {
    ...type,
    features,
    bbox,
    fields: fieldSchema(features),
  };
}

async function capabilities(version, baseURL) {
  const types = await Promise.all(getFeatureTypes().map(descriptor));
  const operations = [
    "GetCapabilities",
    "DescribeFeatureType",
    "GetPropertyValue",
    "GetFeature",
    "GetFeatureWithLock",
    "LockFeature",
    "Transaction",
    "CreateStoredQuery",
    "DropStoredQuery",
    "ListStoredQueries",
    "DescribeStoredQueries",
  ];
  const operationXML = operations
    .map((operation) => {
      return `<ows:Operation name="${operation}"><ows:DCP><ows:HTTP><ows:Get xlink:href="${xmlEscape(baseURL)}"/><ows:Post xlink:href="${xmlEscape(baseURL)}"/></ows:HTTP></ows:DCP></ows:Operation>`;
    })
    .join("");
  const typeXML = types
    .map((type) => {
      return `<wfs:FeatureType><wfs:Name>${xmlEscape(type.name)}</wfs:Name><wfs:Title>${xmlEscape(type.title)}</wfs:Title><wfs:DefaultCRS>urn:ogc:def:crs:OGC:1.3:CRS84</wfs:DefaultCRS><wfs:OtherCRS>urn:ogc:def:crs:EPSG::3857</wfs:OtherCRS><ows:WGS84BoundingBox><ows:LowerCorner>${type.bbox[0]} ${type.bbox[1]}</ows:LowerCorner><ows:UpperCorner>${type.bbox[2]} ${type.bbox[3]}</ows:UpperCorner></ows:WGS84BoundingBox><wfs:OutputFormats><wfs:Format>application/json</wfs:Format><wfs:Format>application/gml+xml; version=3.2</wfs:Format><wfs:Format>GML2</wfs:Format></wfs:OutputFormats></wfs:FeatureType>`;
    })
    .join("");
  if (version === WFS_1_0_0) {
    return `<?xml version="1.0" encoding="UTF-8"?><WFS_Capabilities version="1.0.0" xmlns="${WFS_NAMESPACE}" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink"><Service><Name>WFS</Name><Title>Tile Server WFS</Title><OnlineResource>${xmlEscape(baseURL)}</OnlineResource></Service><Capability><Request><GetCapabilities><DCPType><HTTP><Get onlineResource="${xmlEscape(baseURL)}"/></HTTP></DCPType></GetCapabilities><DescribeFeatureType><DCPType><HTTP><Get onlineResource="${xmlEscape(baseURL)}"/></HTTP></DCPType></DescribeFeatureType><GetFeature><DCPType><HTTP><Get onlineResource="${xmlEscape(baseURL)}"/></HTTP></DCPType></GetFeature><Transaction><DCPType><HTTP><Post onlineResource="${xmlEscape(baseURL)}"/></HTTP></DCPType></Transaction></Request><ogc:Filter_Capabilities/></Capability></WFS_Capabilities>`;
  }
  return `<?xml version="1.0" encoding="UTF-8"?><wfs:WFS_Capabilities version="${version}" xmlns:wfs="${WFS_NAMESPACE}" xmlns:ows="${OWS_NAMESPACE}" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:fes="${FES_NAMESPACE}" xmlns:gml="${version === WFS_2_0_0 ? GML32_NAMESPACE : GML_NAMESPACE}"><ows:ServiceIdentification><ows:Title>Tile Server WFS</ows:Title><ows:ServiceType>WFS</ows:ServiceType><ows:ServiceTypeVersion>${version}</ows:ServiceType></ows:ServiceIdentification><ows:OperationsMetadata>${operationXML}</ows:OperationsMetadata><wfs:FeatureTypeList>${typeXML}</wfs:FeatureTypeList><fes:Filter_Capabilities><fes:Conformance><fes:Constraint name="ImplementsQuery"><ows:NoValues/></fes:Constraint></fes:Conformance><fes:Id_Capabilities><fes:ResourceId/></fes:Id_Capabilities><fes:Scalar_Capabilities><fes:ComparisonOperators><fes:ComparisonOperator name="PropertyIsEqualTo"/><fes:ComparisonOperator name="PropertyIsNotEqualTo"/><fes:ComparisonOperator name="PropertyIsLessThan"/><fes:ComparisonOperator name="PropertyIsGreaterThan"/></fes:ComparisonOperators></fes:Scalar_Capabilities><fes:Spatial_Capabilities><fes:SpatialOperator name="BBOX"/></fes:Spatial_Capabilities></fes:Filter_Capabilities></wfs:WFS_Capabilities>`;
}

function parseXMLRequest(xml) {
  const source = String(xml ?? "");
  const operation = source.match(/<\s*(?:[\w.-]+:)?([A-Za-z]+)(?:\s|>)/)?.[1];
  const rootAttrs = {};
  const root = source.match(/<\s*(?:[\w.-]+:)?[A-Za-z]+([^>]*)>/)?.[1] ?? "";
  for (const match of root.matchAll(/([\w:-]+)=["']([^"']*)["']/g)) {
    rootAttrs[localName(match[1])] = match[2];
  }
  const query =
    source.match(
      /<(?:[\w.-]+:)?Query\b[^>]*>([\s\S]*?)<\/(?:[\w.-]+:)?Query>/i,
    )?.[1] ?? source;
  const attributes = {
    ...rootAttrs,
  };
  for (const match of query.matchAll(
    /<(?:[\w.-]+:)?(PropertyName|ValueReference|Filter|BBOX|SortBy|SortProperty)\b[^>]*>([\s\S]*?)<\/(?:[\w.-]+:)?\1>/gi,
  )) {
    attributes[match[1]] = match[2];
  }
  const queryAttrs = query.match(/<(?:[\w.-]+:)?Query([^>]*)>/i)?.[1] ?? "";
  for (const match of queryAttrs.matchAll(/([\w:-]+)=["']([^"']*)["']/g)) {
    attributes[localName(match[1])] = match[2];
  }
  if (attributes.Filter) {
    attributes.FILTER = attributes.Filter;
  }
  return {
    operation,
    parameters: attributes,
    xml: source,
  };
}

function getRequestData(req) {
  if (typeof req.body === "string" && req.body.trim()) {
    return parseXMLRequest(req.body);
  }
  return {
    operation: getParameter(req.query, "REQUEST", "GetCapabilities"),
    parameters: {
      ...(req.body ?? {}),
      ...(req.query ?? {}),
    },
    xml: "",
  };
}

function applyQuery(features, type, parameters) {
  const filter =
    filterFromXML(getParameter(parameters, "FILTER")) ??
    filterFromCQL(getParameter(parameters, "CQL_FILTER")) ??
    (getParameter(
      parameters,
      "FEATUREID",
      getParameter(parameters, "RESOURCEID"),
    )
      ? {
          type: "id",
          value: getParameter(
            parameters,
            "FEATUREID",
            getParameter(parameters, "RESOURCEID"),
          ),
        }
      : undefined);
  const bbox = parseBBox(getParameter(parameters, "BBOX"));
  let result = features.filter((feature, index) => {
    if (!matchesFilter(feature, filter, type, index)) {
      return false;
    }
    if (!bbox) {
      return true;
    }
    const geometry = getGeometryBBox(feature.geometry);

    return Boolean(geometry && bboxIntersects(geometry, bbox));
  });
  const sort = getParameter(parameters, "SORTBY");
  if (sort) {
    const parts = String(sort).trim().split(/\s+/);
    const field = localName(parts[0]);
    const descending =
      String(parts[1] ?? "").toUpperCase() === "D" ||
      String(parts[1] ?? "").toUpperCase() === "DESC";
    result = result.sort((first, second) => {
      const direction = descending ? -1 : 1;
      return (
        (propertyValue(first, field) > propertyValue(second, field) ? 1 : -1) *
        direction
      );
    });
  }
  return result;
}

function outputFormat(parameters) {
  const format = String(
    getParameter(parameters, "OUTPUTFORMAT", "application/json"),
  ).toLowerCase();
  if (format.includes("json") || format === "geojson") {
    return "json";
  }
  if (format === "gml2") {
    return "gml2";
  }
  if (format.includes("gml") || format === "text/xml") {
    return "gml";
  }
  throw new WFSException(
    "InvalidParameterValue",
    `OUTPUTFORMAT "${format}" is not supported.`,
  );
}

function describeFeatureType(type, version) {
  const gml = version === WFS_2_0_0 ? GML32_NAMESPACE : GML_NAMESPACE;
  const fields = [...type.fields.entries()]
    .map(([name, fieldType]) => {
      return `<xsd:element name="${xmlEscape(xmlName(name))}" type="xsd:${fieldType}" minOccurs="0"/>`;
    })
    .join("");
  return `<?xml version="1.0" encoding="UTF-8"?><xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:gml="${gml}" xmlns:feature="${WFS_FEATURE_NAMESPACE}" targetNamespace="${WFS_FEATURE_NAMESPACE}" elementFormDefault="qualified"><xsd:element name="${xmlEscape(xmlName(type.layer))}" type="feature:${xmlName(type.layer)}Type" substitutionGroup="gml:AbstractFeature"/><xsd:complexType name="${xmlEscape(xmlName(type.layer))}Type"><xsd:complexContent><xsd:extension base="gml:AbstractFeatureType"><xsd:sequence>${fields}<xsd:element name="geometry" type="gml:GeometryPropertyType" minOccurs="0"/></xsd:sequence></xsd:extension></xsd:complexContent></xsd:complexType></xsd:schema>`;
}

function transactionFeature(xml, type) {
  const feature = {
    type: "Feature",
    properties: {},
    geometry: undefined,
  };
  const body = xml.replace(/^<[^>]+>|<\/[^>]+>$/g, "");
  const geometry = body.match(
    /<(?:[\w.-]+:)?(Point|LineString|Polygon|MultiPoint|MultiLineString|MultiPolygon)[^>]*>([\s\S]*?)<\/(?:[\w.-]+:)?\1>/i,
  );
  if (geometry) {
    const coordinates =
      tagValue(geometry[2], "pos") ?? tagValue(geometry[2], "posList");
    const values = coordinates?.trim().split(/\s+/).map(Number) ?? [];
    if (geometry[1].toLowerCase() === "point") {
      feature.geometry = {
        type: "Point",
        coordinates: values.slice(0, 2),
      };
    }
  }
  for (const match of body.matchAll(/<([\w.-]+)(?:\s[^>]*)?>([^<]*)<\/\1>/g)) {
    const name = localName(match[1]);
    if (
      ![
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "pos",
        "posList",
      ].includes(name)
    ) {
      feature.properties[name] = match[2];
    }
  }
  return feature;
}

async function saveFeatures(type, features) {
  if (type.item.sourceURL) {
    throw new WFSException(
      "OperationNotSupported",
      "Transactions are disabled for forwarded GeoJSON sources.",
    );
  }
  await storeGeoJSONFile(
    type.item.path,
    Buffer.from(
      JSON.stringify(
        {
          type: "FeatureCollection",
          features,
        },
        null,
        2,
      ),
    ),
  );
}

async function transaction(xml, version) {
  const inserted = [];
  const updated = [];
  const deleted = [];
  for (const match of String(xml).matchAll(
    /<(?:[\w.-]+:)?Insert[^>]*>([\s\S]*?)<\/(?:[\w.-]+:)?Insert>/gi,
  )) {
    const child = match[1].match(/<([\w.:-]+)(?:\s[^>]*)?>[\s\S]*<\/\1>/);
    if (!child) {
      continue;
    }
    const type = resolveFeatureType(localName(child[1]));
    const features = await readFeatures(type);
    const feature = transactionFeature(child[0], type);
    feature.id = `${type.group}.${type.layer}.${nanoid()}`;
    features.push(feature);
    await saveFeatures(type, features);
    inserted.push(feature.id);
  }
  for (const match of String(xml).matchAll(
    /<(?:[\w.-]+:)?Update([^>]*)>([\s\S]*?)<\/(?:[\w.-]+:)?Update>/gi,
  )) {
    const typeName = match[1].match(/typeName[s]?=["']([^"']+)["']/i)?.[1];
    const type = resolveFeatureType(typeName);
    const properties = [];
    for (const property of match[2].matchAll(
      /<(?:[\w.-]+:)?Property[^>]*>([\s\S]*?)<\/(?:[\w.-]+:)?Property>/gi,
    )) {
      const name =
        tagValue(property[1], "ValueReference") ??
        tagValue(property[1], "PropertyName");
      const value = tagValue(property[1], "Value");
      if (name) {
        properties.push({
          name: localName(name),
          value,
        });
      }
    }
    const filterBody = match[2].match(
      /<(?:[\w.-]+:)?Filter[^>]*>[\s\S]*?<\/(?:[\w.-]+:)?Filter>/i,
    )?.[0];
    const filter = filterFromXML(filterBody);
    const features = await readFeatures(type);
    let count = 0;
    features.forEach((feature, index) => {
      if (!matchesFilter(feature, filter, type, index)) {
        return;
      }
      for (const property of properties) {
        feature.properties = feature.properties ?? {};
        feature.properties[property.name] = property.value;
      }
      count++;
    });
    await saveFeatures(type, features);
    updated.push(count);
  }
  for (const match of String(xml).matchAll(
    /<(?:[\w.-]+:)?Delete([^>]*)>([\s\S]*?)<\/(?:[\w.-]+:)?Delete>/gi,
  )) {
    const typeName = match[1].match(/typeName[s]?=["']([^"']+)["']/i)?.[1];
    const type = resolveFeatureType(typeName);
    const filter = filterFromXML(match[2]);
    const features = await readFeatures(type);
    const keep = features.filter((feature, index) => {
      return !matchesFilter(feature, filter, type, index);
    });
    deleted.push(features.length - keep.length);
    await saveFeatures(type, keep);
  }
  return `<?xml version="1.0" encoding="UTF-8"?><wfs:TransactionResponse xmlns:wfs="${WFS_NAMESPACE}" version="${version}"><wfs:TransactionSummary><wfs:totalInserted>${inserted.length}</wfs:totalInserted><wfs:totalUpdated>${updated.reduce(
    (sum, item) => {
      return sum + item;
    },
    0,
  )}</wfs:totalUpdated><wfs:totalDeleted>${deleted.reduce((sum, item) => {
    return sum + item;
  }, 0)}</wfs:totalDeleted></wfs:TransactionSummary>${
    inserted.length
      ? `<wfs:InsertResults>${inserted
          .map((id) => {
            return `<wfs:Feature><fes:ResourceId xmlns:fes="${FES_NAMESPACE}" rid="${xmlEscape(id)}"/></wfs:Feature>`;
          })
          .join("")}</wfs:InsertResults>`
      : ""
  }</wfs:TransactionResponse>`;
}

async function handleWFS(req, res, pathName) {
  const request = getRequestData(req);
  const parameters = request.parameters;
  try {
    const version = normalizeVersion(
      getParameter(parameters, "VERSION", WFS_2_0_0),
    );
    if (
      String(getParameter(parameters, "SERVICE", "WFS")).toUpperCase() !== "WFS"
    ) {
      throw new WFSException("InvalidParameterValue", "SERVICE must be WFS.");
    }
    let operation = String(
      request.operation ??
        getParameter(parameters, "REQUEST", "GetCapabilities"),
    ).toLowerCase();
    const baseURL = `${getRequestHost(req)}/wfs`;

    if (operation === "getcapabilities") {
      res
        .type("text/xml")
        .status(200)
        .send(await capabilities(version, baseURL));
      return;
    }
    if (operation === "describefeaturetype") {
      const type = await descriptor(
        resolveFeatureTypes(parameters, pathName)[0],
      );
      res.type("text/xml").status(200).send(describeFeatureType(type, version));
      return;
    }
    if (operation === "liststoredqueries") {
      const values = [...storedQueries.entries()]
        .map(([id, item]) => {
          return `<wfs:StoredQuery><wfs:Id>${xmlEscape(id)}</wfs:Id><wfs:Title>${xmlEscape(item.title ?? id)}</wfs:Title></wfs:StoredQuery>`;
        })
        .join("");
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:ListStoredQueriesResponse xmlns:wfs="${WFS_NAMESPACE}">${values}</wfs:ListStoredQueriesResponse>`,
        );
      return;
    }
    if (operation === "describestoredqueries") {
      const requested = splitParameter(
        getParameter(parameters, "STOREDQUERY_ID"),
      );
      const selected = requested.length
        ? requested.filter((id) => {
            return storedQueries.has(id);
          })
        : [...storedQueries.keys()];
      const values = selected
        .map((id) => {
          const item = storedQueries.get(id);
          return `<wfs:StoredQueryDescription id="${xmlEscape(id)}"><wfs:Title>${xmlEscape(item.title ?? id)}</wfs:Title></wfs:StoredQueryDescription>`;
        })
        .join("");
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:DescribeStoredQueriesResponse xmlns:wfs="${WFS_NAMESPACE}">${values}</wfs:DescribeStoredQueriesResponse>`,
        );
      return;
    }
    if (operation === "dropstoredquery") {
      storedQueries.delete(getParameter(parameters, "STOREDQUERY_ID"));
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:DropStoredQueryResponse xmlns:wfs="${WFS_NAMESPACE}"/>`,
        );
      return;
    }
    if (operation === "createstoredquery") {
      const id =
        request.xml.match(/id=["']([^"']+)["']/i)?.[1] ??
        getParameter(parameters, "ID", nanoid());
      storedQueries.set(id, {
        title: request.xml.match(/<[^>]*Title[^>]*>([^<]+)/i)?.[1] ?? id,
        xml: request.xml,
      });
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:CreateStoredQueryResponse xmlns:wfs="${WFS_NAMESPACE}"><wfs:StoredQueryDefinition id="${xmlEscape(id)}"/></wfs:CreateStoredQueryResponse>`,
        );
      return;
    }
    if (operation === "transaction") {
      if (!request.xml) {
        throw new WFSException(
          "OperationNotSupported",
          "Transaction requires XML POST.",
        );
      }
      res
        .type("text/xml")
        .status(200)
        .send(await transaction(request.xml, version));
      return;
    }
    if (operation === "lockfeature" || operation === "getfeaturewithlock") {
      const types = resolveFeatureTypes(parameters, pathName);
      const lockId = `lock-${nanoid()}`;
      const ids = [];
      for (const type of types) {
        for (const [index, feature] of (await readFeatures(type)).entries()) {
          ids.push(featureId(feature, type, index));
        }
      }
      locks.set(lockId, {
        ids,
        expires:
          Date.now() + Number(getParameter(parameters, "EXPIRY", 300)) * 1000,
      });
      if (operation === "lockfeature") {
        res
          .type("text/xml")
          .status(200)
          .send(
            `<?xml version="1.0" encoding="UTF-8"?><wfs:LockFeatureResponse xmlns:wfs="${WFS_NAMESPACE}" lockId="${lockId}"><wfs:FeaturesLocked>${ids.length}</wfs:FeaturesLocked></wfs:LockFeatureResponse>`,
          );
        return;
      }
      parameters.LOCKID = lockId;
      operation = "getfeature";
    }
    if (operation === "getpropertyvalue") {
      const types = await Promise.all(
        resolveFeatureTypes(parameters, pathName).map(descriptor),
      );
      const property = getParameter(
        parameters,
        "VALUEREFERENCE",
        getParameter(parameters, "PROPERTYNAME"),
      );
      const values = types
        .flatMap((type) => {
          return type.features.map((feature) => {
            return propertyValue(feature, property);
          });
        })
        .filter((value) => {
          return value !== undefined;
        });
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:ValueCollection xmlns:wfs="${WFS_NAMESPACE}">${values
            .map((value) => {
              return `<wfs:member>${xmlEscape(value)}</wfs:member>`;
            })
            .join("")}</wfs:ValueCollection>`,
        );
      return;
    }
    if (operation === "getfeature") {
      const storedQueryId = getParameter(parameters, "STOREDQUERY_ID");
      if (storedQueryId) {
        const stored = storedQueries.get(storedQueryId);
        if (!stored) {
          throw new WFSException(
            "InvalidParameterValue",
            `Stored query "${storedQueryId}" does not exist.`,
          );
        }
        Object.assign(parameters, parseXMLRequest(stored.xml).parameters);
      }
      const types = await Promise.all(
        resolveFeatureTypes(parameters, pathName).map(descriptor),
      );
      const srsName = normalizeCRS(
        getParameter(parameters, "SRSNAME", "EPSG:4326"),
      );
      const all = types.flatMap((type) => {
        return applyQuery(type.features, type, parameters).map(
          (feature, index) => {
            return {
              feature,
              type,
              index,
            };
          },
        );
      });
      const matched = all.length;
      const startIndex = max(
        0,
        Number(getParameter(parameters, "STARTINDEX", 0)) || 0,
      );
      const count = min(
        MAX_FEATURES,
        max(
          0,
          Number(
            getParameter(
              parameters,
              "COUNT",
              getParameter(parameters, "MAXFEATURES", DEFAULT_COUNT),
            ),
          ) || DEFAULT_COUNT,
        ),
      );
      const selected =
        String(
          getParameter(parameters, "RESULTTYPE", "results"),
        ).toLowerCase() === "hits"
          ? []
          : all.slice(startIndex, startIndex + count);
      const propertyNames = splitParameter(
        getParameter(
          parameters,
          "PROPERTYNAME",
          getParameter(parameters, "PROPERTYNAMES"),
        ),
      );
      const output = outputFormat(parameters);
      const projected = selected.map(({ feature, type, index }) => {
        const result = projectFeature(feature, type, index, srsName);
        if (!propertyNames.length) {
          return {
            feature: result,
            type,
            index,
          };
        }
        return {
          feature: {
            ...result,
            properties: Object.fromEntries(
              propertyNames
                .map((name) => {
                  return [name, result.properties?.[name]];
                })
                .filter(([, value]) => {
                  return value !== undefined;
                }),
            ),
          },
          type,
          index,
        };
      });
      if (output === "json") {
        res
          .type("application/json")
          .status(200)
          .send(
            featureCollectionJSON(
              projected.map((item) => {
                return item.feature;
              }),
              matched,
              startIndex,
            ),
          );
      } else {
        res
          .type("text/xml")
          .status(200)
          .send(
            featureCollectionGML(
              projected,
              version,
              srsName,
              matched,
              parameters.LOCKID,
              output,
            ),
          );
      }
      return;
    }
    throw new WFSException(
      "OperationNotSupported",
      `Request "${operation}" is not supported.`,
    );
  } catch (error) {
    const code = error.code ?? "NoApplicableCode";
    const message =
      error instanceof WFSException
        ? error.message
        : "The WFS request could not be completed.";
    res
      .status(400)
      .type("text/xml")
      .send(
        `<?xml version="1.0" encoding="UTF-8"?><ows:ExceptionReport xmlns:ows="${OWS_NAMESPACE}" version="${xmlEscape(getParameter(parameters, "VERSION", WFS_2_0_0))}"><ows:Exception exceptionCode="${xmlEscape(code)}"><ows:ExceptionText>${xmlEscape(message)}</ows:ExceptionText></ows:Exception></ows:ExceptionReport>`,
      );
  }
}

export const serve_wfs = {
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: WFS
     *     description: OGC Web Feature Service endpoints
     * /wfs:
     *   get:
     *     tags: [WFS]
     *     summary: Execute a WFS request
     *     description: Supports GetCapabilities, DescribeFeatureType, GetFeature, GetPropertyValue, locking, stored queries, and transactions.
     *     parameters:
     *       - { in: query, name: REQUEST, required: true, schema: { type: string, example: GetFeature } }
     *       - { in: query, name: VERSION, schema: { type: string, enum: ['1.0.0', '1.1.0', '2.0.0'], default: '2.0.0' } }
     *       - { in: query, name: TYPENAMES, schema: { type: string }, description: Comma-separated feature type names. }
     *       - { in: query, name: OUTPUTFORMAT, schema: { type: string, example: application/json } }
     *       - { in: query, name: BBOX, schema: { type: string }, description: Four coordinates with optional CRS. }
     *       - { in: query, name: COUNT, schema: { type: integer, minimum: 0 } }
     *       - { in: query, name: STARTINDEX, schema: { type: integer, minimum: 0 } }
     *     responses:
     *       200:
     *         description: WFS XML or GeoJSON response.
     *         content:
     *           application/json: { schema: { type: object } }
     *           application/xml: { schema: { type: string } }
     *       400:
     *         description: OGC exception report.
     *   post:
     *     tags: [WFS]
     *     summary: Execute a WFS XML request
     *     requestBody:
     *       required: true
     *       content:
     *         application/xml:
     *           schema: { type: string }
     *     responses:
     *       200:
     *         description: WFS XML response.
     *         content:
     *           application/xml: { schema: { type: string } }
     *       400:
     *         description: OGC exception report.
     */
    const bodyParser = [
      express.urlencoded({
        extended: false,
      }),
      express.text({
        type: ["application/xml", "text/xml", "application/vnd.ogc.wfs_xml"],
        limit: "20mb",
      }),
    ];
    const register = (route) => {
      app.get(route, (req, res) => {
        return handleWFS(
          req,
          res,
          req.params.group && req.params.layer
            ? `${req.params.group}:${req.params.layer}`
            : undefined,
        );
      });
      app.post(route, bodyParser, (req, res) => {
        return handleWFS(
          req,
          res,
          req.params.group && req.params.layer
            ? `${req.params.group}:${req.params.layer}`
            : undefined,
        );
      });
    };
    register("/wfs");
    register("/geojsons/:group/:layer/wfs");
  },
};

export { WFSException, normalizeVersion as normalizeWFSVersion };
