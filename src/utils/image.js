"use strict";

import { getBBoxFromTiles, lonLat4326ToXY3857 } from "./spatial.js";
import { createBase64 } from "./file.js";
import { mkdir } from "node:fs/promises";
import { jsPDF } from "jspdf";
import path from "node:path";
import sharp from "sharp";

// sharp.cache(false);
// sharp.timeout({
//   seconds: 300,
// });

/**
 * Convert a value with unit to pixels
 * @param {number} value Mumeric value
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} unit Unit of the value
 * @param {number} ppi Pixel per inch
 * @returns {number} Value in pixel
 */
export function toPixel(value, unit, ppi = 96) {
  switch (unit) {
    case "km": {
      return (value * ppi * 1000) / 0.0254;
    }

    case "hm": {
      return (value * ppi * 100) / 0.0254;
    }

    case "dam": {
      return (value * ppi * 10) / 0.0254;
    }

    case "m": {
      return (value * ppi) / 0.0254;
    }

    case "dm": {
      return (value * ppi) / 10 / 0.0254;
    }

    case "cm": {
      return (value * ppi) / 100 / 0.0254;
    }

    case "mm": {
      return (value * ppi) / 1000 / 0.0254;
    }

    default: {
      return (value * ppi) / 0.0254;
    }
  }
}

/**
 * Calculate resolution
 * @param {{ filePath: string|Buffer, bbox: [number, number, number, number], width: number, height: number }} input Input object
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} unit unit
 * @returns {Promise<[number, number]>} [X resolution (m/pixel), Y resolution (m/pixel)]
 */
export async function calculateResolution(input, unit) {
  // Convert bbox from EPSG:4326 to EPSG:3857
  const [minX, minY] = lonLat4326ToXY3857(input.bbox[0], input.bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(input.bbox[2], input.bbox[3]);
  let resolution;

  // Get image dimensions
  if (input.filePath) {
    const { width, height } = await sharp(input.filePath, {
      limitInputPixels: false,
    }).metadata();

    resolution = [(maxX - minX) / width, (maxY - minY) / height];
  } else {
    resolution = [(maxX - minX) / input.width, (maxY - minY) / input.height];
  }

  // Convert resolution to the specified unit
  switch (unit) {
    default: {
      return resolution;
    }

    case "km": {
      return [resolution[0] / 1000, resolution[1] / 1000];
    }

    case "hm": {
      return [resolution[0] / 100, resolution[1] / 100];
    }

    case "dam": {
      return [resolution[0] / 10, resolution[1] / 10];
    }

    case "m": {
      return resolution;
    }

    case "dm": {
      return [resolution[0] * 10, resolution[1] * 10];
    }

    case "cm": {
      return [resolution[0] * 100, resolution[1] * 100];
    }

    case "mm": {
      return [resolution[0] * 1000, resolution[1] * 1000];
    }
  }
}

/**
 * Get image metadata
 * @param {string|Buffer} filePath File path or buffer
 * @returns {Promise<sharp.Metadata>}
 */
export async function getImageMetadata(filePath) {
  return await sharp(filePath, {
    limitInputPixels: false,
  }).metadata();
}

/**
 * Convert image
 * @param {string|Buffer} filePath File path or buffer
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function convertImage(filePath, output) {
  return await createImageOutput(
    sharp(filePath, {
      limitInputPixels: false,
    }),
    output
  );
}

/**
 * Format degree
 * @param {number} deg Degree
 * @param {boolean} isLat Is lat?
 * @param {"D"|"DMS"|"DMSD"} format Format
 * @returns {string}
 */
function formatDegree(deg, isLat, format) {
  const rounded = Number(deg.toFixed(9));

  if (format === "DMS" || format === "DMSD") {
    const abs = Math.abs(rounded);
    let d = Math.floor(abs);
    let m = Math.floor((abs - d) * 60);
    let s = Math.round(((abs - d) * 60 - m) * 60);

    if (s === 60) {
      s = 0;
      m += 1;
    }

    if (m === 60) {
      m = 0;
      d += 1;
    }

    let direction = "";
    let sign = "";
    if (format === "DMSD") {
      if (isLat) {
        direction = rounded >= 0 ? "N" : "S";
      } else {
        direction = rounded >= 0 ? "E" : "W";
      }
    } else if (rounded < 0) {
      sign = "-";
    }

    const sFormat = s === 0 ? "" : `${s}"`;
    const mFormat = m === 0 ? "" : `${m}'`;

    return `${sign}${d}°${sFormat}${mFormat}${direction}`;
  } else {
    if (Number.isInteger(rounded)) {
      return Math.round(rounded).toString();
    }

    return rounded.toFixed(2);
  }
}

/**
 * Get SVG stroke dash array
 * @param {"solid"|"dashed"|"longDashed"|"dotted"|"dashedDot"} style Stroke style
 * @returns {string}
 */
function getSVGStrokeDashArray(style) {
  let strokeDashArray = "";

  switch (style) {
    case "solid": {
      strokeDashArray = "";

      break;
    }

    case "dashed": {
      strokeDashArray = `stroke-dasharray="5,5"`;

      break;
    }

    case "longDashed": {
      strokeDashArray = `stroke-dasharray="10,5"`;

      break;
    }

    case "dotted": {
      strokeDashArray = `stroke-dasharray="1,3"`;

      break;
    }

    case "dashedDot": {
      strokeDashArray = `stroke-dasharray="5,3,1,3"`;

      break;
    }
  }

  return strokeDashArray;
}

/**
 * Get SVG stroke dash array
 * @param {number} rotation Rotation
 * @param {boolean} y Is Y axis?
 * @returns {{ topTextAnchor: string, topDominantBaseline: string, bottomTextAnchor: string, bottomDominantBaseline: string, leftTextAnchor: string, leftDominantBaseline: string, rightTextAnchor: string, rightDominantBaseline: string }}
 */
function getSVGTextAlign(rotation, y) {
  if (y) {
    switch (rotation) {
      default: {
        return {
          leftTextAnchor: "end",
          leftDominantBaseline: "middle",
          rightTextAnchor: "start",
          rightDominantBaseline: "middle",
        };
      }

      case 90: {
        return {
          leftTextAnchor: "middle",
          leftDominantBaseline: "hanging",
          rightTextAnchor: "middle",
          rightDominantBaseline: "auto",
        };
      }

      case -90: {
        return {
          leftTextAnchor: "middle",
          leftDominantBaseline: "auto",
          rightTextAnchor: "middle",
          rightDominantBaseline: "hanging",
        };
      }
    }
  } else {
    switch (rotation) {
      default: {
        return {
          topTextAnchor: "middle",
          topDominantBaseline: "auto",
          bottomTextAnchor: "middle",
          bottomDominantBaseline: "hanging",
        };
      }

      case 90: {
        return {
          topTextAnchor: "end",
          topDominantBaseline: "middle",
          bottomTextAnchor: "start",
          bottomDominantBaseline: "middle",
        };
      }

      case -90: {
        return {
          topTextAnchor: "start",
          topDominantBaseline: "middle",
          bottomTextAnchor: "end",
          bottomDominantBaseline: "middle",
        };
      }
    }
  }
}

/**
 * Create SVG
 * @param {{ content: string, width: number, height: number }} svg SVG object
 * @param {boolean} isBuffer Is buffer?
 * @returns {string|Buffer} SVG string
 */
function createSVG(svg, isBuffer) {
  const svgString = `<svg width="${svg.width}" height="${svg.height}" xmlns="http://www.w3.org/2000/svg">${svg.content}</svg>`;

  return isBuffer ? Buffer.from(svgString) : svgString;
}

/**
 * Create image output
 * @param {sharp.Sharp} image Sharp object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} options Options
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function createImageOutput(image, options) {
  const format = options.format || "png";

  // Resize image
  if (options.width > 0 || options.height > 0) {
    image.resize(options.width, options.height);
  }

  // Create format
  switch (options.format) {
    case "gif": {
      image.gif({
        quality: 100,
      });

      break;
    }

    case "png": {
      image.png({
        compressionLevel: 9,
      });

      break;
    }

    case "jpg":
    case "jpeg": {
      image.jpeg({
        quality: 100,
      });

      break;
    }

    case "webp": {
      image.webp({
        quality: 100,
      });

      break;
    }
  }

  // Create gray scale
  if (options.grayscale) {
    image.grayscale();
  }

  // Write to output
  if (options.filePath) {
    await mkdir(path.dirname(options.filePath), {
      recursive: true,
    });

    return await image.toFile(options.filePath);
  } else if (options.base64) {
    return createBase64(await image.toBuffer(), format);
  } else {
    return await image.toBuffer();
  }
}

/**
 * Add frame to image
 * @param {{ filePath: string|Buffer, bbox: [number, number, number, number] }} input Input object
 * @param {{ content: string|Buffer, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif" }[]} overlays Array of overlay object
 * @param {object} frame Frame options object
 * @param {object} grid Grid options object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function addFrameToImage(input, overlays, frame, grid, output) {
  let image = sharp(input.filePath, {
    limitInputPixels: false,
  });

  const { width, height } = await image.metadata();
  const bbox = input.bbox;

  // Add overlays
  if (overlays?.length) {
    const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
    const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);

    // Pixel/meter resolution
    const xRes = width / (maxX - minX);
    const yRes = height / (maxY - minY);

    // Composite image
    const compositeImage = await image
      .composite(
        await Promise.all(
          overlays.map(async (overlay) => {
            const [overlayMinX, overlayMinY] = lonLat4326ToXY3857(
              overlay.bbox[0],
              overlay.bbox[1]
            );
            const [overlayMaxX, overlayMaxY] = lonLat4326ToXY3857(
              overlay.bbox[2],
              overlay.bbox[3]
            );

            return {
              limitInputPixels: false,
              input: await sharp(overlay.content, {
                limitInputPixels: false,
              })
                .resize(
                  Math.round((overlayMaxX - overlayMinX) * xRes),
                  Math.round((overlayMaxY - overlayMinY) * yRes)
                )
                .toBuffer(),
              left: Math.floor((overlayMinX - minX) * xRes),
              top: Math.floor((maxY - overlayMaxY) * yRes),
            };
          })
        )
      )
      .toBuffer();

    // Assign new image
    image = sharp(compositeImage, {
      limitInputPixels: false,
    });
  }

  // For store frame and grid
  const svg = {
    content: "",
    width: width,
    height: height,
  };

  let totalMargin = 0;
  const degPerPixelX = (bbox[2] - bbox[0]) / width;
  const degPerPixelY = (bbox[3] - bbox[1]) / height;

  // Process frame
  if (frame) {
    let {
      frameMargin,

      frameInnerColor = "rgba(0,0,0,1)",
      frameInnerWidth = 6,
      frameInnerStyle = "solid",

      frameOuterColor = "rgba(0,0,0,1)",
      frameOuterWidth = 6,
      frameOuterStyle = "solid",

      frameSpace = 60,

      tickLabelFormat = "DMSD",

      majorTickStep = 0.5,
      minorTickStep = 0.1,

      majorTickWidth = 6,
      minorTickWidth = 4,

      majorTickSize = 30,
      minorTickSize = 15,

      majorTickLabelSize = 15,
      minorTickLabelSize = 0,

      majorTickColor = "rgba(0,0,0,1)",
      minorTickColor = "rgba(0,0,0,1)",

      majorTickLabelColor = "rgba(0,0,0,1)",
      minorTickLabelColor = "rgba(0,0,0,1)",

      majorTickLabelFont = "sans-serif",
      minorTickLabelFont = "sans-serif",

      xTickLabelOffset = 5,
      yTickLabelOffset = 5,

      xTickMajorLabelRotation = 0,
      xTickMinorLabelRotation = 0,

      yTickMajorLabelRotation = 90,
      yTickMinorLabelRotation = 90,

      xTickEnd,
      yTickEnd,
    } = frame;

    if (frameMargin === undefined) {
      frameMargin = Math.ceil(frameOuterWidth / 2);
    }

    frameInnerStyle = getSVGStrokeDashArray(frameInnerStyle);
    frameOuterStyle = getSVGStrokeDashArray(frameOuterStyle);

    totalMargin = frameMargin + frameSpace;

    svg.width = totalMargin * 2 + width;
    svg.height = totalMargin * 2 + height;

    // Inner frame
    svg.content += `<rect x="${totalMargin}" y="${totalMargin}" width="${width}" height="${height}" fill="none" stroke="${frameInnerColor}" stroke-width="${frameInnerWidth}" ${frameInnerStyle}/>`;

    // Outer frame
    svg.content += `<rect x="${frameMargin}" y="${frameMargin}" width="${
      width + frameSpace * 2
    }" height="${
      height + frameSpace * 2
    }" fill="none" stroke="${frameOuterColor}" stroke-width="${frameOuterWidth}" ${frameOuterStyle}/>`;

    const xTickMajorLons = [];
    const yTickMajorLats = [];

    if (majorTickWidth > 0) {
      // X-axis major ticks & labels
      const xMajor = getSVGTextAlign(xTickMajorLabelRotation);

      let xTickMajorStart = Math.floor(bbox[0] / majorTickStep) * majorTickStep;
      if (xTickMajorStart < bbox[0]) {
        xTickMajorStart += majorTickStep;
      }

      for (
        let lon = xTickMajorStart;
        lon <= bbox[2] + 1e-9;
        lon += majorTickStep
      ) {
        xTickMajorLons.push(lon);

        const x = (lon - bbox[0]) / degPerPixelX;

        // Top tick
        svg.content += `<line x1="${totalMargin + x}" y1="${totalMargin}" x2="${
          totalMargin + x
        }" y2="${
          totalMargin - majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Bottom tick
        svg.content += `<line x1="${totalMargin + x}" y1="${
          totalMargin + height
        }" x2="${totalMargin + x}" y2="${
          totalMargin + height + majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        if (majorTickLabelSize > 0) {
          const label = formatDegree(
            bbox[0] + x * degPerPixelX,
            false,
            tickLabelFormat
          );

          // Top label
          svg.content += `<text x="${totalMargin + x}" y="${
            totalMargin - majorTickSize - xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.topTextAnchor
          }" dominant-baseline="${
            xMajor.topDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${totalMargin + x},${
            totalMargin - majorTickSize - xTickLabelOffset
          })">${label}</text>`;

          // Bottom label
          svg.content += `<text x="${totalMargin + x}" y="${
            totalMargin + height + majorTickSize + xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.bottomTextAnchor
          }" dominant-baseline="${
            xMajor.bottomDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${totalMargin + x},${
            totalMargin + height + majorTickSize + xTickLabelOffset
          })">${label}</text>`;
        }
      }

      if (xTickEnd) {
        const xStart = (bbox[0] - bbox[0]) / degPerPixelX;
        const xEnd = (bbox[2] - bbox[0]) / degPerPixelX;

        // Top start tick end
        svg.content += `<line x1="${
          totalMargin + xStart
        }" y1="${totalMargin}" x2="${totalMargin + xStart}" y2="${
          totalMargin - majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Bottom start tick end
        svg.content += `<line x1="${totalMargin + xStart}" y1="${
          totalMargin + height
        }" x2="${totalMargin + xStart}" y2="${
          totalMargin + height + majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Top end tick end
        svg.content += `<line x1="${
          totalMargin + xEnd
        }" y1="${totalMargin}" x2="${totalMargin + xEnd}" y2="${
          totalMargin - majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Bottom end tick end
        svg.content += `<line x1="${totalMargin + xEnd}" y1="${
          totalMargin + height
        }" x2="${totalMargin + xEnd}" y2="${
          totalMargin + height + majorTickSize
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        if (majorTickLabelSize > 0) {
          let label = formatDegree(
            bbox[0] + xStart * degPerPixelX,
            false,
            tickLabelFormat
          );

          // Top start label end
          svg.content += `<text x="${totalMargin + xStart}" y="${
            totalMargin - majorTickSize - xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.topTextAnchor
          }" dominant-baseline="${
            xMajor.topDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${
            totalMargin + xStart
          },${
            totalMargin - majorTickSize - xTickLabelOffset
          })">${label}</text>`;

          // Bottom start label end
          svg.content += `<text x="${totalMargin + xStart}" y="${
            totalMargin + height + majorTickSize + xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.bottomTextAnchor
          }" dominant-baseline="${
            xMajor.bottomDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${
            totalMargin + xStart
          },${
            totalMargin + height + majorTickSize + xTickLabelOffset
          })">${label}</text>`;

          label = formatDegree(
            bbox[0] + xEnd * degPerPixelX,
            false,
            tickLabelFormat
          );

          // Top end label end
          svg.content += `<text x="${totalMargin + xEnd}" y="${
            totalMargin - majorTickSize - xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.topTextAnchor
          }" dominant-baseline="${
            xMajor.topDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${
            totalMargin + xEnd
          },${
            totalMargin - majorTickSize - xTickLabelOffset
          })">${label}</text>`;

          // Bottom end label end
          svg.content += `<text x="${totalMargin + xEnd}" y="${
            totalMargin + height + majorTickSize + xTickLabelOffset
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            xMajor.bottomTextAnchor
          }" dominant-baseline="${
            xMajor.bottomDominantBaseline
          }" transform="rotate(${xTickMajorLabelRotation},${
            totalMargin + xEnd
          },${
            totalMargin + height + majorTickSize + xTickLabelOffset
          })">${label}</text>`;
        }
      }

      // Y-axis major ticks & labels
      const yMajor = getSVGTextAlign(yTickMajorLabelRotation, true);

      let yTickMajorStart = Math.floor(bbox[1] / majorTickStep) * majorTickStep;
      if (yTickMajorStart < bbox[1]) {
        yTickMajorStart += majorTickStep;
      }

      for (
        let lat = yTickMajorStart;
        lat <= bbox[3] + 1e-9;
        lat += majorTickStep
      ) {
        yTickMajorLats.push(lat);

        const y = (bbox[3] - lat) / degPerPixelY;

        // Left tick
        svg.content += `<line x1="${totalMargin}" y1="${totalMargin + y}" x2="${
          totalMargin - majorTickSize
        }" y2="${
          totalMargin + y
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Right tick
        svg.content += `<line x1="${totalMargin + width}" y1="${
          totalMargin + y
        }" x2="${totalMargin + width + majorTickSize}" y2="${
          totalMargin + y
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        if (majorTickLabelSize > 0) {
          const label = formatDegree(
            bbox[3] - y * degPerPixelY,
            true,
            tickLabelFormat
          );

          // Left label
          svg.content += `<text x="${
            totalMargin - majorTickSize - yTickLabelOffset
          }" y="${
            totalMargin + y
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.leftTextAnchor
          }" dominant-baseline="${
            yMajor.leftDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin - majorTickSize - yTickLabelOffset
          },${totalMargin + y})">${label}</text>`;

          // Right label
          svg.content += `<text x="${
            totalMargin + width + majorTickSize + yTickLabelOffset
          }" y="${
            totalMargin + y
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.rightTextAnchor
          }" dominant-baseline="${
            yMajor.rightDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin + width + majorTickSize + yTickLabelOffset
          },${totalMargin + y})">${label}</text>`;
        }
      }

      if (yTickEnd) {
        const yStart = (bbox[3] - bbox[3]) / degPerPixelY;
        const yEnd = (bbox[3] - bbox[1]) / degPerPixelY;

        // Left start tick end
        svg.content += `<line x1="${totalMargin}" y1="${
          totalMargin + yStart
        }" x2="${totalMargin - majorTickSize}" y2="${
          totalMargin + yStart
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Right start tick end
        svg.content += `<line x1="${totalMargin + width}" y1="${
          totalMargin + yStart
        }" x2="${totalMargin + width + majorTickSize}" y2="${
          totalMargin + yStart
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Left end tick end
        svg.content += `<line x1="${totalMargin}" y1="${
          totalMargin + yEnd
        }" x2="${totalMargin - majorTickSize}" y2="${
          totalMargin + yEnd
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        // Right end tick end
        svg.content += `<line x1="${totalMargin + width}" y1="${
          totalMargin + yEnd
        }" x2="${totalMargin + width + majorTickSize}" y2="${
          totalMargin + yEnd
        }" stroke="${majorTickColor}" stroke-width="${majorTickWidth}" />`;

        if (majorTickLabelSize > 0) {
          let label = formatDegree(
            bbox[3] - yStart * degPerPixelY,
            true,
            tickLabelFormat
          );

          // Left start label end
          svg.content += `<text x="${
            totalMargin - majorTickSize - yTickLabelOffset
          }" y="${
            totalMargin + yStart
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.leftTextAnchor
          }" dominant-baseline="${
            yMajor.leftDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin - majorTickSize - yTickLabelOffset
          },${totalMargin + yStart})">${label}</text>`;

          // Right start label end
          svg.content += `<text x="${
            totalMargin + width + majorTickSize + yTickLabelOffset
          }" y="${
            totalMargin + yStart
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.rightTextAnchor
          }" dominant-baseline="${
            yMajor.rightDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin + width + majorTickSize + yTickLabelOffset
          },${totalMargin + yStart})">${label}</text>`;

          label = formatDegree(
            bbox[3] - yEnd * degPerPixelY,
            true,
            tickLabelFormat
          );

          // Left end label end
          svg.content += `<text x="${
            totalMargin - majorTickSize - yTickLabelOffset
          }" y="${
            totalMargin + yEnd
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.leftTextAnchor
          }" dominant-baseline="${
            yMajor.leftDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin - majorTickSize - yTickLabelOffset
          },${totalMargin + yEnd})">${label}</text>`;

          // Right end label end
          svg.content += `<text x="${
            totalMargin + width + majorTickSize + yTickLabelOffset
          }" y="${
            totalMargin + yEnd
          }" font-size="${majorTickLabelSize}" font-family="${majorTickLabelFont}" fill="${majorTickLabelColor}" text-anchor="${
            yMajor.rightTextAnchor
          }" dominant-baseline="${
            yMajor.rightDominantBaseline
          }" transform="rotate(${yTickMajorLabelRotation},${
            totalMargin + width + majorTickSize + yTickLabelOffset
          },${totalMargin + yEnd})">${label}</text>`;
        }
      }
    }

    if (minorTickWidth > 0) {
      // X-axis minor ticks & labels
      const xMinor = getSVGTextAlign(xTickMinorLabelRotation);

      let xTickMinorStart = Math.floor(bbox[0] / minorTickStep) * minorTickStep;
      if (xTickMinorStart < bbox[0]) {
        xTickMinorStart += minorTickStep;
      }
      for (
        let lon = xTickMinorStart;
        lon <= bbox[2] + 1e-9;
        lon += minorTickStep
      ) {
        if (xTickMajorLons.includes(lon)) {
          continue;
        }

        const x = (lon - bbox[0]) / degPerPixelX;

        // Top tick
        svg.content += `<line x1="${totalMargin + x}" y1="${totalMargin}" x2="${
          totalMargin + x
        }" y2="${
          totalMargin - minorTickSize
        }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

        // Bottom tick
        svg.content += `<line x1="${totalMargin + x}" y1="${
          totalMargin + height
        }" x2="${totalMargin + x}" y2="${
          totalMargin + height + minorTickSize
        }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

        if (minorTickLabelSize > 0) {
          const label = formatDegree(
            bbox[0] + x * degPerPixelX,
            false,
            tickLabelFormat
          );

          // Top label
          svg.content += `<text x="${totalMargin + x}" y="${
            totalMargin - minorTickSize - xTickLabelOffset
          }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="${
            xMinor.topTextAnchor
          }" dominant-baseline="${
            xMinor.topDominantBaseline
          }" transform="rotate(${xTickMinorLabelRotation},${totalMargin + x},${
            totalMargin - minorTickSize - xTickLabelOffset
          })">${label}</text>`;

          // Bottom label
          svg.content += `<text x="${totalMargin + x}" y="${
            totalMargin + height + minorTickSize + xTickLabelOffset
          }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="${
            xMinor.bottomTextAnchor
          }" dominant-baseline="${
            xMinor.bottomDominantBaseline
          }" transform="rotate(${xTickMinorLabelRotation},${totalMargin + x},${
            totalMargin + height + minorTickSize + xTickLabelOffset
          })">${label}</text>`;
        }
      }

      // Y-axis minor ticks & labels
      const yMinor = getSVGTextAlign(yTickMinorLabelRotation, true);

      let yTickMinorStart = Math.floor(bbox[1] / minorTickStep) * minorTickStep;
      if (yTickMinorStart < bbox[1]) {
        yTickMinorStart += minorTickStep;
      }
      for (
        let lat = yTickMinorStart;
        lat <= bbox[3] + 1e-9;
        lat += minorTickStep
      ) {
        if (yTickMajorLats.includes(lat)) {
          continue;
        }

        const y = (bbox[3] - lat) / degPerPixelY;

        // Left tick
        svg.content += `<line x1="${totalMargin}" y1="${totalMargin + y}" x2="${
          totalMargin - minorTickSize
        }" y2="${
          totalMargin + y
        }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

        // Right tick
        svg.content += `<line x1="${totalMargin + width}" y1="${
          totalMargin + y
        }" x2="${totalMargin + width + minorTickSize}" y2="${
          totalMargin + y
        }" stroke="${minorTickColor}" stroke-width="${minorTickWidth}" />`;

        if (minorTickLabelSize > 0) {
          const label = formatDegree(
            bbox[3] - y * degPerPixelY,
            true,
            tickLabelFormat
          );

          // Left label
          svg.content += `<text x="${
            totalMargin - minorTickSize - yTickLabelOffset
          }" y="${
            totalMargin + y
          }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="${
            yMinor.leftTextAnchor
          }" dominant-baseline="${
            yMinor.leftDominantBaseline
          }" transform="rotate(${yTickMinorLabelRotation},${
            totalMargin - minorTickSize - yTickLabelOffset
          },${totalMargin + y})">${label}</text>`;

          // Right label
          svg.content += `<text x="${
            totalMargin + width + minorTickSize + yTickLabelOffset
          }" y="${
            totalMargin + y
          }" font-size="${minorTickLabelSize}" font-family="${minorTickLabelFont}" fill="${minorTickLabelColor}" text-anchor="${
            yMinor.rightTextAnchor
          }" dominant-baseline="${
            yMinor.rightDominantBaseline
          }" transform="rotate(${yTickMinorLabelRotation},${
            totalMargin + width + minorTickSize + yTickLabelOffset
          },${totalMargin + y})">${label}</text>`;
        }
      }
    }

    // Extend image
    const extendImage = await image
      .extend({
        top: totalMargin,
        left: totalMargin,
        bottom: totalMargin,
        right: totalMargin,
        background: { r: 255, g: 255, b: 255, alpha: 0 },
      })
      .toBuffer();

    // Assign new image
    image = sharp(extendImage, {
      limitInputPixels: false,
    });
  }

  // Process grid
  if (grid) {
    let {
      majorGridStyle = "longDashed",
      majorGridWidth = 6,
      majorGridStep = 0.5,
      majorGridColor = "rgba(0,0,0,0.3)",

      minorGridStyle = "longDashed",
      minorGridWidth = 0,
      minorGridStep = 0.1,
      minorGridColor = "rgba(0,0,0,0.3)",
    } = grid;

    majorGridStyle = getSVGStrokeDashArray(majorGridStyle);
    minorGridStyle = getSVGStrokeDashArray(minorGridStyle);

    const xGridMajorLons = [];
    const yGridMajorLats = [];

    if (majorGridWidth > 0) {
      // X-axis major grids
      let xGridMajorStart = Math.floor(bbox[0] / majorGridStep) * majorGridStep;
      if (xGridMajorStart < bbox[0]) {
        xGridMajorStart += majorGridStep;
      }
      for (
        let lon = xGridMajorStart;
        lon <= bbox[2] + 1e-9;
        lon += majorGridStep
      ) {
        xGridMajorLons.push(lon);

        const x = totalMargin + (lon - bbox[0]) / degPerPixelX;

        svg.content += `<line x1="${x}" y1="${totalMargin}" x2="${x}" y2="${
          totalMargin + height
        }" stroke="${majorGridColor}" stroke-width="${majorGridWidth}" ${majorGridStyle}/>`;
      }

      // Y-axis major grids
      let yGridMajorStart = Math.floor(bbox[1] / majorGridStep) * majorGridStep;
      if (yGridMajorStart < bbox[1]) {
        yGridMajorStart += majorGridStep;
      }
      for (
        let lat = yGridMajorStart;
        lat <= bbox[3] + 1e-9;
        lat += majorGridStep
      ) {
        yGridMajorLats.push(lat);

        const y = totalMargin + (bbox[3] - lat) / degPerPixelY;

        svg.content += `<line x1="${totalMargin}" y1="${y}" x2="${
          totalMargin + width
        }" y2="${y}" stroke="${majorGridColor}" stroke-width="${majorGridWidth}" ${majorGridStyle}/>`;
      }
    }

    if (minorGridWidth > 0) {
      // X-axis minor grids
      let xGridMinorStart = Math.floor(bbox[0] / minorGridStep) * minorGridStep;
      if (xGridMinorStart < bbox[0]) {
        xGridMinorStart += minorGridStep;
      }
      for (
        let lon = xGridMinorStart;
        lon <= bbox[2] + 1e-9;
        lon += minorGridStep
      ) {
        if (xGridMajorLons.includes(lon)) {
          continue;
        }

        const x = totalMargin + (lon - bbox[0]) / degPerPixelX;

        svg.content += `<line x1="${x}" y1="${totalMargin}" x2="${x}" y2="${
          totalMargin + height
        }" stroke="${minorGridColor}" stroke-width="${minorGridWidth}" ${minorGridStyle}/>`;
      }

      // Y-axis minor grids
      let yGridMinorStart = Math.floor(bbox[1] / minorGridStep) * minorGridStep;
      if (yGridMinorStart < bbox[1]) {
        yGridMinorStart += minorGridStep;
      }
      for (
        let lat = yGridMinorStart;
        lat <= bbox[3] + 1e-9;
        lat += minorGridStep
      ) {
        if (yGridMajorLats.includes(lat)) {
          continue;
        }

        const y = totalMargin + (bbox[3] - lat) / degPerPixelY;

        svg.content += `<line x1="${totalMargin}" y1="${y}" x2="${
          totalMargin + width
        }" y2="${y}" stroke="${minorGridColor}" stroke-width="${minorGridWidth}" ${minorGridStyle}/>`;
      }
    }
  }

  // Add frame and grid (Composite image)
  if (svg.content) {
    // Composite image
    const compositeImage = await image
      .composite([
        {
          limitInputPixels: false,
          input: createSVG(svg, true),
          left: 0,
          top: 0,
        },
      ])
      .toBuffer();

    // Assign new image
    image = sharp(compositeImage, {
      limitInputPixels: false,
    });
  }

  // Return image
  return await createImageOutput(image, output);
}

/**
 * Merge tiles to image
 * @param {{ dirPath: string, z: number, xMin: number, xMax: Number, yMin: number, yMax: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", scheme: "xyz" | "tms", tileSize: number, bbox: [number, number, number, number] }} input Input object
 * @param {{ bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<sharp.OutputInfo|Buffer|string>}
 */
export async function mergeTilesToImage(input, output) {
  // Detect tile size
  const { width, height } = await sharp(
    `${input.dirPath}/${input.z}/${input.xMin}/${input.yMin}.${input.format}`,
    {
      limitInputPixels: false,
    }
  ).metadata();

  // Calculate target width, height
  const targetWidth = (input.xMax - input.xMin + 1) * width;
  const targetHeight = (input.yMax - input.yMin + 1) * height;

  // Process composites
  const composites = [];

  for (let x = input.xMin; x <= input.xMax; x++) {
    for (let y = input.yMin; y <= input.yMax; y++) {
      composites.push({
        input: `${input.dirPath}/${input.z}/${x}/${y}.${input.format}`,
        top: (y - input.yMin) * height,
        left: (x - input.xMin) * width,
      });
    }
  }

  // Composite image
  const compositeImage = await sharp({
    limitInputPixels: false,
    create: {
      width: targetWidth,
      height: targetHeight,
      channels: 4,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    },
  })
    .composite(composites)
    .png()
    .toBuffer();

  // Create image
  const image = sharp(compositeImage, {
    limitInputPixels: false,
  });

  // Extract image
  if (output.bbox) {
    const originBBox = input.bbox
      ? input.bbox
      : getBBoxFromTiles(
          input.xMin,
          input.yMin,
          input.xMax,
          input.yMax,
          input.z,
          input.scheme,
          input.tileSize
        );

    const xRes = targetWidth / (originBBox[2] - originBBox[0]);
    const yRes = targetHeight / (originBBox[3] - originBBox[1]);

    image.extract({
      left: Math.floor((output.bbox[0] - originBBox[0]) * xRes),
      top: Math.floor((originBBox[3] - output.bbox[3]) * yRes),
      width: Math.ceil((output.bbox[2] - output.bbox[0]) * xRes),
      height: Math.ceil((output.bbox[3] - output.bbox[1]) * yRes),
    });
  }

  // Return image
  return await createImageOutput(image, output);
}

/**
 * Split image to PDF
 * @param {{ image: string|Buffer, res: [number, number] }} input Input object
 * @param {object} preview Preview options object
 * @param {{ filePath: string, paperSize: [number, number], orientation: "portrait"|"landscape", base64: boolean, alignContent: { horizontal: "left"|"center"|"right", vertical: "top"|"middle"|"bottom" }, compression: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<jsPDF|sharp.OutputInfo|Buffer|string>}
 */
export async function splitImage(input, preview, output) {
  // Get image size
  const { width, height } = await sharp(input.image, {
    limitInputPixels: false,
  }).metadata();

  // Get paper size (in mm)
  const paperHeight =
    output.orientation === "landscape"
      ? output.paperSize[0]
      : output.paperSize[1];
  const paperWidth =
    output.orientation === "landscape"
      ? output.paperSize[1]
      : output.paperSize[0];

  let paperHeightPx;
  let paperWidthPx;
  let heightPageNum;
  let widthPageNum;

  // Convert paper size to pixel and Calculate number of page
  if (input.resolution) {
    paperHeightPx = Math.round(paperHeight / input.resolution[1]);
    paperWidthPx = Math.round(paperWidth / input.resolution[0]);

    heightPageNum = Math.ceil(height / (paperHeight / input.resolution[1]));
    widthPageNum = Math.ceil(width / (paperWidth / input.resolution[0]));
  } else {
    paperHeightPx = Math.round(toPixel(paperWidth, "mm"));
    paperWidthPx = Math.round(toPixel(paperHeight, "mm"));

    heightPageNum = Math.ceil(height / toPixel(paperWidth, "mm"));
    widthPageNum = Math.ceil(width / toPixel(paperHeight, "mm"));
  }

  const newHeight = heightPageNum * paperHeightPx;
  const newWidth = widthPageNum * paperWidthPx;

  let extendTop = 0;
  let extendLeft = 0;

  if (output.alignContent) {
    switch (output.alignContent.horizontal) {
      case "left": {
        extendTop = 0;

        break;
      }

      case "center": {
        extendTop = Math.floor((newHeight - height) / 2);

        break;
      }

      case "right": {
        extendTop = Math.floor(newHeight - height);

        break;
      }
    }

    switch (output.alignContent.vertical) {
      case "top": {
        extendLeft = 0;

        break;
      }

      case "middle": {
        extendLeft = Math.floor((newWidth - width) / 2);

        break;
      }

      case "bottom": {
        extendLeft = Math.floor(newWidth - width);

        break;
      }
    }
  }

  const extendBottom = Math.ceil(newHeight - height - extendTop);
  const extendRight = Math.ceil(newWidth - width - extendLeft);

  // Extend image
  const extendImage = await sharp(input.image, {
    limitInputPixels: false,
  })
    .extend({
      top: extendTop,
      left: extendLeft,
      bottom: extendBottom,
      right: extendRight,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    })
    .toBuffer();

  // Process preview
  if (preview) {
    let {
      format = "png",
      lineColor = "rgba(255,0,0,1)",
      lineWidth = 6,
      lineStyle = "solid",
      pageColor = "rgba(255,0,0,1)",
      pageSize = 100,
      pageFont = "sans-serif",
      width,
      height,
    } = preview;

    lineStyle = getSVGStrokeDashArray(lineStyle);

    const svg = {
      content: "",
      width: newWidth,
      height: newHeight,
    };

    for (let y = 0; y < heightPageNum; y++) {
      for (let x = 0; x < widthPageNum; x++) {
        if (lineWidth > 0) {
          svg.content += `<rect x="${x * paperWidthPx}" y="${
            y * paperHeightPx
          }" width="${paperWidthPx}" height="${paperHeightPx}" fill="none" stroke="${lineColor}" stroke-width="${lineWidth}" ${lineStyle}/>`;
        }

        if (pageSize > 0) {
          svg.content += `<text x="${x * paperWidthPx + paperWidthPx / 2}" y="${
            y * paperHeightPx + paperHeightPx / 2
          }" text-anchor="middle" alignment-baseline="middle" font-family="${pageFont}" font-size="${pageSize}" fill="${pageColor}">${
            y + x + 1
          }</text>`;
        }
      }
    }

    // Composite image
    const compositeImage = await sharp(extendImage, {
      limitInputPixels: false,
    })
      .composite([
        {
          limitInputPixels: false,
          input: createSVG(svg, true),
          left: 0,
          top: 0,
        },
      ])
      .toBuffer();

    // Return image
    return await createImageOutput(
      sharp(compositeImage, {
        limitInputPixels: false,
      }),
      {
        width: width,
        height: height,
        base64: output.base64,
        filePath: output.filePath,
        format: format,
        grayscale: output.grayscale,
      }
    );
  } else {
    const doc = new jsPDF({
      orientation: output.orientation,
      unit: "mm",
      format: output.paperSize,
      compress: output.compression,
    });

    for (let y = 0; y < heightPageNum; y++) {
      for (let x = 0; x < widthPageNum; x++) {
        const image = await createImageOutput(
          sharp(extendImage, {
            limitInputPixels: false,
          }).extract({
            left: x * paperWidthPx,
            top: y * paperHeightPx,
            width: paperWidthPx,
            height: paperHeightPx,
          }),
          {
            format: "png",
            grayscale: output.grayscale,
          }
        );

        if (x > 0 || y > 0) {
          doc.addPage();
        }

        let pageWidth = paperWidthPx * input.resolution[0];
        if (pageWidth > paperWidth) {
          pageWidth = paperWidth;
        }

        let pageHeight = paperHeightPx * input.resolution[1];
        if (pageHeight > paperHeight) {
          pageHeight = paperHeight;
        }

        doc.addImage(image, "png", 0, 0, pageWidth, pageHeight);
      }
    }

    // Write to output
    if (output.filePath) {
      await mkdir(path.dirname(output.filePath), {
        recursive: true,
      });

      return doc.save(output.filePath);
    } else if (output.base64) {
      return createBase64(Buffer.from(doc.output("arraybuffer")), "pdf");
    } else {
      return doc.output("arraybuffer");
    }
  }
}

/**
 * Check if PNG image file/buffer is full transparent (alpha = 0)
 * @param {Buffer} buffer Buffer of the PNG image
 * @returns {Promise<boolean>}
 */
export async function isFullTransparentPNGImage(buffer) {
  try {
    if (
      buffer[0] !== 0x89 ||
      buffer[1] !== 0x50 ||
      buffer[2] !== 0x4e ||
      buffer[3] !== 0x47 ||
      buffer[4] !== 0x0d ||
      buffer[5] !== 0x0a ||
      buffer[6] !== 0x1a ||
      buffer[7] !== 0x0a
    ) {
      return false;
    }

    const { data, info } = await sharp(buffer, {
      limitInputPixels: false,
    })
      .raw()
      .toBuffer({
        resolveWithObject: true,
      });

    if (info.channels !== 4) {
      return false;
    }

    for (let i = 3; i < data.length; i += 4) {
      if (data[i] !== 0) {
        return false;
      }
    }

    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Process image tile raw data
 * @param {Buffer} data Image data buffer
 * @param {number} originSize Image origin size
 * @param {number} targetSize Image origin size
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @param {string} filePath File path
 * @returns {Promise<sharp.OutputInfo|Buffer>}
 */
export async function processImageTileRawData(
  data,
  originSize,
  targetSize,
  format,
  filePath
) {
  return await createImageOutput(
    sharp(data, {
      limitInputPixels: false,
      raw: {
        premultiplied: true,
        width: originSize,
        height: originSize,
        channels: 4,
      },
    }),
    {
      format: format,
      width: targetSize,
      height: targetSize,
      filePath: filePath,
    }
  );
}

/**
 * Process image static raw data
 * @param {Buffer} data Image data buffer
 * @param {number} originWidth Image origin width size
 * @param {number} originHeight Image origin height size
 * @param {number} targetWidth Image origin width size
 * @param {number} targetHeight Image origin height size
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @param {string} filePath File path
 * @returns {Promise<sharp.OutputInfo|Buffer>}
 */
export async function processImageStaticRawData(
  data,
  originWidth,
  originHeight,
  targetWidth,
  targetHeight,
  format,
  filePath
) {
  return await createImageOutput(
    sharp(data, {
      limitInputPixels: false,
      raw: {
        premultiplied: true,
        width: originWidth,
        height: originHeight,
        channels: 4,
      },
    }),
    {
      format: format,
      width: targetWidth,
      height: targetHeight,
      filePath: filePath,
    }
  );
}
