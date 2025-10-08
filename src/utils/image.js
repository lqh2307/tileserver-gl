"use strict";

import { getBBoxFromTiles, lonLat4326ToXY3857 } from "./spatial.js";
import { createBase64, createFileWithLock } from "./file.js";
import { convertLength, toPixel } from "./util.js";
import { jsPDF } from "jspdf";
import sharp from "sharp";

// sharp.cache(false);
// sharp.timeout({
//   seconds: 300,
// });

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
 * Get tick SVG text align
 * @param {number} rotation Rotation
 * @param {boolean} y Is Y axis?
 * @returns {{ topTextAnchor: string, topDominantBaseline: string, bottomTextAnchor: string, bottomDominantBaseline: string, leftTextAnchor: string, leftDominantBaseline: string, rightTextAnchor: string, rightDominantBaseline: string }}
 */
function getTickSVGTextAlign(rotation, y) {
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
 * Calculate resolution
 * @param {{ image: string|Buffer, bbox: [number, number, number, number], width: number, height: number }} input Input object
 * @param {"km"|"hm"|"dam"|"m"|"dm"|"cm"|"mm"} unit unit
 * @returns {Promise<[number, number]>} [X resolution (m/pixel), Y resolution (m/pixel)]
 */
export async function calculateResolution(input, unit) {
  // Convert bbox from EPSG:4326 to EPSG:3857
  const [minX, minY] = lonLat4326ToXY3857(input.bbox[0], input.bbox[1]);
  const [maxX, maxY] = lonLat4326ToXY3857(input.bbox[2], input.bbox[3]);
  let resolution;

  // Get origin image size
  if (input.image) {
    const { width, height } = await getImageMetadata(input.image);

    resolution = [(maxX - minX) / width, (maxY - minY) / height];
  } else {
    resolution = [(maxX - minX) / input.width, (maxY - minY) / input.height];
  }

  // Convert resolution to the specified unit
  return [
    convertLength(resolution[0], "m", unit),
    convertLength(resolution[1], "m", unit),
  ];
}

/**
 * Get image metadata
 * @param {string|Buffer} filePath File path or buffer image
 * @returns {Promise<sharp.Metadata>}
 */
export async function getImageMetadata(filePath) {
  return await sharp(filePath, {
    limitInputPixels: false,
  }).metadata();
}

/**
 * Create image output (Order: Input -> Extend -> Composites -> Extract -> Resize -> Output !== Default sharp order: Input -> Resize/Extract/... -> Extend -> Composites -> Output)
 * @param {sharp.Sharp|string|Buffer} image Image
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean, createOption: sharp.Create, rawOption: sharp.CreateRaw, resizeOption: sharp.ResizeOptions, compositesOption: sharp.OverlayOptions[], extendOption: sharp.ExtendOptions, extractOption: sharp.Region }} options Options
 * @returns {Promise<void|Buffer|string>}
 */
export async function createImageOutput(image, options) {
  let targetImage;

  if (
    (options.extendOption || options.compositesOption) &&
    (options.extractOption ||
      options.width ||
      options.height ||
      options.grayscale)
  ) {
    // Read input
    targetImage = options.createOption
      ? sharp({
          limitInputPixels: false,
          create: options.createOption,
        }).png()
      : sharp(image, {
          limitInputPixels: false,
          raw: options.rawOption,
        });

    // Extend
    if (options.extendOption) {
      targetImage.extend(options.extendOption);
    }

    // Composites
    if (options.compositesOption) {
      targetImage.composite(options.compositesOption);
    }

    targetImage = sharp(await targetImage.toBuffer(), {
      limitInputPixels: false,
    });
  } else {
    // Read input
    targetImage = options.createOption
      ? sharp({
          limitInputPixels: false,
          create: options.createOption,
        }).png()
      : sharp(image, {
          limitInputPixels: false,
          raw: options.rawOption,
        });

    // Extend
    if (options.extendOption) {
      targetImage.extend(options.extendOption);
    }

    // Composites
    if (options.compositesOption) {
      targetImage.composite(options.compositesOption);
    }
  }

  // Extract
  if (options.extractOption) {
    targetImage.extract(options.extractOption);
  }

  // Resize
  if (options.width || options.height) {
    targetImage.resize(options.width, options.height, options.resizeOption);
  }

  // Grayscale
  if (options.grayscale) {
    targetImage.grayscale(true);
  }

  // Format
  switch (options.format) {
    case "gif": {
      targetImage.gif({
        quality: 100,
      });

      break;
    }

    case "png": {
      targetImage.png({
        compressionLevel: 9,
      });

      break;
    }

    case "jpg":
    case "jpeg": {
      targetImage.jpeg({
        quality: 100,
      });

      break;
    }

    case "webp": {
      targetImage.webp({
        quality: 100,
      });

      break;
    }
  }

  // Buffer
  const buffer = await targetImage.toBuffer();

  // Write to output
  if (options.filePath) {
    await createFileWithLock(
      options.filePath,
      buffer,
      300000 // 5 mins
    );
  } else if (options.base64) {
    return createBase64(buffer, options.format || "png");
  } else {
    return buffer;
  }
}

/**
 * Add frame to image
 * @param {{ image: string|Buffer, bbox: [number, number, number, number] }} input Input object
 * @param {{ image: string, bbox: [number, number, number, number] }[]} overlays Array of overlay object
 * @param {{ frameMargin: number, frameInnerColor: string, frameInnerWidth: number, frameInnerStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", frameOuterColor: string, frameOuterWidth: number, frameOuterStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", frameSpace: number, tickLabelFormat: "D"|"DMS"|"DMSD", majorTickStep: number, minorTickStep: number, majorTickWidth: number, minorTickWidth: number, majorTickSize: number, minorTickSize: number, majorTickLabelSize: number, minorTickLabelSize: number, majorTickColor: string, minorTickColor: string, majorTickLabelColor: string, minorTickLabelColor: string, majorTickLabelFont: string, minorTickLabelFont: string, xTickLabelOffset: number, yTickLabelOffset: number, xTickEnd: boolean, xTickMajorLabelRotation: number, xTickMinorLabelRotation: number, yTickMajorLabelRotation: number, yTickEnd: boolean, yTickMinorLabelRotation: number }} frame Frame object
 * @param {{ majorGridStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", majorGridWidth: number, majorGridStep: number, majorGridColor: string, minorGridStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", minorGridWidth: number, minorGridStep: number, minorGridColor: string }} grid Grid object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<void|Buffer|string>}
 */
export async function addFrameToImage(input, overlays, frame, grid, output) {
  // Get origin image size
  const { width, height } = await getImageMetadata(input.image);
  const bbox = input.bbox;

  let image;

  // Add overlays
  if (overlays?.length) {
    const [minX, minY] = lonLat4326ToXY3857(bbox[0], bbox[1]);
    const [maxX, maxY] = lonLat4326ToXY3857(bbox[2], bbox[3]);

    // Pixel/meter resolution
    const xRes = width / (maxX - minX);
    const yRes = height / (maxY - minY);

    // Create overlays composites option
    const compositesOption = await Promise.all(
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
          input: await createImageOutput(base64ToBuffer(overlay.image), {
            width: Math.round((overlayMaxX - overlayMinX) * xRes),
            height: Math.round((overlayMaxY - overlayMinY) * yRes),
          }),
          left: Math.floor((overlayMinX - minX) * xRes),
          top: Math.floor((maxY - overlayMaxY) * yRes),
        };
      })
    );

    // Create composited image
    image = await createImageOutput(input.image, {
      compositesOption: compositesOption,
    });
  }

  // SVG to store frame and grid
  const svg = {
    content: "",
    width: width,
    height: height,
  };

  let totalMargin = 0;
  const degPerPixelX = (bbox[2] - bbox[0]) / width;
  const degPerPixelY = (bbox[3] - bbox[1]) / height;

  let extendOption;

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

    // Assign SVG size
    svg.width = totalMargin * 2 + width;
    svg.height = totalMargin * 2 + height;

    // Assign SVG inner frame
    svg.content += `<rect x="${totalMargin}" y="${totalMargin}" width="${width}" height="${height}" fill="none" stroke="${frameInnerColor}" stroke-width="${frameInnerWidth}" ${frameInnerStyle}/>`;

    // Asign SVG outer frame
    svg.content += `<rect x="${frameMargin}" y="${frameMargin}" width="${
      width + frameSpace * 2
    }" height="${
      height + frameSpace * 2
    }" fill="none" stroke="${frameOuterColor}" stroke-width="${frameOuterWidth}" ${frameOuterStyle}/>`;

    // Asign SVG ticks and labels
    const xTickMajorLons = [];
    const yTickMajorLats = [];

    if (majorTickWidth > 0) {
      // X-axis major ticks & labels
      const xMajor = getTickSVGTextAlign(xTickMajorLabelRotation);

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
      const yMajor = getTickSVGTextAlign(yTickMajorLabelRotation, true);

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
      const xMinor = getTickSVGTextAlign(xTickMinorLabelRotation);

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
      const yMinor = getTickSVGTextAlign(yTickMinorLabelRotation, true);

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

    // Create extend option
    extendOption = {
      top: totalMargin,
      left: totalMargin,
      bottom: totalMargin,
      right: totalMargin,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    };
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

    // Assign SVG grids
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

  // Create SVG composites option
  const compositesOption = svg.content
    ? [
        {
          limitInputPixels: false,
          input: createSVG(svg, true),
          left: 0,
          top: 0,
        },
      ]
    : undefined;

  // Create image
  return await createImageOutput(image ?? input.image, {
    extendOption: extendOption,
    compositesOption: compositesOption,
    ...output,
  });
}

/**
 * Merge tiles to image
 * @param {{ dirPath: string, z: number, xMin: number, xMax: Number, yMin: number, yMax: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", scheme: "xyz" | "tms", tileSize: 256|512, bbox: [number, number, number, number] }} input Input object
 * @param {{ bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<void|Buffer|string>}
 */
export async function mergeTilesToImage(input, output) {
  // Detect origin tile size
  const { width, height } = await getImageMetadata(
    `${input.dirPath}/${input.z}/${input.xMin}/${input.yMin}.${input.format}`
  );

  // Calculate target width, height
  const targetWidth = (input.xMax - input.xMin + 1) * width;
  const targetHeight = (input.yMax - input.yMin + 1) * height;

  // Process composites
  const compositesOption = [];

  for (let x = input.xMin; x <= input.xMax; x++) {
    for (let y = input.yMin; y <= input.yMax; y++) {
      compositesOption.push({
        limitInputPixels: false,
        input: `${input.dirPath}/${input.z}/${x}/${y}.${input.format}`,
        top: (y - input.yMin) * height,
        left: (x - input.xMin) * width,
      });
    }
  }

  let extractOption;

  // Process extract
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

    extractOption = {
      left: Math.floor((output.bbox[0] - originBBox[0]) * xRes),
      top: Math.floor((originBBox[3] - output.bbox[3]) * yRes),
      width: Math.ceil((output.bbox[2] - output.bbox[0]) * xRes),
      height: Math.ceil((output.bbox[3] - output.bbox[1]) * yRes),
    };
  }

  // Return image
  return await createImageOutput(undefined, {
    createOption: {
      width: targetWidth,
      height: targetHeight,
      channels: 4,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    },
    compositesOption: compositesOption,
    extractOption: extractOption,
    ...output,
  });
}

/**
 * Split image to tiles
 * @param {{ image: string|Buffer, bbox: [number, number, number, number] }} input Input object
 * @param {{ dirPath: string, tileSize: 256|512, format: "jpeg"|"jpg"|"png"|"webp"|"gif", filePath: string, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<void|Buffer|string>}
 */
export async function splitImageToTiles(input, output) {
  await sharp(input.image)
    .tile({
      size: output.tileSize,
      layout: "dz",
      depth: "one",
    })
    .toFile("a.dzi");
}

/**
 * Render image to PDF or Preview image with high quality
 * @param {{ images: { image: string|Buffer, res: [number, number] }[], res: [number, number] }} input Input object
 * @param {{ format?: "png" | "jpg" | "jpeg" | "gif" | "webp", width: number, height: number, lineColor: string, lineWidth: number, lineStyle: "dashed" | "dotted" | "solid" | "longDashed" | "dashedDot", pageColor: string, pageSize: number, pageFont: string }} preview Preview object
 * @param {{ filePath: string, paperSize: [number, number], orientation: "portrait"|"landscape", base64: boolean }} output Output object
 * @returns {Promise<void|Buffer|string>}
 */
export async function renderImageToHighQualityPDF(input, preview, output) {
  // Get paper size (in mm)
  const [paperWidth, paperHeight] =
    output.orientation === "landscape"
      ? [output.paperSize[1], output.paperSize[0]]
      : output.paperSize;

  // Convert paper size to pixel
  let paperWidthPX;
  let paperHeightPX;

  if (input.resolution) {
    paperWidthPX = Math.round(paperWidth / input.resolution[0]);
    paperHeightPX = Math.round(paperHeight / input.resolution[1]);
  } else {
    paperWidthPX = Math.round(toPixel(paperWidth, "mm"));
    paperHeightPX = Math.round(toPixel(paperHeight, "mm"));
  }

  // Init images
  const images = preview
    ? []
    : new jsPDF({
        orientation: output.orientation,
        unit: "mm",
        format: output.paperSize,
        compress: true,
      });

  for (const image of input.images) {
    // Get origin image size
    const { width, height } = await getImageMetadata(image.image);

    // Calculate number of page
    const widthPageNum = Math.ceil(width / paperWidthPX);
    const heightPageNum = Math.ceil(height / paperHeightPX);

    // Asign new image size
    const newWidthPX = widthPageNum * paperWidthPX;
    const newHeightPX = heightPageNum * paperHeightPX;

    // Process horizontal align
    let extendLeft;

    switch (output.alignContent?.horizontal) {
      default: {
        extendLeft = Math.floor((newWidthPX - width) / 2);

        break;
      }

      case "left": {
        extendLeft = 0;

        break;
      }

      case "right": {
        extendLeft = Math.floor(newWidthPX - width);

        break;
      }
    }

    // Process vertical align
    let extendTop;

    switch (output.alignContent?.vertical) {
      default: {
        extendTop = Math.floor((newHeightPX - height) / 2);

        break;
      }

      case "top": {
        extendTop = 0;

        break;
      }

      case "bottom": {
        extendTop = Math.floor(newHeightPX - height);

        break;
      }
    }

    // Create extend option
    const extendOption = {
      left: extendLeft,
      top: extendTop,
      right: Math.ceil(newWidthPX - width - extendLeft),
      bottom: Math.ceil(newHeightPX - height - extendTop),
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    };

    // Process Preview or PDF
    if (preview) {
      let {
        format = "png",
        lineWidth = 6,
        pageSize = 100,
        width,
        height,
      } = preview;

      // Process Preview
      let compositesOption;

      if (lineWidth > 0 || pageSize > 0) {
        let {
          lineColor = "rgba(255,0,0,1)",
          lineStyle = "solid",
          pageColor = "rgba(255,0,0,1)",
          pageFont = "sans-serif",
        } = preview;

        lineStyle = getSVGStrokeDashArray(lineStyle);

        const svg = {
          content: "",
          width: newWidthPX,
          height: newHeightPX,
        };

        for (let y = 0; y < heightPageNum; y++) {
          for (let x = 0; x < widthPageNum; x++) {
            if (lineWidth > 0) {
              svg.content += `<rect x="${x * paperWidthPX}" y="${
                y * paperHeightPX
              }" width="${paperWidthPX}" height="${paperHeightPX}" fill="none" stroke="${lineColor}" stroke-width="${lineWidth}" ${lineStyle}/>`;
            }

            if (pageSize > 0) {
              svg.content += `<text x="${x * paperWidthPX + paperWidthPX / 2}" y="${
                y * paperHeightPX + paperHeightPX / 2
              }" text-anchor="middle" alignment-baseline="middle" font-family="${pageFont}" font-size="${pageSize}" fill="${pageColor}">${
                y + x + 1
              }</text>`;
            }
          }
        }

        // Create SVG composites option
        compositesOption = [
          {
            limitInputPixels: false,
            input: createSVG(svg, true),
            left: 0,
            top: 0,
          },
        ];
      }

      // Create image
      const previewImage = await createImageOutput(image.image, {
        extendOption: extendOption,
        compositesOption: compositesOption,
        format: format,
        height: height,
        width: width,
        grayscale: output.grayscale,
        filePath: output.filePath,
        base64: output.base64,
      });

      // Add image to array
      images.push(previewImage);
    } else {
      // Create extended image
      const baseImage = await createImageOutput(image.image, {
        extendOption: extendOption,
      });

      // Process PDF
      for (let y = 0; y < heightPageNum; y++) {
        for (let x = 0; x < widthPageNum; x++) {
          // Add new page
          if (x > 0 || y > 0) {
            images.addPage();
          }

          // Create image
          const pdfImage = await createImageOutput(baseImage, {
            extractOption: {
              left: x * paperWidthPX,
              top: y * paperHeightPX,
              width: paperWidthPX,
              height: paperHeightPX,
            },
            format: "png",
            grayscale: output.grayscale,
          });

          // Add image to page
          images.addImage(pdfImage, "png", 0, 0, paperWidth, paperHeight);
        }
      }
    }
  }

  if (preview) {
    return images;
  } else {
    // Create array buffer
    const arrayBuffer = images.output("arraybuffer");

    // Write to output
    if (output.filePath) {
      await createFileWithLock(
        output.filePath,
        arrayBuffer,
        300000 // 5 mins
      );
    } else if (output.base64) {
      return createBase64(Buffer.from(arrayBuffer), "pdf");
    } else {
      return arrayBuffer;
    }
  }
}

/**
 * Render image to PDF or Preview image
 * @param {{ images: string[]|Buffer[] }} input Input object
 * @param {{ format?: "png" | "jpg" | "jpeg" | "gif" | "webp", width: number, height: number, lineColor: string, lineWidth: number, lineStyle: "dashed" | "dotted" | "solid" | "longDashed" | "dashedDot" }} preview Preview object
 * @param {{ filePath: string, paperSize: [number, number], orientation: "portrait"|"landscape", base64: boolean, fit: "auto"|"cover"|"contain"|"fill", alignContent: { horizontal: "left"|"center"|"right", vertical: "top"|"middle"|"bottom" }, pagination: { horizontal: "left"|"center"|"right", vertical: "top"|"middle"|"bottom" }, grayscale: boolean, grid: { row: number, column: number, marginX: number, marginY: number, gapX: number, gapY: number } }} output Output object
 * @returns {Promise<void|Buffer[]|string[]>}
 */
export async function renderImageToPDF(input, preview, output) {
  // Get paper size (in mm)
  const [paperWidth, paperHeight] =
    output.orientation === "landscape"
      ? [output.paperSize[1], output.paperSize[0]]
      : output.paperSize;

  // Convert paper size to pixel
  const paperWidthPX = Math.round(toPixel(paperWidth, "mm"));
  const paperHeightPX = Math.round(toPixel(paperHeight, "mm"));

  // Get grid info
  let {
    row = 1,
    column = 1,
    marginX = 0,
    marginY = 0,
    gapX = 0,
    gapY = 0,
  } = output.grid ?? {};

  // Convert padding and gap to pixel
  const marginXPX = Math.round(toPixel(marginX, "mm"));
  const marginYPX = Math.round(toPixel(marginY, "mm"));
  const gapXPX = Math.round(toPixel(gapX, "mm"));
  const gapYPX = Math.round(toPixel(gapY, "mm"));

  const cellWidth = Math.floor(
    (paperWidthPX - marginXPX * 2 - (column - 1) * gapXPX) / column
  );
  const cellHeight = Math.floor(
    (paperHeightPX - marginYPX * 2 - (row - 1) * gapYPX) / row
  );

  // Process align
  let position;

  if (output.alignContent) {
    const horizontalAlign = output.alignContent.horizontal;
    const verticalAlign = output.alignContent.vertical;

    if (horizontalAlign === "left" && verticalAlign === "top") {
      position = "left top";
    } else if (horizontalAlign === "center" && verticalAlign === "top") {
      position = "top";
    } else if (horizontalAlign === "right" && verticalAlign === "top") {
      position = "right top";
    } else if (horizontalAlign === "left" && verticalAlign === "middle") {
      position = "left";
    } else if (horizontalAlign === "right" && verticalAlign === "middle") {
      position = "right";
    } else if (horizontalAlign === "left" && verticalAlign === "bottom") {
      position = "left bottom";
    } else if (horizontalAlign === "center" && verticalAlign === "bottom") {
      position = "bottom";
    } else if (horizontalAlign === "right" && verticalAlign === "bottom") {
      position = "right bottom";
    } else {
      position = "center";
    }
  }

  // Calculate number of page
  const imageInPageNum = row * column;
  const pageNum = Math.ceil(input.images.length / imageInPageNum);

  // Process Preview or PDF
  if (preview) {
    let { format = "png", lineWidth = 6, width, height } = preview;

    // Create cell frame SVG
    let svg;

    if (lineWidth > 0) {
      let { lineColor = "rgba(255,0,0,1)", lineStyle = "solid" } = preview;

      svg = createSVG(
        {
          content: `<rect x="0" y="0" width="${cellWidth}" height="${cellHeight}" fill="none" stroke="${lineColor}" stroke-width="${lineWidth}" ${getSVGStrokeDashArray(lineStyle)}/>`,
          width: cellWidth,
          height: cellHeight,
        },
        true
      );
    }

    // Create Preview
    const images = [];

    // Process Preview
    for (let page = 0; page < pageNum; page++) {
      const items = input.images.slice(
        page * imageInPageNum,
        (page + 1) * imageInPageNum
      );

      // Create composites option
      const compositesOption = await Promise.all(
        items.map(async (item, idx) => ({
          limitInputPixels: false,
          input: await createImageOutput(item, {
            resizeOption: {
              fit: output.fit,
              position: position,
              background: { r: 255, g: 255, b: 255, alpha: 0 },
            },
            width: cellWidth,
            height: cellHeight,
          }),
          left: marginXPX + (idx % column) * (cellWidth + gapXPX),
          top: marginYPX + Math.floor(idx / column) * (cellHeight + gapYPX),
        }))
      );

      if (svg) {
        items.forEach((_, idx) =>
          compositesOption.push({
            limitInputPixels: false,
            input: svg,
            left: marginXPX + (idx % column) * (cellWidth + gapXPX),
            top: marginYPX + Math.floor(idx / column) * (cellHeight + gapYPX),
          })
        );
      }

      // Process pagination
      if (output.pagination) {
        const horizontalPagination = output.pagination.horizontal;
        let x;

        if (horizontalPagination === "left") {
          x = 36;
        } else if (horizontalPagination === "center") {
          x = paperWidthPX / 2;
        } else {
          x = paperWidthPX - 36;
        }

        const verticalPagination = output.pagination.vertical;
        let y;

        if (verticalPagination === "top") {
          y = 36;
        } else if (verticalPagination === "middle") {
          y = paperHeightPX / 2;
        } else {
          y = paperHeightPX - 36;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: createSVG(
            {
              content: `<text x="${x}" y="${y}" font-size="12" font-family="sans-serif" fill="#000000" text-anchor="middle" dominant-baseline="middle">${page + 1}</text>`,
              width: paperWidthPX,
              height: paperHeightPX,
            },
            true
          ),
          left: 0,
          top: 0,
        });
      }

      // Create image
      const previewImage = await createImageOutput(undefined, {
        createOption: {
          width: paperWidthPX,
          height: paperHeightPX,
          channels: 4,
          background: { r: 255, g: 255, b: 255, alpha: 0 },
        },
        compositesOption: compositesOption,
        format: format,
        width: width,
        height: height,
        base64: output.base64,
        grayscale: output.grayscale,
      });

      // Add image to array
      images.push(previewImage);
    }

    return images;
  } else {
    // Create PDF
    const images = new jsPDF({
      orientation: output.orientation,
      unit: "mm",
      format: output.paperSize,
      compress: true,
    });

    // Process PDF
    for (let page = 0; page < pageNum; page++) {
      // Add new page
      if (page > 0) {
        images.addPage();
      }

      // Create composites option
      const compositesOption = await Promise.all(
        input.images
          .slice(page * imageInPageNum, (page + 1) * imageInPageNum)
          .map(async (item, idx) => ({
            limitInputPixels: false,
            input: await createImageOutput(item, {
              resizeOption: {
                fit: output.fit,
                position: position,
                background: { r: 255, g: 255, b: 255, alpha: 0 },
              },
              width: cellWidth,
              height: cellHeight,
            }),
            left: marginXPX + (idx % column) * (cellWidth + gapXPX),
            top: marginYPX + Math.floor(idx / column) * (cellHeight + gapYPX),
          }))
      );

      // Process pagination
      if (output.pagination) {
        const horizontalPagination = output.pagination.horizontal;
        let x;

        if (horizontalPagination === "left") {
          x = 36;
        } else if (horizontalPagination === "center") {
          x = paperWidthPX / 2;
        } else {
          x = paperWidthPX - 36;
        }

        const verticalPagination = output.pagination.vertical;
        let y;

        if (verticalPagination === "top") {
          y = 36;
        } else if (verticalPagination === "middle") {
          y = paperHeightPX / 2;
        } else {
          y = paperHeightPX - 36;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: createSVG(
            {
              content: `<text x="${x}" y="${y}" font-size="12" font-family="sans-serif" fill="#000000" text-anchor="middle" dominant-baseline="middle">${page + 1}</text>`,
              width: paperWidthPX,
              height: paperHeightPX,
            },
            true
          ),
          left: 0,
          top: 0,
        });
      }

      // Create image
      const pdfImage = await createImageOutput(undefined, {
        createOption: {
          width: paperWidthPX,
          height: paperHeightPX,
          channels: 4,
          background: { r: 255, g: 255, b: 255, alpha: 0 },
        },
        compositesOption: compositesOption,
        format: "png",
        grayscale: output.grayscale,
      });

      // Add image to page
      images.addImage(pdfImage, "png", 0, 0, paperWidth, paperHeight);
    }

    // Create array buffer
    const arrayBuffer = images.output("arraybuffer");

    // Write to output
    if (output.filePath) {
      await createFileWithLock(
        output.filePath,
        arrayBuffer,
        300000 // 5 mins
      );
    } else if (output.base64) {
      return createBase64(Buffer.from(arrayBuffer), "pdf");
    } else {
      return arrayBuffer;
    }
  }
}

/**
 * Check if image file/buffer is full transparent (alpha = 0)
 * @param {Buffer} buffer Buffer of the PNG image
 * @returns {Promise<boolean>}
 */
export async function isFullTransparentImage(buffer) {
  try {
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
 * Convert base64 string to buffer
 * @param {string} base64
 * @returns {Buffer}
 */
export function base64ToBuffer(base64) {
  return Buffer.from(base64.slice(base64.indexOf(",") + 1), "base64");
}
