/* Color assignment adapted from maplibre-gl-inspect's semantic palette. */
function assignInspectLayerColor(layerName, alpha = 1) {
  const name = String(layerName).toLowerCase();
  let hue = [0, 360];
  let saturation = [55, 80];
  let lightness = [45, 68];

  if (/water|ocean|lake|sea|river/.test(name)) hue = [178, 257];
  else if (/state|country|place/.test(name)) hue = [282, 334];
  else if (/road|highway|transport|streets/.test(name)) hue = [18, 46];
  else if (/contour|building|earth/.test(name)) {
    hue = [0, 0];
    saturation = [0, 0];
  } else if (/contour|landuse/.test(name)) hue = [46, 62];
  else if (/wood|forest|park|landcover|land|natural/.test(name)) hue = [62, 178];

  if (/building/.test(name)) lightness = [28, 48];
  if (/earth/.test(name)) lightness = [62, 82];

  const seed = [...name].reduce((value, character) => value + character.charCodeAt(0), 0);
  const random = (minimum, maximum, offset = 0) => {
    const value = Math.abs(Math.sin(seed + offset) * 10000) % 1;
    return minimum + value * (maximum - minimum);
  };
  const selectedHue = hue[0] === hue[1] ? hue[0] : random(hue[0], hue[1], 1);
  const selectedSaturation = random(saturation[0], saturation[1], 2);
  const selectedLightness = random(lightness[0], lightness[1], 3);
  return `hsla(${selectedHue.toFixed(1)}, ${selectedSaturation.toFixed(1)}%, ${selectedLightness.toFixed(1)}%, ${alpha})`;
}
