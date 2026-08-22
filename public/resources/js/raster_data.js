const baseURL = document.body.dataset.baseUrl;
const id = document.body.dataset.id;

function handleError(error) {
  if (error.response) {
    alert(
      `Error: Status code: ${error.response.status} - ${error.response.statusText}`,
    );
  } else if (error.request) {
    alert(`No response received: ${error.message}`);
  } else {
    alert(`Error: ${error.message}`);
  }
}

fetch(`${baseURL}/datas/${id}.json`, {
  method: "GET",
  headers: {
    "content-type": "application/json",
  },
})
  .then((response) => response.json())
  .then((jsonResponse) => {
    const map = L.map("map", {
      zoomControl: false,
      maxBounds: [
        [-90, -180],
        [90, 180],
      ],
      maxBoundsViscosity: 1,
    });

    for (const tile of jsonResponse.tiles || []) {
      L.tileLayer(tile, {
        minZoom: jsonResponse.minzoom,
        maxZoom: jsonResponse.maxzoom,
        maxNativeZoom: 22,
        attribution: jsonResponse.attribution,
      }).addTo(map);
    }

    if (jsonResponse.cacheCoverages) {
      const cacheCoverageLayers = {};
      const cacheCoverageGeoJSONLayers = {};

      for (const { zoom, bbox } of jsonResponse.cacheCoverages) {
        if (!cacheCoverageLayers[zoom]) {
          cacheCoverageLayers[zoom] = {
            type: "MultiLineString",
            coordinates: [],
          };
        }

        cacheCoverageLayers[zoom].coordinates.push([
          [bbox[0], bbox[1]],
          [bbox[2], bbox[1]],
          [bbox[2], bbox[3]],
          [bbox[0], bbox[3]],
          [bbox[0], bbox[1]],
        ]);
      }

      let isCoverageVisible = true;
      let currentCoverageLayer = undefined;

      function updateVisibleCoverages() {
        if (currentCoverageLayer) {
          currentCoverageLayer.remove();

          currentCoverageLayer = undefined;
        }

        if (isCoverageVisible) {
          const currentZoom = map.getZoom();

          if (cacheCoverageLayers[currentZoom]) {
            currentCoverageLayer = L.geoJSON(cacheCoverageLayers[currentZoom], {
              weight: 1,
              color: "rgba(255, 0, 0, 0.6)",
            });

            currentCoverageLayer.addTo(map);
          }
        }
      }

      map.on("zoomend", updateVisibleCoverages);

      const cacheCoveragesButton = document.createElement("div");
      cacheCoveragesButton.innerHTML = "Hide cache Coverages";
      cacheCoveragesButton.style.cssText =
        "position:absolute;bottom:50px;left:10px;padding:5px 10px;font-size:14px;cursor:pointer;background:rgba(0, 150, 255, 0.5);color:white;border:none;border-radius:5px;display:inline-block;z-index:1000;";

      cacheCoveragesButton.onclick = () => {
        if (isCoverageVisible) {
          cacheCoveragesButton.innerHTML = "Show cache Coverages";

          isCoverageVisible = false;
        } else {
          cacheCoveragesButton.innerHTML = "Hide cache Coverages";

          isCoverageVisible = true;
        }

        updateVisibleCoverages();
      };

      document.body.appendChild(cacheCoveragesButton);

      updateVisibleCoverages();
    }

    addLeafletGlobalControls(map);

    new L.Hash(map);

    let currentMarker;

    map.on("click", (event) => {
      if (currentMarker) {
        currentMarker.remove();
      }

      currentMarker = L.marker([event.latlng.lat, event.latlng.lng], {
        draggable: true,
      }).addTo(map);

      alert(`Position: [${event.latlng.lng}, ${event.latlng.lat}]`);

      currentMarker.on("dragend", () => {
        const position = currentMarker.getLatLng();

        alert(`Position: [${position.lng}, ${position.lat}]`);
      });
    });

    map.on("contextmenu", () => {
      if (currentMarker) {
        currentMarker.remove();

        currentMarker = undefined;
      }
    });
  })
  .catch(handleError);
