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
    const style = {
      version: 8,
      sources: {
        source: {
          type: "vector",
          url: `${baseURL}/datas/${id}.json`,
        },
      },
      layers: [],
    };

    const cacheCoverageLayers = {};

    if (jsonResponse.cacheCoverages) {
      for (const { zoom, bbox } of jsonResponse.cacheCoverages) {
        if (!cacheCoverageLayers[zoom]) {
          cacheCoverageLayers[zoom] = {
            type: "FeatureCollection",
            features: [],
          };
        }

        cacheCoverageLayers[zoom]["features"].push({
          type: "Feature",
          geometry: {
            type: "LineString",
            coordinates: [
              [bbox[0], bbox[1]],
              [bbox[2], bbox[1]],
              [bbox[2], bbox[3]],
              [bbox[0], bbox[3]],
              [bbox[0], bbox[1]],
            ],
          },
        });
      }

      for (const zoom in cacheCoverageLayers) {
        style["sources"][`cache_${zoom}`] = {
          type: "geojson",
          data: cacheCoverageLayers[zoom],
        };

        style["layers"].push({
          id: `cache_${zoom}_line`,
          type: "line",
          source: `cache_${zoom}`,
          paint: {
            "line-color": "rgba(255, 0, 0, 0.6)",
            "line-width": 1,
          },
        });
      }
    }

    const map = new maplibregl.Map({
      container: "map",
      hash: true,
      style: style,
    });

    addMapLibreGlobalControls(map);

    const inspect = new MaplibreInspect({
      showInspectMap: true,
      showInspectButton: false,
    });

    map.addControl(inspect);

    map.once("idle", () => {
      const layerList = document.getElementById("layerList");

      for (const source in inspect.sources) {
        if (source === "source") {
          inspect.sources[source].forEach((layerID) => {
            const item = document.createElement("div");

            const layerColor = inspect.assignLayerColor(layerID);

            const row = document.createElement("div");
            row.style.cssText =
              "display:flex;align-items:center;cursor:pointer;";

            const swatch = document.createElement("div");
            swatch.style.cssText =
              "width:15px;height:15px;display:inline-block;";
            swatch.style.background = layerColor;

            const label = document.createElement("span");
            label.style.cssText = `margin-left:5px;color:${layerColor};display:inline-block;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;`;
            label.textContent = layerID;

            row.append(swatch, label);
            item.appendChild(row);

            item.onclick = () => {
              const newVisibility =
                map.getLayoutProperty(
                  `${source}_${layerID}_polygon`,
                  "visibility",
                ) === "none"
                  ? "visible"
                  : "none";

              map.setLayoutProperty(
                `${source}_${layerID}_polygon`,
                "visibility",
                newVisibility,
              );
              map.setLayoutProperty(
                `${source}_${layerID}_line`,
                "visibility",
                newVisibility,
              );
              map.setLayoutProperty(
                `${source}_${layerID}_circle`,
                "visibility",
                newVisibility,
              );

              item.style.textDecoration =
                newVisibility === "visible" ? "none" : "line-through";
            };

            layerList.appendChild(item);
          });
        } else {
          inspect.assignLayerColor(source);
        }
      }

      if (jsonResponse.cacheCoverages) {
        let isCoverageVisible = true;
        let currentZoomLayer = undefined;

        function updateVisibleCoverages() {
          if (currentZoomLayer) {
            map.setLayoutProperty(currentZoomLayer, "visibility", "none");

            currentZoomLayer = undefined;
          }

          if (isCoverageVisible) {
            const currentZoom = Math.floor(map.getZoom());

            const layerId = `cache_${currentZoom}_line`;

            if (style["layers"].some((layer) => layer.id === layerId)) {
              map.setLayoutProperty(layerId, "visibility", "visible");

              currentZoomLayer = layerId;
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
    });

    let currentMarker;

    map.on("click", (event) => {
      if (currentMarker) {
        currentMarker.remove();
      }

      currentMarker = new maplibregl.Marker({
        draggable: true,
      })
        .setLngLat(event.lngLat)
        .addTo(map);

      alert(`Position: [${event.lngLat.lng}, ${event.lngLat.lat}]`);

      currentMarker.on("dragend", () => {
        const lngLat = currentMarker.getLngLat();

        alert(`Position: [${lngLat.lng}, ${lngLat.lat}]`);
      });
    });

    map.on("contextmenu", (event) => {
      if (currentMarker) {
        currentMarker.remove();

        currentMarker = undefined;
      }
    });
  })
  .catch(handleError);
