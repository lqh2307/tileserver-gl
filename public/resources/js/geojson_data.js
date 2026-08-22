const baseURL = document.body.dataset.baseUrl;
const group = document.body.dataset.group;
const layer = document.body.dataset.layer;

function handleError(error) {
  if (error.response) {
    alert(
      `Error: Status code: ${error.response.status} - ${error.response.statusText}`,
    );
  } else if (error.request) {
    alert("No response received");
  } else {
    alert(`Error: ${error.message}`);
  }
}

fetch(`${baseURL}/geojsons/${group}/${layer}.json`, {
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
        [layer]: {
          type: "geojson",
          data: jsonResponse.url,
        },
      },
      layers: [],
    };

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
      const item = document.createElement("div");

      const layerColor = inspect.assignLayerColor(layer);

      const row = document.createElement("div");
      row.style.cssText = "display:flex;align-items:center;cursor:pointer;";

      const swatch = document.createElement("div");
      swatch.style.cssText = "width:15px;height:15px;display:inline-block;";
      swatch.style.background = layerColor;

      const label = document.createElement("span");
      label.style.cssText = `margin-left:5px;color:${layerColor};display:inline-block;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;`;
      label.textContent = layer;

      row.append(swatch, label);
      item.appendChild(row);

      item.onclick = () => {
        const newVisibility =
          map.getLayoutProperty(`${layer}_polygon`, "visibility") === "none"
            ? "visible"
            : "none";

        map.setLayoutProperty(`${layer}_polygon`, "visibility", newVisibility);
        map.setLayoutProperty(`${layer}_line`, "visibility", newVisibility);
        map.setLayoutProperty(`${layer}_circle`, "visibility", newVisibility);

        item.style.textDecoration =
          newVisibility === "visible" ? "none" : "line-through";
      };

      layerList.appendChild(item);
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

fetch(`${baseURL}/geojsons/${group}.json`, {
  method: "GET",
  headers: {
    "content-type": "application/json",
  },
})
  .then((response) => {
    if (response.status === 200) {
      return response.json();
    } else {
      alert(
        `Failed to fetch GeoJSON group info: Status code: ${response.status} - ${response.statusText}`,
      );
    }
  })
  .then((jsonResponse) => {
    const style = {
      version: 8,
      sources: {},
      layers: [],
    };

    for (const layer in jsonResponse.geojsons) {
      style["sources"][layer] = {
        type: "geojson",
        data: jsonResponse.geojsons[layer].url,
      };
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
      for (const layer in jsonResponse.geojsons) {
        const item = document.createElement("div");

        const layerColor = inspect.assignLayerColor(layer);

        const row = document.createElement("div");
        row.style.cssText = "display:flex;align-items:center;cursor:pointer;";

        const swatch = document.createElement("div");
        swatch.style.cssText = "width:15px;height:15px;display:inline-block;";
        swatch.style.background = layerColor;

        const label = document.createElement("span");
        label.style.cssText = `margin-left:5px;color:${layerColor};display:inline-block;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;`;
        label.textContent = layer;

        row.append(swatch, label);
        item.appendChild(row);

        item.onclick = () => {
          const newVisibility =
            map.getLayoutProperty(`${layer}_polygon`, "visibility") === "none"
              ? "visible"
              : "none";

          map.setLayoutProperty(
            `${layer}_polygon`,
            "visibility",
            newVisibility,
          );
          map.setLayoutProperty(`${layer}_line`, "visibility", newVisibility);
          map.setLayoutProperty(`${layer}_circle`, "visibility", newVisibility);

          item.style.textDecoration =
            newVisibility === "visible" ? "none" : "line-through";
        };

        layerList.appendChild(item);
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
  .catch((error) => {
    alert(`Failed to fetch GeoJSON info: Status code: ${error.message}`);
  });
