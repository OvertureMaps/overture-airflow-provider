// Display component: partition bounding boxes on a map.
// Depends on: core.js, map-utils.js (CARTO_STYLE, fitMapToFeatures), components/registry.js, maplibre-gl

async function renderBboxMap(container, prefix) {
    const mapDiv = document.createElement('div');
    mapDiv.className = 'component-map';
    container.appendChild(mapDiv);

    const loading = document.createElement('div');
    loading.style.cssText = 'color:#6c757d;margin-top:8px;';
    loading.innerHTML = '<span class="spinner-inline"></span> Loading parquet data...';
    container.appendChild(loading);

    const dataUrl = `${API_BASE}/component-data?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${dataUrl}${userParam(dataUrl)}`);
    if (!res.ok) {
        loading.innerHTML = '<span style="color:#dc3545;">Error loading data.</span>';
        return () => {};
    }
    const geojson = await res.json();
    loading.remove();

    const map = new maplibregl.Map({
        container: mapDiv,
        style: CARTO_STYLE,
        center: [0, 20],
        zoom: 1,
    });

    map.on('load', () => {
        map.addSource('bboxes', { type: 'geojson', data: geojson });
        map.addLayer({
            id: 'bbox-fill',
            type: 'fill',
            source: 'bboxes',
            paint: { 'fill-color': '#4051CC', 'fill-opacity': 0.12 },
        });
        map.addLayer({
            id: 'bbox-outline',
            type: 'line',
            source: 'bboxes',
            paint: { 'line-color': '#4051CC', 'line-width': 1 },
        });

        fitMapToFeatures(map, geojson.features);
    });

    const info = document.createElement('div');
    info.style.cssText = 'margin-top:8px;font-size:0.85em;color:#6c757d;';
    info.textContent = `${geojson.features.length} partition bboxes`;
    container.appendChild(info);

    return () => map.remove();
}

registerComponent('partition_bboxes', { render: renderBboxMap });
