// Display component: PMTiles vector tile viewer.
// Range requests are proxied through Airflow (/s3proxy) to avoid S3 CORS issues.
// Depends on: core.js, map-utils.js (CARTO_STYLE), components/registry.js, maplibre-gl

let pmtilesReady = null;

function ensurePmtiles() {
    if (pmtilesReady) return pmtilesReady;
    pmtilesReady = import('https://cdn.jsdelivr.net/npm/pmtiles@4.4.0/+esm')
        .then(mod => {
            const protocol = new mod.Protocol();
            maplibregl.addProtocol('pmtiles', protocol.tile);
            return mod;
        })
        .catch(err => {
            pmtilesReady = null;  // allow retry on next call
            throw err;
        });
    return pmtilesReady;
}

function proxyUrl(key) {
    const url = `${location.origin}${API_BASE}/s3proxy?key=${encodeURIComponent(key)}`;
    return `${url}${userParam(url)}`;
}

async function loadPmtilesMap(mapContainer, key) {
    const url = proxyUrl(key);

    const pmtiles = await ensurePmtiles();
    const pm = new pmtiles.PMTiles(url);
    const header = await pm.getHeader();
    const metadata = await pm.getMetadata();

    const layers = (metadata.vector_layers || []).map(l => l.id);
    const layersToUse = layers.length > 0 ? layers : ['default'];

    const map = new maplibregl.Map({
        container: mapContainer,
        style: CARTO_STYLE,
        center: [
            (header.minLon + header.maxLon) / 2,
            (header.minLat + header.maxLat) / 2,
        ],
        zoom: 1,
    });

    map.on('load', () => {
        map.addSource('pmtiles', {
            type: 'vector',
            url: `pmtiles://${url}`,
        });

        for (const layer of layersToUse) {
            map.addLayer({
                id: `pm-fill-${layer}`,
                type: 'fill',
                source: 'pmtiles',
                'source-layer': layer,
                filter: ['==', '$type', 'Polygon'],
                paint: { 'fill-color': '#4051CC', 'fill-opacity': 0.15 },
            });
            map.addLayer({
                id: `pm-line-${layer}`,
                type: 'line',
                source: 'pmtiles',
                'source-layer': layer,
                filter: ['==', '$type', 'LineString'],
                paint: { 'line-color': '#4051CC', 'line-width': 1 },
            });
            map.addLayer({
                id: `pm-point-${layer}`,
                type: 'circle',
                source: 'pmtiles',
                'source-layer': layer,
                filter: ['==', '$type', 'Point'],
                paint: {
                    'circle-color': '#4051CC',
                    'circle-radius': 2.5,
                    'circle-opacity': 0.6,
                },
            });
        }

        // Collect all interactive layer IDs for hover/click queries
        const interactiveLayers = layersToUse.flatMap(layer => [
            `pm-fill-${layer}`, `pm-line-${layer}`, `pm-point-${layer}`,
        ]);

        // Hover: highlight feature under cursor
        map.on('mousemove', (e) => {
            const features = map.queryRenderedFeatures(e.point, { layers: interactiveLayers });
            map.getCanvas().style.cursor = features.length ? 'pointer' : '';

            // Update hover paint for fills
            for (const layer of layersToUse) {
                map.setPaintProperty(`pm-fill-${layer}`, 'fill-opacity',
                    features.length > 0
                        ? ['case', ['==', ['id'], features[0].id ?? -1], 0.45, 0.15]
                        : 0.15
                );
            }
        });
        map.getCanvas().addEventListener('mouseleave', () => {
            map.getCanvas().style.cursor = '';
        });

        // Click: show all overlapping features in a popup
        map.on('click', (e) => {
            const features = map.queryRenderedFeatures(e.point, { layers: interactiveLayers });
            if (!features.length) return;

            function featureTable(props) {
                const rows = Object.entries(props)
                    .map(([k, v]) => {
                        const val = typeof v === 'string' && v.length > 120
                            ? v.slice(0, 120) + '...' : v;
                        return `<tr><td style="font-weight:600;padding-right:8px;vertical-align:top;white-space:nowrap;">${escapeHtml(k)}</td><td>${escapeHtml(String(val))}</td></tr>`;
                    })
                    .join('');
                return rows ? `<table>${rows}</table>` : '<em>No properties</em>';
            }

            const sections = features.map((feat, i) => {
                const label = feat.sourceLayer || `Feature ${i + 1}`;
                const header = features.length > 1
                    ? `<div style="font-weight:700;font-size:11px;color:#4051CC;margin:${i > 0 ? '10px' : '0'} 0 4px;border-top:${i > 0 ? '1px solid #e0e0e0;padding-top:8px;' : 'none'}">${escapeHtml(label)} (${i + 1}/${features.length})</div>`
                    : '';
                return header + featureTable(feat.properties || {});
            }).join('');

            const html = `<div style="max-height:300px;overflow:auto;font-size:12px;">${sections}</div>`;

            new maplibregl.Popup({ maxWidth: '400px' })
                .setLngLat(e.lngLat)
                .setHTML(html)
                .addTo(map);
        });

        if (header.minLon !== 0 || header.maxLon !== 0) {
            map.fitBounds(
                [[header.minLon, header.minLat], [header.maxLon, header.maxLat]],
                { padding: 40 }
            );
        }
    });

    const zoomRange = `z${header.minZoom}-${header.maxZoom}`;
    const infoText = layers.length > 0
        ? `${layers.join(', ')} (${zoomRange})`
        : `PMTiles (${zoomRange})`;

    return { map, infoText };
}

async function renderPmtilesViewer(container, prefix) {
    // List children to discover .pmtiles files
    const data = await fetchChildren(prefix);
    const pmtilesFiles = (data.children || [])
        .filter(c => c.type === 'file' && c.name.endsWith('.pmtiles'))
        .map(c => c.name);

    if (pmtilesFiles.length === 0) {
        container.innerHTML = '<span style="color:#dc3545;">No .pmtiles files found.</span>';
        return () => {};
    }

    let currentMap = null;

    async function showFile(filename) {
        if (currentMap) { currentMap.remove(); currentMap = null; }

        // Clear any previous map/info from the render area
        renderArea.innerHTML = '';

        const mapDiv = document.createElement('div');
        mapDiv.className = 'component-map';
        renderArea.appendChild(mapDiv);

        const loading = document.createElement('div');
        loading.style.cssText = 'color:#6c757d;margin-top:8px;';
        loading.innerHTML = '<span class="spinner-inline"></span> Loading PMTiles header...';
        renderArea.appendChild(loading);

        // Build the S3 key relative to namespace
        const key = prefix + filename;

        try {
            const result = await loadPmtilesMap(mapDiv, key);
            currentMap = result.map;
            loading.remove();

            const info = document.createElement('div');
            info.style.cssText = 'margin-top:8px;font-size:0.85em;color:#6c757d;';
            info.textContent = result.infoText;
            renderArea.appendChild(info);
        } catch (err) {
            loading.innerHTML = `<span style="color:#dc3545;">Error loading PMTiles: ${escapeHtml(err.message)}</span>`;
            console.error('loadPmtilesMap error:', err);
        }
    }

    // Render area for the map (below selector if present)
    const renderArea = document.createElement('div');

    // Show file selector if multiple .pmtiles files
    if (pmtilesFiles.length > 1) {
        const selectorDiv = document.createElement('div');
        selectorDiv.className = 'theme-type-selector';

        const label = document.createElement('label');
        label.textContent = 'Theme';

        const select = document.createElement('select');
        for (const f of pmtilesFiles) {
            const opt = document.createElement('option');
            opt.value = f;
            opt.textContent = f.replace(/\.pmtiles$/, '');
            select.appendChild(opt);
        }
        select.addEventListener('change', () => showFile(select.value));

        selectorDiv.appendChild(label);
        selectorDiv.appendChild(select);
        container.appendChild(selectorDiv);
    }

    container.appendChild(renderArea);
    await showFile(pmtilesFiles[0]);

    return () => { if (currentMap) currentMap.remove(); };
}

registerComponent('pmtiles', { render: renderPmtilesViewer });
