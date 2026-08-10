// Display component: Athena query editor with streaming CSV results and geometry map.
// Depends on: core.js, csv-utils.js, map-utils.js, wkb.js (parseWkbHex, detectGeometryColumn,
//             vertexAverage), components/registry.js, maplibre-gl

const ATHENA_FOLDERS = new Set(['data', 'metrics', 'changelog', 'changelog_internal', 'bridgefiles', 'registry', 'validation']);
const KNOWN_STAGES = new Set(['theme_promote', 'theme_stage', 'release_candidate']);
const CHUNK_SIZE = 1024 * 1024; // 1 MB per Range request
const MAX_MAP_FEATURES = 10000;

let _codeJar;
async function loadCodeJar() {
    if (_codeJar) return _codeJar;
    const mod = await import('https://cdn.jsdelivr.net/npm/codejar@4.2.0/dist/codejar.js');
    _codeJar = mod.CodeJar;
    return _codeJar;
}

function sanitizeIdentifier(s) {
    return s.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+|_+$/g, '') || 'segment';
}

function deriveAthenaTable(prefix) {
    const parts = prefix.replace(/\/$/, '').split('/');

    const stageIndex = parts.findIndex(p => KNOWN_STAGES.has(p));
    if (stageIndex === -1) return null;
    const stage = parts[stageIndex];
    const rest = parts.slice(stageIndex + 1);

    let folderIndex = -1;
    for (let i = parts.length - 1; i >= 0; i--) {
        if (ATHENA_FOLDERS.has(parts[i])) { folderIndex = i; break; }
    }
    if (folderIndex === -1) return null;
    const folder = parts[folderIndex];

    let run = null;
    for (const part of rest) {
        if (part.startsWith('run=')) { run = sanitizeIdentifier(part.split('=')[1]); break; }
    }

    // Inner theme= (after folder) is the fan-out root, not a data partition.
    // Segments after the inner theme= are the actual partition columns (e.g. type=).
    const afterFolder = parts.slice(folderIndex + 1);
    const innerThemeIdx = afterFolder.findIndex(p => p.startsWith('theme='));
    const innerTheme = innerThemeIdx >= 0 ? afterFolder[innerThemeIdx].split('=')[1] : null;

    // Partition filters: Hive key=value segments after the table root.
    const STRUCTURAL_KEYS = new Set(['run', 'schema', 'release']);
    const tableRootEnd = innerThemeIdx >= 0
        ? folderIndex + 2 + innerThemeIdx
        : folderIndex + 1;
    const partitionFilters = parts.slice(tableRootEnd)
        .filter(p => p.includes('=') && !STRUCTURAL_KEYS.has(p.split('=')[0]))
        .map(p => {
            const eqIdx = p.indexOf('=');
            const val = p.slice(eqIdx + 1).replace(/'/g, "''");
            return `${p.slice(0, eqIdx)} = '${val}'`;
        });

    const nameParts = [stage];
    if (stage === 'theme_promote') {
        const restFolderIdx = folderIndex - stageIndex - 1;
        const outerThemeSegment = rest.slice(0, restFolderIdx).find(p => p.startsWith('theme='));
        const theme = outerThemeSegment ? outerThemeSegment.split('=')[1] : rest[0];
        nameParts.push(theme);
    } else if (stage === 'theme_stage') {
        const dataset = rest[0] && !rest[0].includes('=') ? rest[0] : '';
        if (dataset) nameParts.push(dataset);
    }
    if (run) nameParts.push(run);
    if (innerTheme && folder === 'data') nameParts.push(innerTheme);
    nameParts.push(folder);
    const table = nameParts.map(sanitizeIdentifier).join('_');

    return { table, partitionFilters };
}

function deriveAthenaDatabase() {
    const cfg = state.config;
    if (!cfg) return 'pipeline';
    const ns = state.user || cfg.namespace;
    if (cfg.environment === 'dev' && ns) return `${ns}_pipeline`;
    return 'pipeline';
}

// Find the last safe line boundary in a chunk of CSV text.
// "Safe" means all quotes are balanced up to that point, so we don't
// split inside a quoted field that contains embedded newlines.
function findSafeBreak(text) {
    let inQuote = false;
    let lastNewline = -1;
    let i = 0;
    while (i < text.length) {
        const ch = text[i];
        if (ch === '"') {
            if (inQuote && i + 1 < text.length && text[i + 1] === '"') {
                i += 2; // escaped double-quote inside quoted field
                continue;
            }
            inQuote = !inQuote;
        } else if (ch === '\r') {
            if (!inQuote && i + 1 < text.length && text[i + 1] === '\n') {
                lastNewline = i + 1;
            }
        } else if (ch === '\n' && !inQuote) {
            lastNewline = i;
        }
        i++;
    }
    return lastNewline;
}

// Fetch a byte range from a URL. Returns {text, totalSize, bytesRead}.
async function fetchChunk(url, start, size) {
    const end = start + size - 1;
    const res = await fetch(url, { headers: { 'Range': `bytes=${start}-${end}` } });
    if (!res.ok && res.status !== 206) {
        throw new Error(res.status === 403 ? 'URL expired, re-run query' : `Fetch failed: ${res.status}`);
    }
    const text = await res.text();
    const contentRange = res.headers.get('Content-Range');
    const contentLength = res.headers.get('Content-Length');
    const bytesRead = contentLength ? parseInt(contentLength, 10) : new TextEncoder().encode(text).length;
    const totalSize = contentRange ? parseInt(contentRange.split('/')[1], 10) : bytesRead;
    return { text, totalSize, bytesRead };
}

function renderResultTable(container, columns, rows) {
    const headerCells = columns.map(c => `<th>${escapeHtml(c)}</th>`).join('');
    const bodyRows = rows.map(row =>
        '<tr>' + row.map(val => `<td>${escapeHtml(val)}</td>`).join('') + '</tr>'
    ).join('');

    const wrap = document.createElement('div');
    wrap.className = 'athena-table-wrap';
    wrap.innerHTML = `
        <table class="athena-table">
            <thead><tr>${headerCells}</tr></thead>
            <tbody>${bodyRows}</tbody>
        </table>
    `;
    container.appendChild(wrap);
    return wrap.querySelector('tbody');
}

function appendRows(tbody, rows) {
    const fragment = document.createDocumentFragment();
    for (const row of rows) {
        const tr = document.createElement('tr');
        for (const val of row) {
            const td = document.createElement('td');
            td.textContent = val;
            tr.appendChild(td);
        }
        fragment.appendChild(tr);
    }
    tbody.appendChild(fragment);
}

function featurePopupHtml(properties) {
    const rows = Object.entries(properties)
        .map(([k, v]) => {
            const val = typeof v === 'string' && v.length > 120 ? v.slice(0, 120) + '...' : v;
            return `<tr><td style="font-weight:600;padding-right:8px;vertical-align:top;white-space:nowrap;">${escapeHtml(k)}</td><td>${escapeHtml(String(val))}</td></tr>`;
        })
        .join('');
    return rows ? `<div style="max-height:300px;overflow:auto;font-size:12px;"><table>${rows}</table></div>` : '<em>No properties</em>';
}

function renderGeometryMap(container, dataFc, markerFc) {
    const mapDiv = document.createElement('div');
    mapDiv.className = 'component-map';
    container.appendChild(mapDiv);

    const bboxDiv = document.createElement('div');
    bboxDiv.className = 'bbox-filter-hint';
    container.appendChild(bboxDiv);

    const map = new maplibregl.Map({
        container: mapDiv,
        style: CARTO_STYLE,
        center: [0, 20],
        zoom: 1,
        minZoom: 0,
        maxZoom: 22,
    });

    function updateBboxFilter() {
        const b = map.getBounds();
        const fmt = (n) => n.toFixed(4);
        const text = `AND bbox.xmax >= ${fmt(b.getWest())} AND bbox.xmin <= ${fmt(b.getEast())} AND bbox.ymax >= ${fmt(b.getSouth())} AND bbox.ymin <= ${fmt(b.getNorth())}`;
        bboxDiv.textContent = text;
        bboxDiv.title = 'Click to copy';
    }
    map.on('moveend', updateBboxFilter);
    bboxDiv.addEventListener('click', () => {
        navigator.clipboard.writeText(bboxDiv.textContent);
        bboxDiv.classList.add('copied');
        setTimeout(() => bboxDiv.classList.remove('copied'), 1200);
    });

    map.on('load', () => {
        map.addSource('athena-geom', { type: 'geojson', data: dataFc });
        map.addSource('athena-markers', { type: 'geojson', data: markerFc });
        map.addLayer({
            id: 'athena-fill',
            type: 'fill',
            source: 'athena-geom',
            minzoom: 0,
            filter: ['==', '$type', 'Polygon'],
            paint: { 'fill-color': '#4051CC', 'fill-opacity': 0.35 },
        });
        map.addLayer({
            id: 'athena-outline',
            type: 'line',
            source: 'athena-geom',
            minzoom: 0,
            filter: ['==', '$type', 'Polygon'],
            paint: { 'line-color': '#4051CC', 'line-width': 1.5 },
        });
        map.addLayer({
            id: 'athena-line',
            type: 'line',
            source: 'athena-geom',
            minzoom: 0,
            filter: ['==', '$type', 'LineString'],
            paint: { 'line-color': '#4051CC', 'line-width': 2 },
        });
        map.addLayer({
            id: 'athena-point',
            type: 'circle',
            source: 'athena-geom',
            minzoom: 0,
            filter: ['==', '$type', 'Point'],
            paint: { 'circle-color': '#4051CC', 'circle-radius': 5, 'circle-opacity': 0.8 },
        });
        map.addLayer({
            id: 'athena-marker',
            type: 'circle',
            source: 'athena-markers',
            minzoom: 0,
            maxzoom: 14,
            paint: { 'circle-color': '#4051CC', 'circle-radius': 5, 'circle-opacity': 0.8 },
        });

        const interactiveLayers = ['athena-fill', 'athena-outline', 'athena-line', 'athena-point', 'athena-marker'];
        map.on('mousemove', (e) => {
            const features = map.queryRenderedFeatures(e.point, { layers: interactiveLayers });
            map.getCanvas().style.cursor = features.length ? 'pointer' : '';
        });
        map.on('click', (e) => {
            const features = map.queryRenderedFeatures(e.point, { layers: interactiveLayers });
            if (!features.length) return;
            new maplibregl.Popup({ maxWidth: '400px' })
                .setLngLat(e.lngLat)
                .setHTML(featurePopupHtml(features[0].properties || {}))
                .addTo(map);
        });

        fitMapToFeatures(map, dataFc.features);
        updateBboxFilter();
    });

    return map;
}

async function renderAthenaPreview(container, prefix) {
    const info = deriveAthenaTable(prefix);
    const database = deriveAthenaDatabase();

    if (!info) {
        const msg = document.createElement('div');
        msg.style.cssText = 'color:#6c757d;margin-top:16px;';
        msg.textContent = 'Could not derive Athena table name from path.';
        container.appendChild(msg);
        return () => {};
    }

    const wrapper = document.createElement('div');
    wrapper.className = 'athena-preview';
    container.appendChild(wrapper);

    const title = document.createElement('div');
    title.className = 'detail-section-title';
    title.style.marginTop = '24px';
    title.textContent = 'Athena Query';
    wrapper.appendChild(title);

    const editorRow = document.createElement('div');
    editorRow.className = 'athena-editor';
    wrapper.appendChild(editorRow);

    const partitionFilters = info.partitionFilters || [];
    const typeFilter = partitionFilters.length > 0 ? `\nWHERE ${partitionFilters.join('\n  AND ')}` : '';
    const defaultSql = `SELECT * FROM "${database}"."${info.table}"${typeFilter}\nLIMIT 10`;
    let getSql, editorCleanup = () => {};

    function onEditorKey(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            runQuery();
        }
    }

    try {
        const CodeJar = await loadCodeJar();
        const editorEl = document.createElement('div');
        editorEl.className = 'athena-sql language-sql';
        editorEl.textContent = defaultSql;
        editorRow.appendChild(editorEl);
        const highlight = (el) => Prism.highlightElement(el);
        const jar = CodeJar(editorEl, highlight, { tab: '  ' });
        jar.updateCode(jar.toString());
        getSql = () => jar.toString().trim();
        editorCleanup = () => jar.destroy();
        editorEl.addEventListener('keydown', onEditorKey);
    } catch (err) {
        console.warn('CodeJar load failed, using textarea fallback', err);
        const textarea = document.createElement('textarea');
        textarea.className = 'athena-sql';
        textarea.rows = 3;
        textarea.value = defaultSql;
        editorRow.appendChild(textarea);
        getSql = () => textarea.value.trim();
        textarea.addEventListener('keydown', onEditorKey);
    }

    const btnRow = document.createElement('div');
    btnRow.className = 'athena-controls';
    wrapper.appendChild(btnRow);

    const runBtn = document.createElement('button');
    runBtn.className = 'btn btn-primary';
    runBtn.textContent = 'Run';
    btnRow.appendChild(runBtn);

    const statusSpan = document.createElement('span');
    statusSpan.className = 'athena-status';
    btnRow.appendChild(statusSpan);

    const mapArea = document.createElement('div');
    wrapper.appendChild(mapArea);

    const resultsDiv = document.createElement('div');
    resultsDiv.className = 'athena-results';
    wrapper.appendChild(resultsDiv);

    const statusBar = document.createElement('div');
    statusBar.className = 'athena-status-bar';
    wrapper.appendChild(statusBar);

    // Mutable query state
    let currentMap = null;
    let downloadUrl = null;  // presigned URL for download link (navigation, no CORS)
    let csvProxyUrl = null;  // proxied URL for Range fetches (avoids CORS)
    let columns = null;
    let allRows = [];
    let parsedFeatures = []; // cached GeoJSON features to avoid re-decoding WKB
    let remainder = '';
    let bytesLoaded = 0;
    let totalSize = 0;
    let geomCol = -1;
    let tbody = null;

    function updateStatusBar() {
        const loaded = formatSize(bytesLoaded);
        const total = formatSize(totalSize);
        const hasMore = bytesLoaded < totalSize;

        let html = `${allRows.length} row${allRows.length !== 1 ? 's' : ''} (${loaded} of ${total})`;
        if (geomCol >= 0 && allRows.length > MAX_MAP_FEATURES) {
            html += ` &middot; map shows first ${MAX_MAP_FEATURES.toLocaleString()} features`;
        }
        if (downloadUrl) {
            html += ` <a href="${escapeHtml(downloadUrl)}" target="_blank" rel="noopener" class="athena-download">Download full CSV</a>`;
        }
        statusBar.innerHTML = html;

        // Load more button
        const existingBtn = statusBar.querySelector('.athena-load-more');
        if (existingBtn) existingBtn.remove();
        if (hasMore) {
            const btn = document.createElement('button');
            btn.className = 'btn btn-secondary athena-load-more';
            btn.textContent = 'Load more';
            btn.addEventListener('click', loadMore);
            statusBar.appendChild(btn);
        }
    }

    function updateMap() {
        if (geomCol < 0) return;
        // Parse only newly added rows since last update
        const propIndices = columns.map((col, i) => [col, i]).filter(([_, i]) => i !== geomCol);
        while (parsedFeatures.length < allRows.length && parsedFeatures.length < MAX_MAP_FEATURES) {
            const row = allRows[parsedFeatures.length];
            const geometry = parseWkbHex(row[geomCol]);
            if (!geometry) { parsedFeatures.push(null); continue; }
            const properties = {};
            for (const [col, idx] of propIndices) properties[col] = row[idx];
            const feature = { type: 'Feature', geometry, properties };
            parsedFeatures.push(feature);
        }
        const features = parsedFeatures.filter(f => f !== null);
        // Add vertex-average markers for non-point features
        const markers = features
            .filter(f => f.geometry.type !== 'Point')
            .map(f => ({
                type: 'Feature',
                properties: { ...f.properties, _marker: true },
                geometry: { type: 'Point', coordinates: vertexAverage(f.geometry) },
            }));
        const dataFc = { type: 'FeatureCollection', features };
        const markerFc = { type: 'FeatureCollection', features: markers };
        if (currentMap) {
            const src = currentMap.getSource('athena-geom');
            if (src) {
                src.setData(dataFc);
                currentMap.getSource('athena-markers').setData(markerFc);
                return;
            }
        }
        currentMap = renderGeometryMap(mapArea, dataFc, markerFc);
    }

    async function loadMore() {
        if (bytesLoaded >= totalSize) return;
        const loadBtn = statusBar.querySelector('.athena-load-more');
        if (loadBtn) { loadBtn.disabled = true; loadBtn.textContent = 'Loading...'; }

        try {
            const chunk = await fetchChunk(csvProxyUrl, bytesLoaded, CHUNK_SIZE);
            const text = remainder + chunk.text;
            const breakIdx = findSafeBreak(text);

            if (breakIdx === -1) {
                // Entire chunk is one incomplete line -- accumulate and try again
                remainder = text;
                bytesLoaded += chunk.bytesRead;
            } else {
                const complete = text.slice(0, breakIdx + 1);
                remainder = text.slice(breakIdx + 1);
                bytesLoaded += chunk.bytesRead;
                if (bytesLoaded > totalSize) bytesLoaded = totalSize;

                const newRows = splitCsvLines(complete);
                allRows = allRows.concat(newRows);
                appendRows(tbody, newRows);
                updateMap();
            }

            updateStatusBar();
        } catch (err) {
            statusBar.innerHTML = `<span style="color:#dc3545;">${escapeHtml(String(err))}</span>`;
        }
    }

    async function runQuery() {
        const sql = getSql();
        if (!sql) return;

        runBtn.disabled = true;
        statusSpan.innerHTML = '<span class="spinner-inline"></span> Running...';
        resultsDiv.innerHTML = '';
        mapArea.innerHTML = '';
        statusBar.innerHTML = '';
        if (currentMap) { currentMap.remove(); currentMap = null; }
        allRows = [];
        parsedFeatures = [];
        remainder = '';
        bytesLoaded = 0;
        totalSize = 0;
        geomCol = -1;
        columns = null;
        csvProxyUrl = null;
        tbody = null;

        // Phase 1: start query, poll until Athena finishes
        let data;
        try {
            data = await fetchAthenaQuery(sql, (athenaState) => {
                const label = athenaState === 'RUNNING' ? 'Running' : athenaState === 'QUEUED' ? 'Queued' : athenaState;
                statusSpan.innerHTML = `<span class="spinner-inline"></span> ${label}...`;
            });
        } catch (err) {
            runBtn.disabled = false;
            statusSpan.textContent = '';
            resultsDiv.innerHTML = `<div class="athena-error">${escapeHtml(String(err))}</div>`;
            return;
        }

        runBtn.disabled = false;
        statusSpan.textContent = '';

        if (data.error) {
            resultsDiv.innerHTML = `<div class="athena-error">${escapeHtml(data.error)}</div>`;
            return;
        }

        downloadUrl = data.download_url;
        const queryId = data.query_id;
        if (!queryId) {
            resultsDiv.innerHTML = '<div class="athena-error">No query ID returned.</div>';
            return;
        }

        // Build proxy URL for Range fetches (avoids CORS on S3 presigned URLs)
        const proxyBase = `${API_BASE}/athena-csv?query_id=${encodeURIComponent(queryId)}`;
        csvProxyUrl = `${proxyBase}${userParam(proxyBase)}`;

        // Phase 2: stream first CSV chunk via proxy
        statusSpan.innerHTML = '<span class="spinner-inline"></span> Fetching results...';

        let chunk;
        try {
            chunk = await fetchChunk(csvProxyUrl, 0, CHUNK_SIZE);
        } catch (err) {
            statusSpan.textContent = '';
            resultsDiv.innerHTML = `<div class="athena-error">${escapeHtml(String(err))}</div>`;
            return;
        }

        statusSpan.textContent = '';
        totalSize = chunk.totalSize;

        const text = chunk.text;
        const breakIdx = findSafeBreak(text);

        let complete;
        if (breakIdx === -1) {
            // Entire chunk is one logical line (or file is small enough)
            complete = text;
            remainder = '';
            bytesLoaded = chunk.bytesRead;
        } else {
            complete = text.slice(0, breakIdx + 1);
            remainder = text.slice(breakIdx + 1);
            bytesLoaded = chunk.bytesRead;
        }
        if (bytesLoaded >= totalSize) {
            bytesLoaded = totalSize;
            remainder = '';
            // If the whole file fits in one chunk, use all the text
            complete = text;
        }

        const parsed = splitCsvLines(complete);
        if (parsed.length === 0) {
            resultsDiv.innerHTML = '<div style="color:#6c757d;">No results.</div>';
            return;
        }

        columns = parsed[0];
        allRows = parsed.slice(1);

        if (columns.length === 0) {
            resultsDiv.innerHTML = '<div style="color:#6c757d;">No results.</div>';
            return;
        }

        // Phase 3: render table
        tbody = renderResultTable(resultsDiv, columns, allRows);

        // Phase 4: detect geometry and render map
        // Scan up to 50 rows in case early rows have null geometry
        const scanLimit = Math.min(allRows.length, 50);
        for (let i = 0; i < scanLimit && geomCol < 0; i++) {
            geomCol = detectGeometryColumn(columns, allRows[i]);
        }
        if (geomCol >= 0) updateMap();

        updateStatusBar();
    }

    runBtn.addEventListener('click', runQuery);

    return () => {
        editorCleanup();
        if (currentMap) currentMap.remove();
    };
}

registerComponent('data', { render: renderAthenaPreview });
registerComponent('metrics', { render: renderAthenaPreview });
registerComponent('changelog', { render: renderAthenaPreview });
registerComponent('changelog_internal', { render: renderAthenaPreview });
registerComponent('bridgefiles', { render: renderAthenaPreview });
registerComponent('registry', { render: renderAthenaPreview });
registerComponent('validation', { render: renderAthenaPreview });
