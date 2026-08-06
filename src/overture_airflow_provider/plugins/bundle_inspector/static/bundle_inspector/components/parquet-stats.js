// Display component: parquet file stats and schema.
// Depends on: core.js (formatSize, fetchParquetStats, escapeHtml), components/registry.js

async function renderParquetStats(container, prefix) {
    const loading = document.createElement('div');
    loading.style.cssText = 'color:#6c757d;margin-top:8px;';
    loading.innerHTML = '<span class="spinner-inline"></span> Loading parquet stats...';
    container.appendChild(loading);

    let stats;
    try {
        stats = await fetchParquetStats(prefix);
    } catch (err) {
        loading.innerHTML = '<span style="color:#dc3545;">Error loading stats.</span>';
        console.error('parquet-stats error:', err);
        return () => {};
    }

    loading.remove();

    if (stats.file_count === 0) {
        container.innerHTML = '<span style="color:#6c757d;">No parquet files found.</span>';
        return () => {};
    }

    const statsHtml = `
        <table class="parquet-stats-table">
            <tr><td>Files</td><td>${stats.file_count.toLocaleString()}</td></tr>
            <tr><td>Total size</td><td>${formatSize(stats.total_size)}</td></tr>
            <tr><td>Avg size</td><td>${formatSize(stats.avg_size)}</td></tr>
            <tr><td>Min size</td><td>${formatSize(stats.min_size)}</td></tr>
            <tr><td>Max size</td><td>${formatSize(stats.max_size)}</td></tr>
        </table>
    `;

    let schemaHtml = '';
    if (stats.schema && stats.schema.length > 0) {
        const rows = stats.schema.map(f =>
            `<tr><td>${escapeHtml(f.name)}</td><td>${escapeHtml(f.type)}</td></tr>`
        ).join('');
        schemaHtml = `
            <div class="detail-section-title" style="margin-top:16px;">Schema</div>
            <table class="parquet-schema-table">
                <thead><tr><th>Column</th><th>Type</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        `;
    }

    const wrapper = document.createElement('div');
    wrapper.innerHTML = statsHtml + schemaHtml;
    container.appendChild(wrapper);

    return () => {};
}

registerComponent('data', { render: renderParquetStats });
registerComponent('metrics', { render: renderParquetStats });
registerComponent('changelog', { render: renderParquetStats });
registerComponent('changelog_internal', { render: renderParquetStats });
registerComponent('bridgefiles', { render: renderParquetStats });
registerComponent('registry', { render: renderParquetStats });
registerComponent('validation', { render: renderParquetStats });
