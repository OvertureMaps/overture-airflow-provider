// Display component: CSV file viewer.
// Lists .csv files under a prefix, fetches each via s3proxy, and renders as tables.
// Depends on: core.js (fetchChildren, escapeHtml, userParam, API_BASE),
//             csv-utils.js (splitCsvRow, parseCsv), components/registry.js

async function fetchCsvText(key) {
    const url = `${API_BASE}/s3proxy?key=${encodeURIComponent(key)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`Failed to fetch CSV: ${res.status}`);
    return await res.text();
}

function renderCsvTable(container, title, csv) {
    const heading = document.createElement('div');
    heading.className = 'detail-section-title';
    heading.style.marginTop = '16px';
    heading.textContent = title;
    container.appendChild(heading);

    if (csv.columns.length === 0) {
        const msg = document.createElement('div');
        msg.style.color = '#6c757d';
        msg.textContent = 'Empty CSV.';
        container.appendChild(msg);
        return;
    }

    const headerCells = csv.columns.map(c =>
        `<th>${escapeHtml(c)}</th>`
    ).join('');

    const bodyRows = csv.rows.map(row =>
        '<tr>' + row.map(val =>
            `<td>${escapeHtml(val)}</td>`
        ).join('') + '</tr>'
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
}

async function renderCsvViewer(container, prefix) {
    const data = await fetchChildren(prefix);
    const csvFiles = (data.children || [])
        .filter(c => c.type === 'file' && c.name.endsWith('.csv'))
        .map(c => c.name);

    if (csvFiles.length === 0) {
        const msg = document.createElement('div');
        msg.style.cssText = 'color:#6c757d;margin-top:16px;';
        msg.textContent = 'No CSV files found.';
        container.appendChild(msg);
        return () => {};
    }

    const results = await Promise.all(csvFiles.map(async filename => {
        const key = prefix + filename;
        try {
            const text = await fetchCsvText(key);
            return { filename, csv: parseCsv(text) };
        } catch (err) {
            return { filename, error: err.message };
        }
    }));

    for (const r of results) {
        if (r.error) {
            const errDiv = document.createElement('div');
            errDiv.style.color = '#dc3545';
            errDiv.textContent = `Error loading ${r.filename}: ${r.error}`;
            container.appendChild(errDiv);
        } else {
            renderCsvTable(container, r.filename.replace(/\.csv$/, ''), r.csv);
        }
    }

    return () => {};
}

registerComponent('metrics_summary', { render: renderCsvViewer });
