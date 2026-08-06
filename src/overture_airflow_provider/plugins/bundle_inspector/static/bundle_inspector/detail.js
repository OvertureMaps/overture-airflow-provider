// Run-level detail panel and recent bundles.
// Depends on: core.js

function renderDetail(prefix, data) {
    const panel = document.getElementById('detailPanel');
    if (!data) {
        panel.innerHTML = '<div class="detail-empty">Select a node to inspect.</div>';
        return;
    }

    const successChip = data.has_success
        ? '<span class="status-chip success">\u2714 success</span>'
        : '<span class="status-chip absent">\u2718 no success</span>';

    const metadataChip = data.has_metadata
        ? '<span class="status-chip success">\u2714 metadata.json</span>'
        : '<span class="status-chip neutral">no metadata</span>';

    const dirs = (data.children || []).filter(c => c.type === 'directory');
    const files = (data.children || []).filter(c => c.type === 'file');
    const mdFiles = files.filter(f => f.name.endsWith('.md'));
    const otherFiles = files.filter(f => !f.name.endsWith('.md'));

    let componentsHtml = '';
    if (dirs.length > 0) {
        componentsHtml += `<div class="detail-section-title">Components (${dirs.length})</div>`;
        componentsHtml += '<ul class="component-list">';
        for (const d of dirs) {
            componentsHtml += `<li><span class="component-icon">&#128193;</span> ${escapeHtml(d.name.replace(/\/$/, ''))}</li>`;
        }
        componentsHtml += '</ul>';
    }
    if (otherFiles.length > 0) {
        componentsHtml += `<div class="detail-section-title">Files (${otherFiles.length})</div>`;
        componentsHtml += '<ul class="component-list">';
        for (const f of otherFiles) {
            const size = f.size != null ? ` (${formatSize(f.size)})` : '';
            componentsHtml += `<li><span class="component-icon">&#128196;</span> ${escapeHtml(f.name)}${size}</li>`;
        }
        componentsHtml += '</ul>';
    }

    panel.innerHTML = `
        <div class="detail-heading">Run</div>
        <div class="detail-path"><a href="${escapeHtml(s3ConsoleUrl(prefix))}" target="_blank" rel="noopener">${escapeHtml(s3PathForPrefix(prefix))}</a></div>
        <div class="detail-status">${successChip} ${metadataChip}</div>
        ${(componentsHtml || mdFiles.length > 0) ? componentsHtml : '<div style="color:#6c757d;font-style:italic;">No children found.</div>'}
    `;

    if (mdFiles.length > 0) {
        const mdContainer = document.createElement('div');
        panel.appendChild(mdContainer);
        renderMarkdownFiles(mdContainer, prefix, mdFiles);
    }
}
