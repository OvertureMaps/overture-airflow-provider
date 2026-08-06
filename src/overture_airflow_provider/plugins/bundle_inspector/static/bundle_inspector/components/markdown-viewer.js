// Display component: markdown file viewer.
// Fetches .md files via s3proxy and renders them as HTML using marked.
// Depends on: core.js (API_BASE, userParam, escapeHtml), marked (CDN)

// Escape raw HTML in markdown source to prevent XSS.
marked.use({
    renderer: {
        html(token) { return escapeHtml(token.raw); },
    }
});

async function fetchMarkdownText(key) {
    const url = `${API_BASE}/s3proxy?key=${encodeURIComponent(key)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`Failed to fetch markdown: ${res.status}`);
    return await res.text();
}

async function renderMarkdownFiles(container, prefix, mdFiles) {
    if (mdFiles.length === 0) return;

    const results = await Promise.all(mdFiles.map(async f => {
        const key = prefix + f.name;
        try {
            const text = await fetchMarkdownText(key);
            return { filename: f.name, text };
        } catch (err) {
            return { filename: f.name, error: err.message };
        }
    }));

    for (const r of results) {
        const section = document.createElement('div');
        section.className = 'markdown-section';

        const heading = document.createElement('div');
        heading.className = 'detail-section-title';
        heading.textContent = r.filename;
        section.appendChild(heading);

        if (r.error) {
            const errDiv = document.createElement('div');
            errDiv.style.color = '#dc3545';
            errDiv.textContent = `Error loading ${r.filename}: ${r.error}`;
            section.appendChild(errDiv);
        } else {
            const body = document.createElement('div');
            body.className = 'markdown-body';
            body.innerHTML = marked.parse(r.text);
            section.appendChild(body);
        }

        container.appendChild(section);
    }
}
