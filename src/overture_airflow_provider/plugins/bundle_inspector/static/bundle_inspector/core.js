// Shared state, API helpers, and utilities.

const API_BASE = '/api/bundle-inspector';

const state = {
    config: null,
    tree: {},
    selectedPrefix: null,
    user: null,
    stageInfo: {},
};

// -- Utilities ------------------------------------------------

function domIdForPrefix(prefix) {
    return 'ch-' + btoa(prefix).replace(/=/g, '_');
}

function escapeHtml(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}

function formatSize(bytes) {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    const val = bytes / Math.pow(1024, i);
    return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`;
}

function s3PathForPrefix(prefix) {
    const c = state.config;
    const ns = state.user || c.namespace;
    let path = `s3://${c.bucket}/`;
    if (c.environment === 'dev' && ns) path += `${ns}/`;
    return path + prefix;
}

function s3ConsoleUrl(prefix) {
    const c = state.config;
    const ns = state.user || c.namespace;
    let key = '';
    if (c.environment === 'dev' && ns) key += `${ns}/`;
    key += prefix;
    return `https://${c.aws_region || 'us-west-2'}.console.aws.amazon.com/s3/buckets/${c.bucket}?prefix=${encodeURIComponent(key)}`;
}

function setSelectedTreeItem(prefix) {
    document.querySelectorAll('.tree-item.selected').forEach(el => el.classList.remove('selected'));
    const row = document.querySelector(`.tree-item[data-prefix="${CSS.escape(prefix)}"]`);
    if (row) row.classList.add('selected');
    state.selectedPrefix = prefix;
    pushPrefixToUrl(prefix);
}

function pushPrefixToUrl(prefix) {
    const base = '/bundle-inspector/';
    const userPart = state.user ? state.user + '/' : '';
    const url = base + userPart + (prefix || '');
    if (window.location.pathname !== url) {
        history.replaceState(null, '', url);
    }
}

function prefixFromUrl() {
    const path = window.location.pathname;
    const base = '/bundle-inspector/';
    if (!path.startsWith(base)) return null;
    return path.slice(base.length) || null;
}

// -- API helpers ----------------------------------------------

function userParam(url) {
    if (!state.user) return '';
    const sep = url.includes('?') ? '&' : '?';
    return `${sep}user=${encodeURIComponent(state.user)}`;
}

async function fetchConfig() {
    const url = `${API_BASE}/config?_=1`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`config: ${res.status}`);
    state.config = await res.json();
    renderSidebarInfo();
}

async function fetchChildren(prefix) {
    const url = `${API_BASE}/list?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`list: ${res.status}`);
    return await res.json();
}


async function fetchUsers() {
    const res = await fetch(`${API_BASE}/users`);
    if (!res.ok) throw new Error(`users: ${res.status}`);
    return await res.json();
}

async function fetchThemeTypes(prefix) {
    const url = `${API_BASE}/theme-types?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`theme-types: ${res.status}`);
    return await res.json();
}

async function fetchCount(prefix) {
    const url = `${API_BASE}/count?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`count: ${res.status}`);
    return await res.json();
}

async function fetchParquetStats(prefix) {
    const url = `${API_BASE}/parquet-stats?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`parquet-stats: ${res.status}`);
    return await res.json();
}

async function fetchTree(prefix) {
    const url = `${API_BASE}/tree?prefix=${encodeURIComponent(prefix)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    if (!res.ok) throw new Error(`tree: ${res.status}`);
    return await res.json();
}

async function fetchAthenaStart(sql) {
    const params = new URLSearchParams({ sql });
    const url = `${API_BASE}/athena-query?${params}`;
    const res = await fetch(`${url}${userParam(url)}`);
    return await res.json();
}

async function fetchAthenaStatus(queryId) {
    const url = `${API_BASE}/athena-status?query_id=${encodeURIComponent(queryId)}`;
    const res = await fetch(`${url}${userParam(url)}`);
    return await res.json();
}

async function fetchAthenaQuery(sql, onStatus) {
    const start = await fetchAthenaStart(sql);
    if (start.error) return start;
    const queryId = start.query_id;

    const POLL_INTERVAL = 1000;
    const TIMEOUT = 5 * 60 * 1000;
    const deadline = Date.now() + TIMEOUT;

    while (Date.now() < deadline) {
        const status = await fetchAthenaStatus(queryId);
        if (status.error) return status;
        if (onStatus) onStatus(status.state);
        if (status.state === 'SUCCEEDED') return status;
        if (status.state === 'FAILED' || status.state === 'CANCELLED') return status;
        await new Promise(r => setTimeout(r, POLL_INTERVAL));
    }
    return { error: 'Query timed out after 5 minutes.' };
}

// -- Sidebar info ---------------------------------------------

function renderSidebarInfo() {
    const el = document.getElementById('sidebarInfo');
    if (!state.config) { el.textContent = ''; return; }
    const c = state.config;
    let html = `<div>Bucket: ${escapeHtml(c.bucket)}</div>`;
    if (c.environment) html += `<div>Environment: ${escapeHtml(c.environment)}</div>`;
    if (c.environment === 'dev' && c.namespace) html += `<div>User: ${escapeHtml(c.namespace)}</div>`;
    el.innerHTML = html;
}

// -- User selector --------------------------------------------

async function loadUserSelector() {
    if (!state.config || state.config.environment !== 'dev') return;
    try {
        const data = await fetchUsers();
        if (!data.users || data.users.length === 0) return;

        const selector = document.getElementById('userSelector');
        const select = document.getElementById('userSelect');
        selector.style.display = 'block';

        select.innerHTML = '';
        const activeUser = state.user || state.config.namespace;
        for (const u of data.users) {
            const opt = document.createElement('option');
            opt.value = u;
            opt.textContent = u;
            if (u === activeUser) opt.selected = true;
            select.appendChild(opt);
        }
    } catch (err) {
        console.error('loadUserSelector error:', err);
    }
}

async function onUserChange() {
    const select = document.getElementById('userSelect');
    state.user = select.value;
    pushPrefixToUrl(null);
    await refresh();
}
