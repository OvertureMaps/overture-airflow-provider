// Display component: abridged folder tree for any bundle component.
// Depends on: core.js (fetchTree, escapeHtml, formatSize), components/registry.js

async function renderFolderTree(container, prefix) {
    const loading = document.createElement('div');
    loading.style.cssText = 'color:#6c757d;margin-top:16px;';
    loading.innerHTML = '<span class="spinner-inline"></span> Loading folder structure...';
    container.appendChild(loading);

    let tree;
    try {
        tree = await fetchTree(prefix);
    } catch (err) {
        loading.innerHTML = '<span style="color:#dc3545;">Error loading tree.</span>';
        console.error('folder-tree error:', err);
        return () => {};
    }

    loading.remove();

    const title = document.createElement('div');
    title.className = 'detail-section-title';
    title.style.marginTop = '24px';
    title.textContent = 'Folder Structure';
    container.appendChild(title);

    if (tree._total_keys === 0) {
        const empty = document.createElement('div');
        empty.style.color = '#6c757d';
        empty.textContent = 'Empty directory.';
        container.appendChild(empty);
        return () => {};
    }

    const info = document.createElement('div');
    info.style.cssText = 'color:#6c757d;font-size:0.85em;margin-bottom:8px;';
    let infoText = `${tree._total_keys.toLocaleString()} objects`;
    if (tree._truncated) infoText += ' (listing truncated)';
    info.textContent = infoText;
    container.appendChild(info);

    const pre = document.createElement('pre');
    pre.className = 'folder-tree';
    container.appendChild(pre);

    const lines = [];
    renderTreeNode(lines, tree, '', true);
    pre.textContent = lines.join('\n');

    return () => {};
}

function renderTreeNode(lines, node, indent, isRoot) {
    const dirNames = Object.keys(node.dirs).sort();
    const files = node.files || [];
    const omitted = node._omitted || 0;

    for (let i = 0; i < dirNames.length; i++) {
        const name = dirNames[i];
        const isLast = i === dirNames.length - 1 && files.length === 0 && omitted === 0;
        const connector = isRoot ? '' : (isLast ? '\u2514\u2500 ' : '\u251c\u2500 ');
        const childIndent = isRoot ? '' : (indent + (isLast ? '   ' : '\u2502  '));
        lines.push(`${indent}${connector}${name}/`);
        renderTreeNode(lines, node.dirs[name], childIndent, false);
    }

    for (let i = 0; i < files.length; i++) {
        const isLast = i === files.length - 1 && omitted === 0;
        const connector = isRoot ? '' : (isLast ? '\u2514\u2500 ' : '\u251c\u2500 ');
        lines.push(`${indent}${connector}${files[i]}`);
    }

    if (omitted > 0) {
        const connector = isRoot ? '' : '\u2514\u2500 ';
        lines.push(`${indent}${connector}... ${omitted} more file${omitted !== 1 ? 's' : ''}`);
    }
}

// No explicit registration -- renderFolderTree is used as the fallback
// in renderComponentContent when no other components match.
