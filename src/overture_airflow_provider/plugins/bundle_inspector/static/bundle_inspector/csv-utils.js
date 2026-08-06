// Shared CSV parsing utilities.
// RFC 4180-aware row splitter and full-text parser.

// Split a single CSV row (already extracted by splitCsvLines) into fields,
// handling quoted fields with embedded commas and escaped quotes (double-quote pairs).
function splitCsvRow(line) {
    const fields = [];
    let i = 0;
    while (i <= line.length) {
        if (i === line.length) { fields.push(''); break; }
        if (line[i] === '"') {
            let val = '';
            i++; // skip opening quote
            while (i < line.length) {
                if (line[i] === '"') {
                    if (i + 1 < line.length && line[i + 1] === '"') {
                        val += '"';
                        i += 2;
                    } else {
                        i++; // skip closing quote
                        break;
                    }
                } else {
                    val += line[i++];
                }
            }
            fields.push(val);
            if (i < line.length && line[i] === ',') i++; // skip delimiter
        } else {
            const next = line.indexOf(',', i);
            if (next === -1) {
                fields.push(line.slice(i));
                break;
            }
            fields.push(line.slice(i, next));
            i = next + 1;
        }
    }
    return fields;
}

// Split CSV text into rows, respecting quoted fields that contain newlines.
// Returns an array of parsed row arrays (each row is an array of field strings).
function splitCsvLines(text) {
    const rows = [];
    let i = 0;
    const len = text.length;
    while (i < len) {
        // Skip leading CRLF/LF left over from previous row
        if (text[i] === '\r' || text[i] === '\n') { i++; continue; }
        let lineStart = i;
        let inQuote = false;
        while (i < len) {
            const ch = text[i];
            if (ch === '"') {
                if (inQuote && i + 1 < len && text[i + 1] === '"') {
                    i += 2; continue;
                }
                inQuote = !inQuote;
            } else if ((ch === '\n' || ch === '\r') && !inQuote) {
                break;
            }
            i++;
        }
        const line = text.slice(lineStart, i);
        if (line.length > 0) rows.push(splitCsvRow(line));
        if (i < len && text[i] === '\r') i++;
        if (i < len && text[i] === '\n') i++;
    }
    return rows;
}

// Parse a complete CSV text block into {columns, rows}.
function parseCsv(text) {
    const allRows = splitCsvLines(text);
    if (allRows.length === 0) return { columns: [], rows: [] };
    return { columns: allRows[0], rows: allRows.slice(1) };
}
