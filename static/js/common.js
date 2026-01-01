/* DQX Common JavaScript Functions */

// Global state
let currentSampleData = null;

// Utility Functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatTimestamp(ts) {
    if (!ts) return '-';
    try {
        const date = new Date(parseInt(ts));
        if (isNaN(date.getTime())) return ts;
        return date.toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    } catch (e) {
        return ts;
    }
}

function showStatus(elementId, message, type) {
    const statusBar = document.getElementById(elementId);
    if (statusBar) {
        statusBar.className = 'status-bar ' + type;
        statusBar.textContent = message;
    }
}

function toggleSection(sectionId) {
    const section = document.getElementById(sectionId);
    const chevron = document.getElementById(sectionId.replace('-section', '-chevron'));

    if (section) section.classList.toggle('expanded');
    if (chevron) chevron.classList.toggle('rotated');
}

// Catalog/Schema/Table Loading Functions
async function loadCatalogs() {
    const select = document.getElementById('catalog');
    if (!select) return;

    try {
        const response = await fetch('/api/catalogs');
        const catalogs = await response.json();
        select.innerHTML = '<option value="">Select a catalog</option>' +
            catalogs.map(c => `<option value="${c}">${c}</option>`).join('');
    } catch (e) {
        select.innerHTML = '<option value="">Error loading catalogs</option>';
    }
}

async function loadSchemas() {
    const catalog = document.getElementById('catalog').value;
    const schemaSelect = document.getElementById('schema');
    const tableSelect = document.getElementById('table');

    if (!catalog) {
        schemaSelect.disabled = true;
        schemaSelect.innerHTML = '<option value="">Select catalog first</option>';
        tableSelect.disabled = true;
        tableSelect.innerHTML = '<option value="">Select schema first</option>';
        return;
    }

    schemaSelect.innerHTML = '<option value="">Loading...</option>';
    schemaSelect.disabled = true;

    try {
        const response = await fetch(`/api/schemas/${catalog}`);
        const schemas = await response.json();
        schemaSelect.innerHTML = '<option value="">Select a schema</option>' +
            schemas.map(s => `<option value="${s}">${s}</option>`).join('');
        schemaSelect.disabled = false;
    } catch (e) {
        schemaSelect.innerHTML = '<option value="">Error loading schemas</option>';
    }

    tableSelect.disabled = true;
    tableSelect.innerHTML = '<option value="">Select schema first</option>';

    if (typeof hideSampleSection === 'function') hideSampleSection();
}

async function loadTables() {
    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const tableSelect = document.getElementById('table');

    if (!schema) {
        tableSelect.disabled = true;
        tableSelect.innerHTML = '<option value="">Select schema first</option>';
        return;
    }

    tableSelect.innerHTML = '<option value="">Loading...</option>';
    tableSelect.disabled = true;

    try {
        const response = await fetch(`/api/tables/${catalog}/${schema}`);
        const tables = await response.json();
        tableSelect.innerHTML = '<option value="">Select a table</option>' +
            tables.map(t => `<option value="${t}">${t}</option>`).join('');
        tableSelect.disabled = false;
    } catch (e) {
        tableSelect.innerHTML = '<option value="">Error loading tables</option>';
    }

    if (typeof hideSampleSection === 'function') hideSampleSection();
}

async function loadSampleData() {
    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const table = document.getElementById('table').value;

    if (!table) {
        if (typeof hideSampleSection === 'function') hideSampleSection();
        return;
    }

    const sampleSection = document.getElementById('sample-section');
    if (sampleSection) sampleSection.classList.remove('hidden');

    const colCount = document.getElementById('col-count');
    const rowCount = document.getElementById('row-count');
    const tableName = document.getElementById('table-name');

    if (colCount) colCount.textContent = '...';
    if (rowCount) rowCount.textContent = '...';
    if (tableName) tableName.textContent = table;

    try {
        const response = await fetch(`/api/sample/${catalog}/${schema}/${table}`);
        const data = await response.json();
        currentSampleData = data;

        if (colCount) colCount.textContent = data.columns.length;
        if (rowCount) rowCount.textContent = data.row_count;

        const thead = document.querySelector('#data-table thead');
        const tbody = document.querySelector('#data-table tbody');

        if (thead && tbody) {
            thead.innerHTML = '<tr>' + data.columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
            tbody.innerHTML = data.rows.map(row =>
                '<tr>' + data.columns.map(c => `<td>${row[c] !== null ? row[c] : '<em>null</em>'}</td>`).join('') + '</tr>'
            ).join('');
        }

        const columnInfo = document.getElementById('column-info');
        if (columnInfo) {
            columnInfo.textContent = `Available columns: ${data.columns.join(', ')}`;
        }

        // Enable generate section if it exists
        const generateSection = document.getElementById('generate-section');
        const generateBtn = document.getElementById('generate-btn');
        if (generateSection) generateSection.classList.remove('hidden');
        if (generateBtn) generateBtn.disabled = false;

        // Call page-specific callback if defined
        if (typeof onSampleDataLoaded === 'function') {
            onSampleDataLoaded(data);
        }

    } catch (e) {
        if (colCount) colCount.textContent = 'Error';
        if (rowCount) rowCount.textContent = '-';
    }
}

// Markdown to HTML formatter
function formatMarkdownToHtml(text) {
    if (!text) return '';

    let html = escapeHtml(text);

    // Convert **bold** to <strong>
    html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    // Convert *italic* to <em>
    html = html.replace(/\*([^*]+)\*/g, '<em>$1</em>');

    // Convert `code` to <code>
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Convert numbered lists (1. item)
    html = html.replace(/^(\d+)\.\s+(.+)$/gm, '<li>$2</li>');

    // Convert bullet points (- item or • item)
    html = html.replace(/^[-•]\s+(.+)$/gm, '<li>$1</li>');

    // Wrap consecutive <li> elements in <ul>
    html = html.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');

    // Convert line breaks to <br> for remaining text
    html = html.replace(/\n\n/g, '</p><p>');
    html = html.replace(/\n/g, '<br>');

    // Wrap in paragraph if not already wrapped
    if (!html.startsWith('<')) {
        html = '<p>' + html + '</p>';
    }

    return html;
}
