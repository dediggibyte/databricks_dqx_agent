/* DQX Rule Validator JavaScript */

// Validator-specific state
let selectedRules = null;
let validationResults = null;

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadCatalogs();
});

function hideSampleSection() {
    const sampleSection = document.getElementById('sample-section');
    const rulesSection = document.getElementById('rules-section');
    const validationSection = document.getElementById('validation-section');

    if (sampleSection) sampleSection.classList.add('hidden');
    if (rulesSection) rulesSection.classList.add('hidden');
    if (validationSection) validationSection.classList.add('hidden');
}

// Callback when sample data is loaded
function onSampleDataLoaded() {
    const rulesSection = document.getElementById('rules-section');
    if (rulesSection) rulesSection.classList.remove('hidden');
    loadRuleVersions();
}

// Load available rule versions for the selected table
async function loadRuleVersions() {
    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const table = document.getElementById('table').value;
    const fullTableName = `${catalog}.${schema}.${table}`;

    const versionSelect = document.getElementById('rule-version');
    versionSelect.innerHTML = '<option value="">Loading versions...</option>';
    versionSelect.disabled = true;

    try {
        const response = await fetch(`/api/history/${encodeURIComponent(fullTableName)}`);
        const result = await response.json();

        if (result.success && result.history && result.history.length > 0) {
            versionSelect.innerHTML = '<option value="">Select a version</option>' +
                result.history.map(v => {
                    const rulesCount = Array.isArray(v.rules) ? v.rules.length : 0;
                    const label = `v${v.version} - ${rulesCount} rules (${v.created_at || 'Unknown date'})`;
                    return `<option value="${v.version}" data-rules='${JSON.stringify(v.rules)}'>${label}</option>`;
                }).join('');
            versionSelect.disabled = false;
        } else {
            versionSelect.innerHTML = '<option value="">No rules found for this table</option>';
            showStatus('rules-status', 'No DQ rules found for this table. Generate rules first using the Generator tab.', 'info');
        }
    } catch (e) {
        versionSelect.innerHTML = '<option value="">Error loading versions</option>';
        showStatus('rules-status', `Error loading rule versions: ${e.message}`, 'error');
    }
}

// When a version is selected, show the rules
function onVersionSelected() {
    const versionSelect = document.getElementById('rule-version');
    const selectedOption = versionSelect.options[versionSelect.selectedIndex];

    if (!selectedOption || !selectedOption.value) {
        document.getElementById('rules-preview').classList.add('hidden');
        document.getElementById('validate-btn').disabled = true;
        selectedRules = null;
        return;
    }

    try {
        selectedRules = JSON.parse(selectedOption.getAttribute('data-rules'));
        displayRulesPreview(selectedRules);
        document.getElementById('rules-preview').classList.remove('hidden');
        document.getElementById('validation-section').classList.remove('hidden');
        document.getElementById('validate-btn').disabled = false;
    } catch (e) {
        showStatus('rules-status', 'Error parsing rules data', 'error');
    }
}

function displayRulesPreview(rules) {
    const container = document.getElementById('rules-preview-content');

    if (!rules || rules.length === 0) {
        container.innerHTML = '<p>No rules in this version.</p>';
        return;
    }

    let html = `<p><strong>${rules.length} rules</strong> will be validated:</p>`;
    html += '<div class="table-container"><table class="data-table"><thead><tr><th>#</th><th>Check Function</th><th>Column</th><th>Criticality</th></tr></thead><tbody>';

    rules.forEach((rule, index) => {
        const checkFn = rule.check?.function || rule.name || '-';
        const args = rule.check?.arguments || {};
        const column = args.col_name || (args.col_names ? args.col_names.join(', ') : '-');
        const criticality = rule.criticality || '-';

        html += `<tr>
            <td>${index + 1}</td>
            <td><code>${escapeHtml(checkFn)}</code></td>
            <td>${escapeHtml(column)}</td>
            <td><span class="criticality-badge ${criticality.toLowerCase()}">${escapeHtml(criticality)}</span></td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// Trigger validation job
async function runValidation() {
    if (!selectedRules) {
        showStatus('validation-status', 'Please select a rule version first', 'error');
        return;
    }

    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const table = document.getElementById('table').value;
    const fullTableName = `${catalog}.${schema}.${table}`;

    const btn = document.getElementById('validate-btn');
    btn.disabled = true;
    btn.innerHTML = '<span class="loading"></span>Triggering validation job...';

    showStatus('validation-status', 'Triggering DQ rule validation job...', 'info');
    document.getElementById('validation-section').classList.remove('hidden');
    document.getElementById('validation-results').classList.add('hidden');

    try {
        const response = await fetch('/api/validate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                table_name: fullTableName,
                rules: selectedRules
            })
        });

        const result = await response.json();

        if (result.error) {
            showStatus('validation-status', `Error: ${result.error}`, 'error');
            btn.disabled = false;
            btn.textContent = 'Run Validation';
            return;
        }

        const runId = result.run_id;
        showStatus('validation-status', `Job triggered! Run ID: ${runId}. Waiting for completion...`, 'info');

        // Poll for completion
        let attempts = 0;
        const maxAttempts = 120;

        const pollInterval = setInterval(async () => {
            attempts++;

            try {
                const statusResponse = await fetch(`/api/validate/status/${runId}`);
                const status = await statusResponse.json();

                if (status.status === 'completed') {
                    clearInterval(pollInterval);
                    validationResults = status.result;
                    displayValidationResults(status.result);
                    showStatus('validation-status', 'Validation completed successfully!', 'success');
                    btn.disabled = false;
                    btn.textContent = 'Run Validation';
                } else if (status.status === 'failed' || status.status === 'error') {
                    clearInterval(pollInterval);
                    showStatus('validation-status', `Job failed: ${status.message}`, 'error');
                    btn.disabled = false;
                    btn.textContent = 'Run Validation';
                } else {
                    btn.innerHTML = `<span class="loading"></span>Validating... (${Math.round(attempts * 5 / 60)}m)`;
                }
            } catch (e) {
                // Continue polling
            }

            if (attempts >= maxAttempts) {
                clearInterval(pollInterval);
                showStatus('validation-status', `Job is taking longer than expected. Check Jobs UI for Run ID: ${runId}`, 'error');
                btn.disabled = false;
                btn.textContent = 'Run Validation';
            }
        }, 5000);

    } catch (e) {
        showStatus('validation-status', `Error: ${e.message}`, 'error');
        btn.disabled = false;
        btn.textContent = 'Run Validation';
    }
}

function displayValidationResults(results) {
    const container = document.getElementById('validation-results');
    container.classList.remove('hidden');

    // Summary stats
    const totalRules = results.total_rules || 0;
    const passed = results.passed || 0;
    const failed = results.failed || 0;
    const warnings = results.warnings || 0;

    document.getElementById('stat-total').textContent = totalRules;
    document.getElementById('stat-passed').textContent = passed;
    document.getElementById('stat-failed').textContent = failed;
    document.getElementById('stat-warnings').textContent = warnings;

    // Detail table
    const detailContainer = document.getElementById('validation-details');
    const ruleResults = results.rule_results || [];

    if (ruleResults.length === 0) {
        detailContainer.innerHTML = '<p>No detailed results available.</p>';
        return;
    }

    let html = '<table class="validation-detail-table"><thead><tr><th>Rule</th><th>Column</th><th>Status</th><th>Violations</th><th>Details</th></tr></thead><tbody>';

    ruleResults.forEach(r => {
        const statusClass = r.status === 'pass' ? 'status-pass' : (r.status === 'fail' ? 'status-fail' : 'status-warn');
        const statusText = r.status === 'pass' ? 'PASS' : (r.status === 'fail' ? 'FAIL' : 'WARN');

        html += `<tr>
            <td><code>${escapeHtml(r.rule_name || '-')}</code></td>
            <td>${escapeHtml(r.column || '-')}</td>
            <td class="${statusClass}">${statusText}</td>
            <td>${r.violation_count || 0}</td>
            <td>${escapeHtml(r.details || '-')}</td>
        </tr>`;
    });

    html += '</tbody></table>';
    detailContainer.innerHTML = html;
}

function downloadValidationReport() {
    if (!validationResults) {
        showStatus('validation-status', 'No validation results to download', 'error');
        return;
    }

    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const table = document.getElementById('table').value;

    const report = {
        table_name: `${catalog}.${schema}.${table}`,
        validated_at: new Date().toISOString(),
        summary: {
            total_rules: validationResults.total_rules,
            passed: validationResults.passed,
            failed: validationResults.failed,
            warnings: validationResults.warnings
        },
        rule_results: validationResults.rule_results
    };

    const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `dq_validation_${table}_${new Date().toISOString().slice(0,10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
}
