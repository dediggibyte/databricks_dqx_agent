/* DQX Rule Generator JavaScript */

// Generator-specific state
let currentRules = null;
let currentAnalysis = null;
let lakebaseConnected = false;

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadCatalogs();
});

function hideSampleSection() {
    const sampleSection = document.getElementById('sample-section');
    const generateSection = document.getElementById('generate-section');
    const resultsSection = document.getElementById('results-section');
    const confirmSection = document.getElementById('confirm-section');

    if (sampleSection) sampleSection.classList.add('hidden');
    if (generateSection) generateSection.classList.add('hidden');
    if (resultsSection) resultsSection.classList.add('hidden');
    if (confirmSection) confirmSection.classList.add('hidden');
}

async function generateRules() {
    const catalog = document.getElementById('catalog').value;
    const schema = document.getElementById('schema').value;
    const table = document.getElementById('table').value;
    const prompt = document.getElementById('prompt').value.trim();
    const sampleLimitInput = document.getElementById('sample-limit').value;

    if (!prompt) {
        showStatus('status-bar', 'Please enter your data quality requirements', 'error');
        return;
    }

    const fullTableName = `${catalog}.${schema}.${table}`;
    const btn = document.getElementById('generate-btn');
    btn.disabled = true;
    btn.innerHTML = '<span class="loading"></span>Triggering job...';

    showStatus('status-bar', 'Triggering DQ rule generation job...', 'info');

    // Build request body - only include sample_limit if specified
    const requestBody = {
        table_name: fullTableName,
        user_prompt: prompt
    };

    // If sample limit is specified, add it to the request
    if (sampleLimitInput && parseInt(sampleLimitInput) > 0) {
        requestBody.sample_limit = parseInt(sampleLimitInput);
    }

    try {
        const triggerResponse = await fetch('/api/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        const triggerResult = await triggerResponse.json();

        if (triggerResult.error) {
            showStatus('status-bar', `Error: ${triggerResult.error}`, 'error');
            btn.disabled = false;
            btn.textContent = 'Generate DQ Rules';
            return;
        }

        const runId = triggerResult.run_id;
        showStatus('status-bar', `Job triggered! Run ID: ${runId}. Waiting for completion...`, 'info');

        let attempts = 0;
        const maxAttempts = 120;

        const pollInterval = setInterval(async () => {
            attempts++;

            try {
                const statusResponse = await fetch(`/api/status/${runId}`);
                const status = await statusResponse.json();

                if (status.status === 'completed') {
                    clearInterval(pollInterval);
                    currentRules = status.result;
                    showResults(status.result);
                    showStatus('status-bar', 'DQ rules generated successfully!', 'success');
                    btn.disabled = false;
                    btn.textContent = 'Generate DQ Rules';
                } else if (status.status === 'failed' || status.status === 'error') {
                    clearInterval(pollInterval);
                    showStatus('status-bar', `Job failed: ${status.message}`, 'error');
                    btn.disabled = false;
                    btn.textContent = 'Generate DQ Rules';
                } else {
                    btn.innerHTML = `<span class="loading"></span>Running... (${Math.round(attempts * 5 / 60)}m)`;
                }
            } catch (e) {
                // Continue polling
            }

            if (attempts >= maxAttempts) {
                clearInterval(pollInterval);
                showStatus('status-bar', `Job is taking longer than expected. Check Jobs UI for Run ID: ${runId}`, 'error');
                btn.disabled = false;
                btn.textContent = 'Generate DQ Rules';
            }
        }, 5000);

    } catch (e) {
        showStatus('status-bar', `Error: ${e.message}`, 'error');
        btn.disabled = false;
        btn.textContent = 'Generate DQ Rules';
    }
}

function showResults(data) {
    const resultsSection = document.getElementById('results-section');
    resultsSection.classList.remove('hidden');

    showConfirmSection();

    // Populate metadata
    const metadata = data.metadata || {};
    document.getElementById('result-rules-count').textContent = metadata.rules_generated || (data.rules ? data.rules.length : 0);
    document.getElementById('result-row-count').textContent = (metadata.row_count || 0).toLocaleString();
    document.getElementById('result-col-count').textContent = metadata.column_count || 0;
    document.getElementById('result-timestamp').textContent = formatTimestamp(data.timestamp);
    document.getElementById('result-table-name').textContent = data.table_name || '-';
    document.getElementById('result-columns').textContent = (metadata.columns || []).join(', ') || '-';
    document.getElementById('result-prompt').textContent = data.user_prompt || '-';

    // Summary
    if (data.summary) {
        document.getElementById('result-summary').textContent = data.summary;
    }

    // Render rules in JSON editor
    renderRulesJson(data.rules || []);

    // Render column profiles
    renderProfiles(data.column_profiles || []);
}

function renderRulesJson(rules) {
    const editor = document.getElementById('rules-json-editor');
    const badge = document.getElementById('rule-count-badge');

    editor.value = JSON.stringify(rules, null, 2);
    badge.textContent = `${rules.length} rules`;
}

function formatJson() {
    const editor = document.getElementById('rules-json-editor');
    try {
        const parsed = JSON.parse(editor.value);
        editor.value = JSON.stringify(parsed, null, 2);
        showValidationMessage('JSON formatted successfully', true);
    } catch (e) {
        showValidationMessage(`Invalid JSON: ${e.message}`, false);
    }
}

function validateJson() {
    const editor = document.getElementById('rules-json-editor');
    try {
        const parsed = JSON.parse(editor.value);
        if (!Array.isArray(parsed)) {
            showValidationMessage('Rules must be an array', false);
            return false;
        }

        for (let i = 0; i < parsed.length; i++) {
            const rule = parsed[i];
            if (!rule.check || !rule.check.function) {
                showValidationMessage(`Rule ${i + 1}: Missing 'check.function' field`, false);
                return false;
            }
            if (!rule.criticality) {
                showValidationMessage(`Rule ${i + 1}: Missing 'criticality' field`, false);
                return false;
            }
        }

        showValidationMessage(`Valid JSON with ${parsed.length} rules`, true);
        document.getElementById('rule-count-badge').textContent = `${parsed.length} rules`;
        return true;
    } catch (e) {
        showValidationMessage(`Invalid JSON: ${e.message}`, false);
        return false;
    }
}

function showValidationMessage(message, isValid) {
    const msgDiv = document.getElementById('json-validation-message');
    msgDiv.textContent = message;
    msgDiv.className = 'json-validation-message ' + (isValid ? 'valid' : 'invalid');

    if (isValid) {
        setTimeout(() => {
            msgDiv.className = 'json-validation-message';
        }, 3000);
    }
}

function onRulesEdited() {
    try {
        const parsed = JSON.parse(document.getElementById('rules-json-editor').value);
        if (Array.isArray(parsed)) {
            currentRules.rules = parsed;
            document.getElementById('rule-count-badge').textContent = `${parsed.length} rules`;
        }
    } catch (e) {
        // Invalid JSON, don't update
    }
}

function renderProfiles(profiles) {
    const container = document.getElementById('profiles-container');

    if (!profiles || profiles.length === 0) {
        container.innerHTML = '<p style="color: #666;">No column profiles available.</p>';
        return;
    }

    let html = '<div class="table-container"><table class="data-table"><thead><tr><th>Type</th><th>Column</th><th>Description</th><th>Parameters</th></tr></thead><tbody>';

    profiles.forEach(profile => {
        let name = '', column = '', description = '', parameters = '';

        if (typeof profile === 'string') {
            const nameMatch = profile.match(/name='([^']+)'/);
            const colMatch = profile.match(/column='([^']+)'/);
            const descMatch = profile.match(/description='([^']+)'/);
            const paramsMatch = profile.match(/parameters=(\{[^}]+\}|None)/);

            name = nameMatch ? nameMatch[1] : '-';
            column = colMatch ? colMatch[1] : '-';
            description = descMatch ? descMatch[1] : '-';
            parameters = paramsMatch ? paramsMatch[1] : '-';
        } else if (typeof profile === 'object') {
            name = profile.name || '-';
            column = profile.column || '-';
            description = profile.description || '-';
            parameters = profile.parameters ? JSON.stringify(profile.parameters) : '-';
        }

        html += `<tr>
            <td><strong>${escapeHtml(name)}</strong></td>
            <td>${escapeHtml(column)}</td>
            <td>${escapeHtml(description)}</td>
            <td style="font-family: monospace; font-size: 11px;">${escapeHtml(parameters)}</td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

function downloadRules() {
    if (!currentRules) return;

    const table = document.getElementById('table').value;
    const blob = new Blob([JSON.stringify(currentRules, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `dq_rules_full_${table}_${new Date().toISOString().slice(0,10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
}

function downloadEditedRules() {
    const editor = document.getElementById('rules-json-editor');
    let rules;

    try {
        rules = JSON.parse(editor.value);
    } catch (e) {
        showValidationMessage('Cannot download: Invalid JSON in editor', false);
        return;
    }

    const exportData = {
        table_name: currentRules ? currentRules.table_name : '',
        rules: rules,
        exported_at: new Date().toISOString()
    };

    const table = document.getElementById('table').value;
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `dq_rules_${table}_${new Date().toISOString().slice(0,10)}.json`;
    a.click();
    URL.revokeObjectURL(url);
}

function resetForm() {
    document.getElementById('prompt').value = '';
    document.getElementById('results-section').classList.add('hidden');
    document.getElementById('confirm-section').classList.add('hidden');
    document.getElementById('status-bar').className = 'status-bar';
    currentRules = null;
    currentAnalysis = null;
}

// Step 4: Confirm and Save Functions
function showConfirmSection() {
    document.getElementById('confirm-section').classList.remove('hidden');
    checkLakebaseStatus();
}

async function checkLakebaseStatus() {
    const indicator = document.getElementById('lakebase-indicator');
    const statusText = document.getElementById('lakebase-status-text');

    indicator.className = 'status-indicator checking';
    statusText.textContent = 'Checking Lakebase connection...';

    try {
        const response = await fetch('/api/lakebase/status');
        const status = await response.json();

        if (status.connected) {
            indicator.className = 'status-indicator connected';
            statusText.textContent = `Connected to Lakebase (${status.host})`;
            lakebaseConnected = true;
            document.getElementById('save-btn').disabled = false;
        } else if (status.configured) {
            indicator.className = 'status-indicator disconnected';
            statusText.textContent = `Connection failed: ${status.error || status.message || 'Unknown error'}`;
            lakebaseConnected = false;
        } else {
            indicator.className = 'status-indicator disconnected';
            statusText.textContent = 'Lakebase not configured. Set LAKEBASE_HOST in app.yaml';
            lakebaseConnected = false;
        }
    } catch (e) {
        indicator.className = 'status-indicator disconnected';
        statusText.textContent = 'Error checking Lakebase status';
        lakebaseConnected = false;
    }
}

async function analyzeRules() {
    const btn = document.getElementById('analyze-btn');
    const statusBar = document.getElementById('analysis-status');

    let rules;
    try {
        rules = JSON.parse(document.getElementById('rules-json-editor').value);
    } catch (e) {
        statusBar.className = 'status-bar error';
        statusBar.textContent = 'Invalid JSON in rules editor. Please fix before analyzing.';
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<span class="loading"></span>Analyzing...';
    statusBar.className = 'status-bar info';
    statusBar.textContent = 'Analyzing DQ rules with AI...';

    try {
        const response = await fetch('/api/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                rules: rules,
                table_name: currentRules ? currentRules.table_name : '',
                user_prompt: currentRules ? currentRules.user_prompt : ''
            })
        });

        const result = await response.json();

        if (result.success) {
            currentAnalysis = result.analysis;
            displayAnalysisResults(result.analysis);
            statusBar.className = 'status-bar success';
            statusBar.textContent = 'Analysis complete!';

            if (lakebaseConnected) {
                document.getElementById('save-btn').disabled = false;
            }
        } else {
            statusBar.className = 'status-bar error';
            statusBar.textContent = `Analysis failed: ${result.error}`;
        }
    } catch (e) {
        statusBar.className = 'status-bar error';
        statusBar.textContent = `Error: ${e.message}`;
    }

    btn.disabled = false;
    btn.textContent = 'Analyze Rules with AI';
}

function displayAnalysisResults(analysis) {
    const resultsDiv = document.getElementById('analysis-results');
    resultsDiv.classList.remove('hidden');

    if (analysis.raw_response) {
        const formattedSummary = formatMarkdownToHtml(analysis.summary);
        document.getElementById('analysis-summary').innerHTML = `<div class="analysis-formatted">${formattedSummary}</div>`;
        document.getElementById('quality-score').textContent = '-';
        document.getElementById('coverage-assessment').innerHTML = '<em>See summary above for detailed analysis</em>';
        document.getElementById('rule-analysis-list').innerHTML = '<p style="color: var(--text-secondary);">Detailed rule-by-rule analysis included in summary above.</p>';
        document.getElementById('recommendations-list').innerHTML = '<li>See summary above for recommendations</li>';
        return;
    }

    const summaryText = analysis.summary || '-';
    document.getElementById('analysis-summary').innerHTML = `<div class="analysis-formatted">${formatMarkdownToHtml(summaryText)}</div>`;

    const score = analysis.overall_quality_score;
    document.getElementById('quality-score').textContent = score ? `${score}/10` : '-';

    const ruleAnalysisList = document.getElementById('rule-analysis-list');

    let actualRules = [];
    try {
        actualRules = JSON.parse(document.getElementById('rules-json-editor').value);
    } catch (e) {}

    if (analysis.rule_analysis && analysis.rule_analysis.length > 0) {
        ruleAnalysisList.innerHTML = analysis.rule_analysis.map((rule, index) => {
            let ruleName = rule.rule_function || rule.function || rule.name || rule.check || '';
            let ruleColumn = rule.column || rule.col_name || rule.field || '';
            let ruleCriticality = rule.criticality_justification || rule.criticality || '';

            if ((!ruleName || ruleName === '-') && actualRules[index]) {
                const actualRule = actualRules[index];
                ruleName = actualRule.check?.function || actualRule.name || '';
                ruleCriticality = ruleCriticality || actualRule.criticality || '';
            }

            if ((!ruleColumn || ruleColumn === '-') && actualRules[index]) {
                const actualRule = actualRules[index];
                const args = actualRule.check?.arguments || {};
                ruleColumn = args.col_name || (args.col_names ? args.col_names[0] : '') || '';
            }

            const ruleExplanation = rule.explanation || rule.description || rule.what_it_checks || '-';
            const ruleImportance = rule.importance || rule.why_important || rule.reason || '-';
            const displayRuleName = ruleName ? ruleName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()) : 'Rule ' + (index + 1);

            return `
            <div class="rule-analysis-item">
                <div class="rule-name">${escapeHtml(String(displayRuleName))}</div>
                ${ruleColumn ? `<div class="rule-column">Column: <code>${escapeHtml(String(ruleColumn))}</code></div>` : ''}
                <div class="rule-explanation">${formatMarkdownToHtml(String(ruleExplanation))}</div>
                <div class="rule-importance"><strong>Importance:</strong> ${formatMarkdownToHtml(String(ruleImportance))}</div>
                ${ruleCriticality ? `<div class="rule-criticality"><strong>Criticality:</strong> <span class="criticality-badge ${ruleCriticality.toLowerCase()}">${escapeHtml(String(ruleCriticality))}</span></div>` : ''}
            </div>
        `}).join('');
    } else if (actualRules.length > 0) {
        ruleAnalysisList.innerHTML = actualRules.map((rule, index) => {
            const ruleName = rule.check?.function || rule.name || 'Rule ' + (index + 1);
            const args = rule.check?.arguments || {};
            const ruleColumn = args.col_name || (args.col_names ? args.col_names.join(', ') : '');
            const displayRuleName = ruleName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

            return `
            <div class="rule-analysis-item">
                <div class="rule-name">${escapeHtml(displayRuleName)}</div>
                ${ruleColumn ? `<div class="rule-column">Column: <code>${escapeHtml(ruleColumn)}</code></div>` : ''}
                <div class="rule-criticality"><strong>Criticality:</strong> <span class="criticality-badge ${(rule.criticality || '').toLowerCase()}">${escapeHtml(rule.criticality || 'N/A')}</span></div>
            </div>
        `}).join('');
    } else {
        ruleAnalysisList.innerHTML = '<p style="color: var(--text-secondary);">No detailed rule analysis available.</p>';
    }

    const coverageText = analysis.coverage_assessment || '-';
    document.getElementById('coverage-assessment').innerHTML = `<div class="analysis-formatted">${formatMarkdownToHtml(coverageText)}</div>`;

    const recommendationsList = document.getElementById('recommendations-list');
    if (analysis.recommendations && analysis.recommendations.length > 0) {
        recommendationsList.innerHTML = analysis.recommendations.map(rec => {
            let recText = '';
            if (typeof rec === 'string') {
                recText = rec;
            } else if (typeof rec === 'object' && rec !== null) {
                recText = rec.text || rec.recommendation || rec.description || rec.message || rec.content || JSON.stringify(rec);
            } else {
                recText = String(rec);
            }
            return `<li>${formatMarkdownToHtml(recText)}</li>`;
        }).join('');
    } else {
        recommendationsList.innerHTML = '<li>No additional recommendations.</li>';
    }
}

async function confirmAndSave() {
    const btn = document.getElementById('save-btn');
    const statusBar = document.getElementById('save-status');

    let rules;
    try {
        rules = JSON.parse(document.getElementById('rules-json-editor').value);
    } catch (e) {
        statusBar.className = 'status-bar error';
        statusBar.textContent = 'Invalid JSON in rules editor. Please fix before saving.';
        return;
    }

    if (!currentRules || !currentRules.table_name) {
        statusBar.className = 'status-bar error';
        statusBar.textContent = 'No table name available. Please generate rules first.';
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<span class="loading"></span>Saving...';
    statusBar.className = 'status-bar info';
    statusBar.textContent = 'Saving rules to Lakebase...';

    try {
        const response = await fetch('/api/confirm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                rules: rules,
                table_name: currentRules.table_name,
                user_prompt: currentRules.user_prompt || '',
                ai_summary: currentAnalysis,
                metadata: currentRules.metadata || {}
            })
        });

        const result = await response.json();

        if (result.success) {
            statusBar.className = 'status-bar success';
            statusBar.textContent = 'Rules saved successfully!';

            document.getElementById('save-result').classList.remove('hidden');
            document.getElementById('saved-version').textContent = `v${result.version}`;
            document.getElementById('saved-at').textContent = result.created_at || new Date().toISOString();
            document.getElementById('saved-id').textContent = result.id;
        } else {
            statusBar.className = 'status-bar error';
            statusBar.textContent = `Save failed: ${result.error}`;
        }
    } catch (e) {
        statusBar.className = 'status-bar error';
        statusBar.textContent = `Error: ${e.message}`;
    }

    btn.disabled = false;
    btn.textContent = 'Confirm & Save Rules';
}

async function loadHistory() {
    const container = document.getElementById('history-container');

    if (!currentRules || !currentRules.table_name) {
        container.innerHTML = '<p style="color: #c5221f;">No table selected. Generate rules first.</p>';
        return;
    }

    container.innerHTML = '<p>Loading history...</p>';

    try {
        const response = await fetch(`/api/history/${encodeURIComponent(currentRules.table_name)}`);
        const result = await response.json();

        if (result.success && result.history.length > 0) {
            let html = `
                <table class="history-table">
                    <thead>
                        <tr>
                            <th>Version</th>
                            <th>Created At</th>
                            <th>Rules</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            result.history.forEach(item => {
                const rulesCount = Array.isArray(item.rules) ? item.rules.length : 0;
                const statusBadge = item.is_active
                    ? '<span class="version-badge active">Active</span>'
                    : '<span class="version-badge">Archived</span>';

                html += `
                    <tr>
                        <td>v${item.version}</td>
                        <td>${item.created_at || '-'}</td>
                        <td>${rulesCount} rules</td>
                        <td>${statusBadge}</td>
                    </tr>
                `;
            });

            html += '</tbody></table>';
            container.innerHTML = html;
        } else if (result.success) {
            container.innerHTML = '<p style="color: #666;">No version history found for this table.</p>';
        } else {
            container.innerHTML = `<p style="color: #c5221f;">Error: ${result.error}</p>`;
        }
    } catch (e) {
        container.innerHTML = `<p style="color: #c5221f;">Error loading history: ${e.message}</p>`;
    }
}
