document.addEventListener('DOMContentLoaded', () => {
    let currentScenario = 1;

    const navBtns = document.querySelectorAll('.nav-btn');
    const runBtn = document.getElementById('run-sim-btn');
    const titleEl = document.getElementById('scenario-title');
    const loader = document.getElementById('loader');
    const uploadPanel = document.getElementById('upload-panel');
    const fileInput = document.getElementById('custom-file-input');

    // DOM Elements for Metrics
    const mLatency = document.getElementById('metric-latency');
    const mSeverity = document.getElementById('metric-severity');
    const mPipelines = document.getElementById('metric-pipelines');
    const mResolution = document.getElementById('metric-resolution');

    const schemaBefore = document.getElementById('schema-before');
    const schemaAfter = document.getElementById('schema-after');
    const logOutput = document.getElementById('log-output');

    // Nav Selection
    navBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            navBtns.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentScenario = e.target.dataset.scenario;
            if (currentScenario == 5) {
                uploadPanel.classList.remove('hidden');
                titleEl.textContent = 'Scenario 5: Custom Data Upload';
            } else {
                uploadPanel.classList.add('hidden');
                titleEl.textContent = e.target.textContent.replace(/^\d+\.\s*/, 'Scenario ' + currentScenario + ': ');
            }
            
            // clear state
            schemaBefore.innerHTML = '';
            schemaAfter.innerHTML = '';
            logOutput.textContent = 'Awaiting simulation...';
            mLatency.textContent = '-- ms';
            mSeverity.textContent = '--';
            mSeverity.className = '';
            mPipelines.textContent = '--';
            mResolution.textContent = '--';
        });
    });

    runBtn.addEventListener('click', async () => {
        if (currentScenario == 5 && !fileInput.files.length) {
            alert('Please select a file to upload first!');
            return;
        }

        runBtn.disabled = true;
        loader.classList.remove('hidden');

        try {
            let res;
            if (currentScenario == 5) {
                const formData = new FormData();
                formData.append('file', fileInput.files[0]);
                res = await fetch('/api/upload_dataset', {
                    method: 'POST',
                    body: formData
                });
            } else {
                res = await fetch(`/api/scenario/${currentScenario}`);
            }
            
            const data = await res.json();
            if (data.error) {
                alert("Error from server: " + data.error);
            } else {
                updateDashboard(data);
            }
        } catch (error) {
            console.error(error);
            logOutput.textContent = "Error running simulation. Is backend running?";
        } finally {
            runBtn.disabled = false;
            loader.classList.add('hidden');
        }
    });

    function updateDashboard(data) {
        titleEl.textContent = data.title;
        mLatency.textContent = `${data.elapsed_ms} ms`;
        
        let sevClass = "";
        let sev = data.impact.severity;
        if(sev === "LOW") sevClass = "severity-low";
        if(sev === "MEDIUM") sevClass = "severity-medium";
        if(sev === "HIGH") sevClass = "severity-high";
        
        mSeverity.textContent = sev || "N/A";
        mSeverity.className = sevClass;
        
        mPipelines.textContent = data.impact.impact_count !== undefined ? data.impact.impact_count : '--';
        mResolution.textContent = data.resolution;

        // Animate elements entering linearly instead of immediately
        renderSchema(schemaBefore, data.before);
        setTimeout(() => {
            renderDriftedSchema(schemaAfter, data.after, data.changes);
        }, 300);

        logOutput.textContent = JSON.stringify(data.changes, null, 2);
    }

    function renderSchema(container, fields) {
        container.innerHTML = fields.map(f => `
            <div class="field-item">
                <span>${f.name}</span>
                <span class="type">${f.dataType}</span>
            </div>
        `).join('');
    }

    function renderDriftedSchema(container, fields, changes) {
        // Build a map of column -> change type for highlights
        const changeMap = {};
        changes.forEach(c => {
            if(c.type === 'column_added') changeMap[c.column] = 'field-added';
            if(c.type === 'type_changed') changeMap[c.column] = 'field-changed';
        });

        const removed = changes.filter(c => c.type === 'column_removed');
        
        let html = fields.map(f => {
            const hlClass = changeMap[f.name] || '';
            return `
                <div class="field-item ${hlClass}">
                    <span>${f.name}</span>
                    <span class="type">${f.dataType}</span>
                </div>
            `;
        }).join('');

        removed.forEach(r => {
            html += `
                <div class="field-item field-removed">
                    <span>${r.column}</span>
                    <span class="type">Removed</span>
                </div>
            `;
        });

        container.innerHTML = html;
    }
});
