/**
 * app.js — GSCO Extraction Toolkit frontend logic
 * Handles both index.html (Control Panel) and results.html (Results Viewer)
 */

// ─────────────────────────────────────────────────────────────
// Shared state (persisted in sessionStorage)
// ─────────────────────────────────────────────────────────────

const CURRENT_JOB_KEY = 'gsco_job_id';
const CURRENT_FILE_KEY = 'gsco_filename';

function setJob(id) { sessionStorage.setItem(CURRENT_JOB_KEY, id); }
function getJob() { return sessionStorage.getItem(CURRENT_JOB_KEY); }
function setFile(f) { sessionStorage.setItem(CURRENT_FILE_KEY, f); }
function getFile() { return sessionStorage.getItem(CURRENT_FILE_KEY); }

// ─────────────────────────────────────────────────────────────
// Detect current page
// ─────────────────────────────────────────────────────────────

const IS_RESULTS = document.getElementById('factsTable') !== null;
const IS_INDEX = document.getElementById('runBtn') !== null;

document.addEventListener('DOMContentLoaded', () => {
    if (IS_INDEX) initControlPanel();
    if (IS_RESULTS) initResultsPage();
});

// ═════════════════════════════════════════════════════════════
//  CONTROL PANEL
// ═════════════════════════════════════════════════════════════

let uploadedFilename = null;
let sseSource = null;

const els = {
    file: document.getElementById('fileInput'),
    drop: document.getElementById('dropZone'),
    sel: document.getElementById('fileSelected'),
    name: document.getElementById('selectedFileName'),
    status: document.getElementById('uploadStatus'),
    model: document.getElementById('modelSelect'),
    obadge: document.getElementById('ollamaBadge'),
    obadgetxt: document.getElementById('ollamaBadgeText'),
    p_ext: document.getElementById('triagePrompt'),
    p_tbl: document.getElementById('graphPrompt'),
    run: document.getElementById('runBtn'),
    prog_sec: document.getElementById('progressSection'),
    prog_lbl: document.getElementById('progressLabel'),
    prog_pct: document.getElementById('progressPct'),
    prog_fill: document.getElementById('progressFill'),
    stat_facts: document.getElementById('statFacts'),
    stat_status: document.getElementById('statStatus'),
    log: document.getElementById('logConsole'),
    lines: document.getElementById('logLines'),
    done: document.getElementById('doneBanner'),
    done_sum: document.getElementById('doneSummary')
};

function initControlPanel() {
    setupDropZone();
    checkOllama();
    loadPrompts();

    // Restore filename if reloaded
    const saved = getFile();
    if (saved) showFileSelected(saved);
}

// ── Ollama ──────────────────────────────────────────────────

async function checkOllama() {
    const badge = document.getElementById('ollamaBadge');
    const badgeT = document.getElementById('ollamaBadgeText');
    const pulse = badge.querySelector('.pulse');
    const hint = document.getElementById('ollamaStatus');
    const select = document.getElementById('modelSelect');

    try {
        const res = await fetch('/api/models');
        const data = await res.json();

        badgeT.textContent = 'Connected';
        pulse.classList.add('connected');
        hint.textContent = `Ollama OK — ${data.models.length} model(s) found`;

        if (data.models.length > 0) {
            select.innerHTML = '';
            data.models.forEach(m => {
                const opt = document.createElement('option');
                opt.value = m; opt.textContent = m;
                if (m === data.default) opt.selected = true;
                select.appendChild(opt);
            });
        } else {
            // Ollama connected but no models listed — keep default
            const opt = document.createElement('option');
            opt.value = data.default || 'deepseek-v3.1:671b-cloud';
            opt.textContent = data.default || 'deepseek-v3.1:671b-cloud';
            select.innerHTML = '';
            select.appendChild(opt);
            hint.textContent = 'Ollama connected (no models listed — using default)';
        }
    } catch {
        badgeT.textContent = 'Unreachable';
        pulse.classList.add('error');
        hint.textContent = 'Cannot reach Ollama — make sure it is running on port 11434';
    }
}

// ── File upload ──────────────────────────────────────────────

function setupDropZone() {
    const zone = document.getElementById('dropZone');
    const input = document.getElementById('fileInput');

    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
        e.preventDefault(); zone.classList.remove('drag-over');
        const file = e.dataTransfer.files[0];
        if (file) uploadFile(file);
    });
    input.addEventListener('change', () => {
        if (input.files[0]) uploadFile(input.files[0]);
    });
}

async function uploadFile(file) {
    if (!file.name.endsWith('.pdf')) {
        showUploadStatus('Only PDF files are supported.', 'err'); return;
    }
    showUploadStatus('Uploading…', '');
    const fd = new FormData();
    fd.append('file', file);

    try {
        const res = await fetch('/api/upload', { method: 'POST', body: fd });
        const data = await res.json();
        if (!res.ok) { showUploadStatus(data.error || 'Upload failed', 'err'); return; }
        uploadedFilename = data.filename;
        setFile(data.filename);
        showFileSelected(data.filename);
        showUploadStatus('✓ Uploaded successfully', 'ok');
    } catch (e) {
        showUploadStatus('Network error: ' + e.message, 'err');
    }
}

function showFileSelected(name) {
    uploadedFilename = name;
    els.drop.querySelector('.drop-content').classList.add('hidden');
    els.sel.classList.remove('hidden');
    els.name.textContent = name;
}

function clearFile() {
    uploadedFilename = null;
    sessionStorage.removeItem(CURRENT_FILE_KEY);
    els.drop.querySelector('.drop-content').classList.remove('hidden');
    els.sel.classList.add('hidden');
    els.name.textContent = '—';
    els.file.value = '';
    hideUploadStatus();
}

function showUploadStatus(msg, cls) {
    els.status.textContent = msg;
    els.status.className = 'upload-status' + (cls ? ' ' + cls : '');
    els.status.classList.remove('hidden');
}
function hideUploadStatus() {
    els.status.classList.add('hidden');
}

// ── Prompts ──────────────────────────────────────────────────

async function loadPrompts() {
    try {
        const res = await fetch('/api/prompts');
        const cfg = await res.json();
        els.p_ext.value = cfg.triage_system || '';
        els.p_tbl.value = cfg.graph_system || '';
    } catch (e) {
        console.error('Failed to load prompts', e);
    }
}

async function savePrompts() {
    const btn = document.getElementById('savePromptsBtn');
    btn.textContent = 'Saving…';
    try {
        const cfg = {
            triage_system: els.p_ext.value,
            graph_system: els.p_tbl.value
        };
        await fetch('/api/prompts', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cfg)
        });
        btn.textContent = 'Saved ✓';
        setTimeout(() => btn.textContent = 'Save Prompts', 2000);
    } catch (e) {
        alert('Failed to save prompts: ' + e.message);
        btn.textContent = 'Save Prompts';
    }
}

async function resetPrompts() {
    if (!confirm('Reset UI prompts to system defaults?')) return;
    try {
        await fetch('/api/prompts/reset', { method: 'POST' });
        await loadPrompts();
    } catch (e) {
        alert('Failed to reset: ' + e.message);
    }
}

// ── Tabs ─────────────────────────────────────────────────────

function switchTab(btn, id) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + id).classList.add('active');
}

// ── Run pipeline ─────────────────────────────────────────────

async function runPipeline() {
    if (!uploadedFilename) {
        alert('Please upload a PDF first.');
        return;
    }

    const model = document.getElementById('modelSelect').value;
    const runBtn = document.getElementById('runBtn');

    runBtn.querySelector('.btn-text').textContent = 'Running…';
    runBtn.disabled = true;

    // Reset UI
    document.getElementById('progressSection').classList.remove('hidden');
    document.getElementById('logConsole').classList.remove('hidden');
    document.getElementById('doneBanner').classList.add('hidden');
    document.getElementById('logLines').innerHTML = '';
    setProgress(0, 'Starting…');

    // Close any previous SSE
    if (sseSource) { sseSource.close(); sseSource = null; }

    try {
        const res = await fetch('/api/run', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: uploadedFilename, model })
        });
        const data = await res.json();
        if (!res.ok) { alert(data.error || 'Failed to start'); runBtn.disabled = false; return; }

        const jobId = data.job_id;
        setJob(jobId);
        startSSE(jobId);
    } catch (e) {
        alert('Network error: ' + e.message);
        runBtn.disabled = false;
    }
}

function startSSE(jobId) {
    sseSource = new EventSource(`/api/status/${jobId}`);
    let totalFacts = 0;

    sseSource.onmessage = (e) => {
        const d = JSON.parse(e.data);

        if (d.log) {
            appendLog(d.log);
            if (d.log.includes('[EXTRACTION]')) setProgress(d.progress, 'Parsing results...');
            else if (d.log.includes('[TRIAGE]')) setProgress(d.progress, 'Waiting for LLM Graph Extractor (this may take a few minutes)...');
            else setProgress(d.progress, d.log);
        } else if (d.progress !== undefined && d.ping) {
            setProgress(d.progress); // just update bar width, leave text alone
        }

        if (d.total_facts !== undefined) {
            totalFacts = d.total_facts;
            document.getElementById('statFacts').textContent = totalFacts;
        }
        if (d.status) {
            document.getElementById('statStatus').textContent = d.status;
        }

        if (d.status === 'done') {
            sseSource.close();
            onPipelineDone(totalFacts);
        }
        if (d.status === 'error') {
            sseSource.close();
            appendLog('[FATAL] ' + (d.error || 'unknown error'));
            document.getElementById('runBtn').querySelector('.btn-text').textContent = 'Retry';
            document.getElementById('runBtn').disabled = false;
        }
    };

    sseSource.onerror = () => {
        appendLog('[WARN] SSE connection lost — polling job status…');
        sseSource.close();
        pollJobStatus(jobId);
    };
}

async function pollJobStatus(jobId) {
    const interval = setInterval(async () => {
        try {
            const res = await fetch(`/api/jobs/${jobId}`);
            const data = await res.json();
            setProgress(data.progress, '');
            document.getElementById('statFacts').textContent = data.total_facts;
            document.getElementById('statStatus').textContent = data.status;
            if (data.status === 'done') {
                clearInterval(interval);
                onPipelineDone(data.total_facts);
            }
            if (data.status === 'error') {
                clearInterval(interval);
                appendLog('[FATAL] ' + (data.error || 'unknown'));
                document.getElementById('runBtn').querySelector('.btn-text').textContent = 'Retry';
                document.getElementById('runBtn').disabled = false;
            }
        } catch { /* retry on next tick */ }
    }, 2000);
}

function onPipelineDone(totalFacts) {
    setProgress(100, 'Complete!');
    document.getElementById('doneBanner').classList.remove('hidden');
    document.getElementById('doneSummary').textContent =
        `Extracted ${totalFacts} facts. Ready to view and download.`;
    document.getElementById('runBtn').querySelector('.btn-text').textContent = 'Run Again';
    document.getElementById('runBtn').disabled = false;
    appendLog('[DONE] Pipeline finished successfully.');
}

function setProgress(pct, label) {
    els.prog_pct.textContent = pct + '%';
    els.prog_fill.style.width = pct + '%';
    if (label && label.trim() !== '') els.prog_lbl.textContent = label;
}

function appendLog(msg) {
    const container = document.getElementById('logLines');
    const div = document.createElement('div');
    div.className = 'log-line ' + logClass(msg);
    div.textContent = msg;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function logClass(msg) {
    if (!msg) return 'info';
    const m = msg.toLowerCase();
    if (m.includes('[done]') || m.includes('complete')) return 'done';
    if (m.includes('[error]') || m.includes('[fatal]') || m.includes('error')) return 'error';
    if (m.includes('fact')) return 'fact';
    return 'info';
}

function clearLog() {
    document.getElementById('logLines').innerHTML = '';
}

// ═════════════════════════════════════════════════════════════
//  RESULTS PAGE
// ═════════════════════════════════════════════════════════════

let allFacts = [];
let filtered = [];
let currentPage = 1;
const PAGE_SIZE = 25;

function initResultsPage() {
    const jobId = getJob();
    if (!jobId) {
        document.getElementById('loadingState').innerHTML =
            '<div class="empty-icon">⚠</div><p>No job found. <a href="/">Run an extraction first.</a></p>';
        return;
    }
    loadResults(jobId);
}

async function loadResults(jobId) {
    try {
        const res = await fetch(`/api/results/${jobId}`);
        const data = await res.json();

        if (!res.ok) {
            document.getElementById('loadingState').innerHTML =
                `<div class="empty-icon">⚠</div><p>${data.error || 'Results not ready yet.'} <a href="/">Return to Control Panel</a></p>`;
            return;
        }

        allFacts = data.facts || [];
        const doc = allFacts[0]?.document_name || '—';
        document.getElementById('resultsMeta').textContent =
            `${allFacts.length} facts extracted from "${doc}"`;

        if (data.summary || data.limitations) {
            document.getElementById('docInsight').classList.remove('hidden');
            document.getElementById('docSummary').textContent = data.summary || 'Not provided';
            document.getElementById('docLimitations').textContent = data.limitations || 'Not provided';
        }

        document.getElementById('loadingState').classList.add('hidden');
        document.getElementById('tableWrapper').classList.remove('hidden');

        buildFilterOptions();
        applyFilters();
    } catch (e) {
        document.getElementById('loadingState').innerHTML =
            `<div class="empty-icon">⚠</div><p>Error: ${e.message}</p>`;
    }
}

function buildFilterOptions() {
    const stages = [...new Set(allFacts.map(f => f.stage).filter(Boolean))].sort();
    const chains = [...new Set(allFacts.map(f => f.chain).filter(Boolean))].sort();
    const factTypes = [...new Set(allFacts.map(f => f.fact_type).filter(Boolean))].sort();

    populateSelect('filterStage', stages);
    populateSelect('filterChain', chains);
    populateSelect('filterFactType', factTypes);
}

function populateSelect(id, values) {
    const sel = document.getElementById(id);
    if (!sel) return;
    const first = sel.options[0];
    sel.innerHTML = '';
    sel.appendChild(first);
    values.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v; opt.textContent = v;
        sel.appendChild(opt);
    });
}

function applyFilters() {
    const stage = document.getElementById('filterStage').value;
    const chain = document.getElementById('filterChain').value;
    const factType = document.getElementById('filterFactType').value;
    const search = document.getElementById('searchBox').value.toLowerCase();

    filtered = allFacts.filter(f => {
        if (stage && f.stage !== stage) return false;
        if (chain && f.chain !== chain) return false;
        if (factType && f.fact_type !== factType) return false;
        if (search) {
            if (!JSON.stringify(f).toLowerCase().includes(search)) return false;
        }
        return true;
    });

    currentPage = 1;
    renderTable();
}

function clearFilters() {
    ['filterStage', 'filterChain', 'filterFactType'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.selectedIndex = 0;
    });
    document.getElementById('searchBox').value = '';
    applyFilters();
}

function renderTable() {
    const noRes = document.getElementById('noResults');
    const tbody = document.getElementById('factsBody');
    const pagDiv = document.getElementById('pagination');

    if (filtered.length === 0) {
        noRes.classList.remove('hidden');
        tbody.innerHTML = ''; pagDiv.innerHTML = '';
        return;
    }
    noRes.classList.add('hidden');

    const start = (currentPage - 1) * PAGE_SIZE;
    const slice = filtered.slice(start, start + PAGE_SIZE);

    tbody.innerHTML = slice.map((f, i) => {
        const idx = start + i + 1;
        const conf = typeof f.confidence === 'number' ? (f.confidence * 100).toFixed(0) + '%' : none();

        return `<tr>
      <td>${idx}</td>
      <td>${f.stage ? `<span class="badge badge-conf">${esc(f.stage)}</span>` : none()}</td>
      <td>${f.chain ? `<span class="badge badge-type">${esc(f.chain)}</span>` : none()}</td>
      <td>${esc(f.fact_type)}</td>
      <td class="multi-val"><strong>${esc(f.subject_name)}</strong><br><small>${esc(f.subject_type)}</small></td>
      <td><span class="badge badge-type">${esc(f.relation)}</span></td>
      <td class="multi-val"><strong>${esc(f.object_name)}</strong><br><small>${esc(f.object_type)}</small></td>
      <td>${conf}</td>
      <td class="multi-val" style="max-width: 200px; font-size: 0.8em;">${esc(f.evidence_text)}</td>
      <td>${esc(f.page_no)}</td>
    </tr>`;
    }).join('');

    renderPagination(pagDiv);
}

function none() {
    return '<span style="color:var(--text-dim)">—</span>';
}

function renderPagination(pagDiv) {
    const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
    if (totalPages <= 1) { pagDiv.innerHTML = ''; return; }

    let html = '';
    for (let p = 1; p <= totalPages; p++) {
        html += `<button class="page-btn ${p === currentPage ? 'active' : ''}"
               onclick="goPage(${p})">${p}</button>`;
    }
    pagDiv.innerHTML = html;
}

function goPage(p) {
    currentPage = p;
    renderTable();
    document.getElementById('tableWrapper').scrollIntoView({ behavior: 'smooth' });
}

function esc(v) {
    if (v == null || v === '' || v === 'null') return none();
    return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function downloadCSV() {
    const jobId = getJob();
    if (!jobId) { alert('No job found.'); return; }
    window.location.href = `/api/results/${jobId}/csv`;
}
