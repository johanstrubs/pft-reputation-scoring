const API_BASE = window.location.origin;
const REFRESH_INTERVAL = 5 * 60 * 1000; // 5 minutes

const SCORE_THRESHOLDS = { green: 70, yellow: 40 };

let scoresData = null;
let trendsData = null;
let sortKey = 'composite_score';
let sortAsc = false;

// --- Helpers ---

function healthTier(score) {
    if (score >= SCORE_THRESHOLDS.green) return 'green';
    if (score >= SCORE_THRESHOLDS.yellow) return 'yellow';
    return 'red';
}

function healthLabel(score) {
    if (score >= SCORE_THRESHOLDS.green) return 'Healthy';
    if (score >= SCORE_THRESHOLDS.yellow) return 'Degraded';
    return 'Poor';
}

function truncateKey(key) {
    if (!key) return '';
    return key.slice(0, 8) + '...' + key.slice(-6);
}

function fmt(val, decimals = 1) {
    if (val === null || val === undefined) return '-';
    return typeof val === 'number' ? val.toFixed(decimals) : val;
}

function fmtPct(val) {
    if (val === null || val === undefined) return '-';
    return (val * 100).toFixed(1) + '%';
}

function fmtUptime(seconds) {
    if (seconds === null || seconds === undefined) return '-';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    return `${days}d ${hours}h`;
}

function timeAgo(isoStr) {
    if (!isoStr) return '';
    const diff = Date.now() - new Date(isoStr).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
}

// --- Sparkline SVG ---

function sparklineSVG(points, width = 110, height = 28) {
    if (!points || points.length < 2) {
        return `<svg width="${width}" height="${height}"><text x="${width/2}" y="${height/2 + 4}" text-anchor="middle" fill="#8b949e" font-size="10">No data</text></svg>`;
    }

    const scores = points.map(p => p.composite_score);
    const min = Math.min(...scores);
    const max = Math.max(...scores);
    const range = max - min || 1;
    const pad = 2;

    const coords = scores.map((s, i) => {
        const x = pad + (i / (scores.length - 1)) * (width - 2 * pad);
        const y = pad + (1 - (s - min) / range) * (height - 2 * pad);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    const last = scores[scores.length - 1];
    const first = scores[0];
    const color = last >= first ? '#3fb950' : '#f85149';

    return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
        <polyline points="${coords.join(' ')}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="${coords[coords.length - 1].split(',')[0]}" cy="${coords[coords.length - 1].split(',')[1]}" r="2.5" fill="${color}"/>
    </svg>`;
}

// --- Larger trend chart for detail panel ---

function trendChartSVG(points, width = 600, height = 70) {
    if (!points || points.length < 2) {
        return `<svg width="100%" height="${height}" viewBox="0 0 ${width} ${height}"><text x="${width/2}" y="${height/2 + 4}" text-anchor="middle" fill="#8b949e" font-size="12">Insufficient historical data</text></svg>`;
    }

    const scores = points.map(p => p.composite_score);
    const min = Math.max(0, Math.min(...scores) - 5);
    const max = Math.min(100, Math.max(...scores) + 5);
    const range = max - min || 1;
    const pad = 4;

    const coords = scores.map((s, i) => {
        const x = pad + (i / (scores.length - 1)) * (width - 2 * pad);
        const y = pad + (1 - (s - min) / range) * (height - 2 * pad);
        return [x.toFixed(1), y.toFixed(1)];
    });

    const polyline = coords.map(c => c.join(',')).join(' ');
    const last = scores[scores.length - 1];
    const first = scores[0];
    const color = last >= first ? '#3fb950' : '#f85149';

    // Gradient fill
    const gradId = 'g' + Math.random().toString(36).slice(2, 8);
    const areaPath = `M${coords[0][0]},${height} ${coords.map(c => `L${c[0]},${c[1]}`).join(' ')} L${coords[coords.length-1][0]},${height} Z`;

    return `<svg width="100%" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <defs>
            <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="${color}" stop-opacity="0.3"/>
                <stop offset="100%" stop-color="${color}" stop-opacity="0"/>
            </linearGradient>
        </defs>
        <path d="${areaPath}" fill="url(#${gradId})"/>
        <polyline points="${polyline}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="${coords[coords.length-1][0]}" cy="${coords[coords.length-1][1]}" r="3" fill="${color}"/>
        <text x="${width - pad}" y="${pad + 10}" text-anchor="end" fill="${color}" font-size="11" font-weight="600">${last.toFixed(1)}</text>
    </svg>`;
}

// --- Score bar for sub-scores ---

function scoreBar(score, weight) {
    const pct = Math.min(100, Math.max(0, score * 100));
    const tier = healthTier(pct);
    const contribution = (score * weight * 100).toFixed(1);
    return { pct, tier, contribution };
}

// --- Data fetching ---

async function fetchScores() {
    const resp = await fetch(`${API_BASE}/api/scores`);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
}

async function fetchTrends() {
    const resp = await fetch(`${API_BASE}/api/scores/trends?hours=168`);
    if (!resp.ok) throw new Error(`Trends API error: ${resp.status}`);
    return resp.json();
}

// --- Rendering ---

function renderSummary(data) {
    const validators = data.validators;
    const count = validators.length;
    const avg = validators.reduce((s, v) => s + v.composite_score, 0) / count;
    const tiers = { green: 0, yellow: 0, red: 0 };
    const countries = new Set();
    const isps = new Set();

    validators.forEach(v => {
        tiers[healthTier(v.composite_score)]++;
        if (v.metrics.country) countries.add(v.metrics.country);
        if (v.metrics.isp) isps.add(v.metrics.isp);
    });

    document.getElementById('summary').innerHTML = `
        <div class="summary-card">
            <div class="label">Validators</div>
            <div class="value">${count}</div>
        </div>
        <div class="summary-card">
            <div class="label">Avg Score</div>
            <div class="value">${avg.toFixed(1)}</div>
        </div>
        <div class="summary-card">
            <div class="label">Healthy</div>
            <div class="value green">${tiers.green}</div>
        </div>
        <div class="summary-card">
            <div class="label">Degraded</div>
            <div class="value yellow">${tiers.yellow}</div>
        </div>
        <div class="summary-card">
            <div class="label">Poor</div>
            <div class="value red">${tiers.red}</div>
        </div>
        <div class="summary-card">
            <div class="label">Countries</div>
            <div class="value">${countries.size || '-'}</div>
        </div>
        <div class="summary-card">
            <div class="label">ISPs</div>
            <div class="value">${isps.size || '-'}</div>
        </div>
    `;

    document.getElementById('last-updated').textContent = `Last updated: ${timeAgo(data.timestamp)} | Round #${data.round_id}`;
}

function getSortedValidators(validators) {
    const sorted = [...validators];
    sorted.sort((a, b) => {
        let va, vb;
        if (sortKey === 'composite_score') {
            va = a.composite_score; vb = b.composite_score;
        } else if (sortKey === 'domain') {
            va = (a.domain || '').toLowerCase(); vb = (b.domain || '').toLowerCase();
            return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        } else if (sortKey === 'public_key') {
            va = a.public_key; vb = b.public_key;
            return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        } else if (sortKey === 'health') {
            va = a.composite_score; vb = b.composite_score;
        } else {
            va = a.composite_score; vb = b.composite_score;
        }
        return sortAsc ? va - vb : vb - va;
    });
    return sorted;
}

function renderTable(data) {
    const validators = getSortedValidators(data.validators);
    const trends = trendsData?.trends || {};
    const weights = { agreement_1h: 0.10, agreement_24h: 0.15, agreement_30d: 0.20, uptime: 0.15, latency: 0.10, peer_count: 0.10, version: 0.10, diversity: 0.10 };

    const sortArrow = (key) => {
        if (sortKey !== key) return '';
        return `<span class="sort-arrow">${sortAsc ? '\u25B2' : '\u25BC'}</span>`;
    };

    let html = `
        <thead>
            <tr>
                <th onclick="handleSort('composite_score')"># ${sortArrow('composite_score')}</th>
                <th onclick="handleSort('public_key')">Validator ${sortArrow('public_key')}</th>
                <th onclick="handleSort('composite_score')">Score ${sortArrow('composite_score')}</th>
                <th onclick="handleSort('health')">Health ${sortArrow('health')}</th>
                <th class="hide-mobile sparkline-cell">7d Trend</th>
            </tr>
        </thead>
        <tbody>
    `;

    validators.forEach((v, i) => {
        const rank = i + 1;
        const tier = healthTier(v.composite_score);
        const label = healthLabel(v.composite_score);
        const sparkData = trends[v.public_key] || [];
        const rankClass = rank <= 3 ? `rank-${rank}` : '';

        html += `
            <tr onclick="toggleDetail('${v.public_key}')">
                <td class="rank ${rankClass}">${rank}</td>
                <td>
                    <span class="validator-id">${truncateKey(v.public_key)}</span>
                    ${v.domain ? `<span class="validator-domain">${v.domain}</span>` : ''}
                </td>
                <td class="score-cell">${v.composite_score.toFixed(1)}</td>
                <td>
                    <span class="health-dot ${tier}"></span>
                    <span class="health-label ${tier}">${label}</span>
                </td>
                <td class="hide-mobile sparkline-cell">${sparklineSVG(sparkData)}</td>
            </tr>
            <tr class="detail-row" id="detail-${v.public_key}">
                <td colspan="5">${renderDetailPanel(v, sparkData, weights)}</td>
            </tr>
        `;
    });

    html += '</tbody>';
    document.getElementById('leaderboard-table').innerHTML = html;
}

function renderDetailPanel(v, sparkData, weights) {
    const m = v.metrics;
    const s = v.sub_scores;

    const metricRows = [
        { name: 'Agreement 1h', value: fmtPct(m.agreement_1h), score: s.agreement_1h, weight: weights.agreement_1h },
        { name: 'Agreement 24h', value: fmtPct(m.agreement_24h), score: s.agreement_24h, weight: weights.agreement_24h },
        { name: 'Agreement 30d', value: fmtPct(m.agreement_30d), score: s.agreement_30d, weight: weights.agreement_30d },
        { name: 'Uptime', value: m.uptime_pct !== null && m.uptime_pct !== undefined ? (m.uptime_pct * 100).toFixed(1) + '%' : fmtUptime(m.uptime_seconds), score: s.uptime, weight: weights.uptime },
        { name: 'Latency', value: m.latency_ms !== null ? fmt(m.latency_ms) + ' ms' : '-', score: s.latency, weight: weights.latency },
        { name: 'Peer Count', value: m.peer_count !== null ? m.peer_count : '-', score: s.peer_count, weight: weights.peer_count },
        { name: 'Server Version', value: m.server_version || '-', score: s.version, weight: weights.version },
        { name: 'Diversity (ASN)', value: m.asn ? `ASN ${m.asn}` : '-', score: s.diversity, weight: weights.diversity },
    ];

    let metricsHtml = metricRows.map(mr => {
        const bar = scoreBar(mr.score, mr.weight);
        return `
            <div class="metric-item">
                <div class="metric-header">
                    <span class="metric-name">${mr.name}</span>
                    <span class="metric-value">${mr.value} (${bar.contribution} pts)</span>
                </div>
                <div class="metric-bar">
                    <div class="metric-bar-fill ${bar.tier}" style="width: ${bar.pct}%"></div>
                </div>
            </div>
        `;
    }).join('');

    return `
        <div class="detail-panel">
            <div class="detail-section">
                <h3>Score Breakdown</h3>
                <div class="metrics-grid">${metricsHtml}</div>
            </div>
            <div class="detail-section">
                <h3>Validator Info</h3>
                <div class="info-grid">
                    <div><span class="info-label">Public Key:</span> <span class="info-value">${v.public_key}</span></div>
                    <div><span class="info-label">Server State:</span> <span class="info-value">${m.server_state || '-'}</span></div>
                    <div><span class="info-label">ISP:</span> <span class="info-value">${m.isp || '-'}</span></div>
                    <div><span class="info-label">Country:</span> <span class="info-value">${m.country || '-'}</span></div>
                    <div><span class="info-label">Ledger Interval:</span> <span class="info-value">${m.avg_ledger_interval ? fmt(m.avg_ledger_interval, 2) + 's' : '-'}</span></div>
                </div>
            </div>
            <div class="detail-section">
                <h3>7-Day Score Trend</h3>
                <div class="trend-chart-container">${trendChartSVG(sparkData)}</div>
            </div>
        </div>
    `;
}

// --- Interaction ---

function toggleDetail(pubkey) {
    const row = document.getElementById(`detail-${pubkey}`);
    if (row) {
        row.classList.toggle('open');
    }
}

function handleSort(key) {
    if (sortKey === key) {
        sortAsc = !sortAsc;
    } else {
        sortKey = key;
        sortAsc = key === 'public_key' || key === 'domain';
    }
    if (scoresData) renderTable(scoresData);
}

// Make handleSort globally accessible
window.handleSort = handleSort;
window.toggleDetail = toggleDetail;

// --- Init ---

async function init() {
    const loading = document.getElementById('loading');
    const errorBanner = document.getElementById('error-banner');

    try {
        [scoresData, trendsData] = await Promise.all([fetchScores(), fetchTrends()]);
        loading.style.display = 'none';
        renderSummary(scoresData);
        renderTable(scoresData);
    } catch (err) {
        loading.style.display = 'none';
        errorBanner.style.display = 'block';
        errorBanner.textContent = `Failed to load data: ${err.message}. Retrying in 30s...`;
        console.error(err);
        setTimeout(init, 30000);
        return;
    }

    // Auto-refresh
    setInterval(async () => {
        try {
            [scoresData, trendsData] = await Promise.all([fetchScores(), fetchTrends()]);
            renderSummary(scoresData);
            renderTable(scoresData);
            errorBanner.style.display = 'none';
        } catch (err) {
            console.error('Refresh failed:', err);
        }
    }, REFRESH_INTERVAL);
}

document.addEventListener('DOMContentLoaded', init);
