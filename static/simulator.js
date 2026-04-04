const API_BASE = window.location.origin;

const adjustableMetrics = [
    {
        key: "agreement_1h",
        label: "Agreement 1h",
        inputType: "percent",
        min: 0,
        max: 100,
        step: 0.1,
        note: "Backend scorer: below 80% gets 0 score, 100% gets full credit.",
    },
    {
        key: "agreement_24h",
        label: "Agreement 24h",
        inputType: "percent",
        min: 0,
        max: 100,
        step: 0.1,
        note: "This is one of the heaviest weighted metrics in the live scorer.",
    },
    {
        key: "agreement_30d",
        label: "Agreement 30d",
        inputType: "percent",
        min: 0,
        max: 100,
        step: 0.1,
        note: "Long-term reliability carries the highest single weight.",
    },
    {
        key: "uptime_pct",
        label: "Uptime",
        inputType: "percent",
        min: 0,
        max: 100,
        step: 0.1,
        note: "Simulated as a percentage of the current cohort max uptime.",
    },
    {
        key: "latency_ms",
        label: "Latency",
        inputType: "number",
        min: 1,
        max: 600,
        step: 1,
        note: "50ms or lower gets full credit. 500ms or higher scores zero.",
    },
    {
        key: "peer_count",
        label: "Peer Count",
        inputType: "number",
        min: 0,
        max: 25,
        step: 1,
        note: "10 peers or more gets full score in the current scorer.",
    },
];

const idealTargets = {
    agreement_1h: 100,
    agreement_24h: 100,
    agreement_30d: 100,
    uptime_pct: 100,
    latency_ms: 50,
    peer_count: 10,
};

const state = {
    data: null,
    methodology: null,
    selected: null,
    controls: {},
    cohort: null,
};

function $(id) {
    return document.getElementById(id);
}

function truncateKey(key) {
    if (!key) return "";
    return `${key.slice(0, 10)}...${key.slice(-6)}`;
}

function displayName(validator) {
    return validator.domain || truncateKey(validator.public_key);
}

function fmtNumber(value, decimals = 1) {
    if (value === null || value === undefined || Number.isNaN(value)) return "-";
    return Number(value).toFixed(decimals);
}

function fmtMetric(key, value) {
    if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
    if (key.startsWith("agreement_")) return `${(value * 100).toFixed(1)}%`;
    if (key === "uptime_pct") return `${Number(value).toFixed(1)}%`;
    if (key === "latency_ms") return `${Number(value).toFixed(0)}ms`;
    if (key === "peer_count") return `${Math.round(Number(value))}`;
    return `${value}`;
}

function fmtControlValue(metric, value) {
    if (value === null || value === undefined || Number.isNaN(value)) return "-";
    if (metric.inputType === "percent") return `${Number(value).toFixed(1)}%`;
    if (metric.key === "latency_ms") return `${Number(value).toFixed(0)}ms`;
    return `${Math.round(Number(value))}`;
}

function scoreAgreement(value, total) {
    if (value === null || value === undefined) return 0.0;
    if (total !== null && total !== undefined && total === 0) return 0.5;
    if (value < 0.8) return 0.0;
    return Math.min(1.0, Math.max(0.0, (value - 0.8) / 0.2));
}

function scoreUptime(seconds, maxUptime) {
    if (seconds === null || seconds === undefined) return 0.0;
    return Math.min(1.0, seconds / maxUptime);
}

function scoreLatency(ms) {
    if (ms === null || ms === undefined) return 0.5;
    if (ms <= 50) return 1.0;
    if (ms >= 500) return 0.0;
    return 1.0 - (ms - 50) / 450;
}

function scorePeerCount(count) {
    if (count === null || count === undefined) return 0.5;
    if (count >= 10) return 1.0;
    if (count < 3) return 0.0;
    return (count - 3) / 7;
}

function determineLatestVersion(validators) {
    const counts = new Map();
    validators.forEach((validator) => {
        const version = validator.metrics.server_version;
        if (!version) return;
        counts.set(version, (counts.get(version) || 0) + 1);
    });
    let best = null;
    let bestCount = -1;
    for (const [version, count] of counts.entries()) {
        if (count > bestCount) {
            best = version;
            bestCount = count;
        }
    }
    return best;
}

function scoreVersion(version, latestVersion) {
    if (!version || !latestVersion) return 0.5;
    if (version === latestVersion) return 1.0;
    try {
        const versionParts = version.split(".").map((part) => parseInt(part, 10));
        const latestParts = latestVersion.split(".").map((part) => parseInt(part, 10));
        if (
            versionParts.length >= 2 &&
            latestParts.length >= 2 &&
            versionParts[0] === latestParts[0] &&
            latestParts[1] - versionParts[1] === 1
        ) {
            return 0.8;
        }
    } catch (_) {
        return 0.5;
    }
    return 0.5;
}

function countAsns(validators) {
    const counts = new Map();
    validators.forEach((validator) => {
        const asn = validator.metrics.asn;
        if (asn === null || asn === undefined) return;
        counts.set(asn, (counts.get(asn) || 0) + 1);
    });
    return counts;
}

function scoreDiversity(asn, asnCounts, totalWithAsn) {
    if (asn === null || asn === undefined || totalWithAsn === 0) return 0.5;
    const concentration = (asnCounts.get(asn) || 0) / totalWithAsn;
    if (concentration > 0.30) {
        return Math.max(0.0, 0.5 * (1.0 - (concentration - 0.30) / 0.70));
    }
    return 1.0 - (concentration / 0.30) * 0.5;
}

function scorePollSuccess(pct) {
    if (pct === null || pct === undefined) return 0.5;
    if (pct >= 95.0) return 1.0;
    if (pct < 70.0) return 0.0;
    return (pct - 70.0) / 25.0;
}

function buildCohort(validators) {
    const maxUptime = validators.reduce((max, validator) => {
        return Math.max(max, validator.metrics.uptime_seconds || 0);
    }, 1) || 1;
    const latestVersion = determineLatestVersion(validators);
    const asnCounts = countAsns(validators);
    const totalWithAsn = validators.filter((validator) => validator.metrics.asn !== null && validator.metrics.asn !== undefined).length;
    return { maxUptime, latestVersion, asnCounts, totalWithAsn };
}

function getWeight(key) {
    return state.methodology?.weights?.[key] ?? 0;
}

function cloneValidator(validator) {
    return JSON.parse(JSON.stringify(validator));
}

function getMetricBaselineValue(validator, metric) {
    const metrics = validator.metrics;
    if (metric.key === "uptime_pct") {
        return metrics.uptime_pct ?? 0;
    }
    if (metric.key.startsWith("agreement_")) {
        return (metrics[metric.key] ?? 0) * 100;
    }
    return metrics[metric.key] ?? 0;
}

function applyControlsToValidator(validator) {
    const clone = cloneValidator(validator);
    const metrics = clone.metrics;
    const maxUptime = state.cohort.maxUptime;

    adjustableMetrics.forEach((metric) => {
        const rawValue = Number(state.controls[metric.key] ?? 0);
        if (metric.key === "uptime_pct") {
            metrics.uptime_pct = rawValue;
            metrics.uptime_seconds = Math.round((rawValue / 100) * maxUptime);
        } else if (metric.key.startsWith("agreement_")) {
            metrics[metric.key] = rawValue / 100;
        } else if (metric.key === "peer_count") {
            metrics.peer_count = Math.round(rawValue);
        } else {
            metrics[metric.key] = rawValue;
        }
    });

    return clone;
}

function scoreSelectedValidator(validator) {
    const v = applyControlsToValidator(validator);
    const metrics = v.metrics;
    const subScores = {
        agreement_1h: scoreAgreement(metrics.agreement_1h, metrics.agreement_1h_total),
        agreement_24h: scoreAgreement(metrics.agreement_24h, metrics.agreement_24h_total),
        agreement_30d: scoreAgreement(metrics.agreement_30d, metrics.agreement_30d_total),
        uptime: scoreUptime(metrics.uptime_seconds, state.cohort.maxUptime),
        poll_success: scorePollSuccess(metrics.poll_success_pct),
        latency: scoreLatency(metrics.latency_ms),
        peer_count: scorePeerCount(metrics.peer_count),
        version: scoreVersion(metrics.server_version, state.cohort.latestVersion),
        diversity: scoreDiversity(metrics.asn, state.cohort.asnCounts, state.cohort.totalWithAsn),
    };

    const composite =
        subScores.agreement_1h * getWeight("agreement_1h") +
        subScores.agreement_24h * getWeight("agreement_24h") +
        subScores.agreement_30d * getWeight("agreement_30d") +
        subScores.uptime * getWeight("uptime") +
        subScores.poll_success * getWeight("poll_success") +
        subScores.latency * getWeight("latency") +
        subScores.peer_count * getWeight("peer_count") +
        subScores.version * getWeight("version") +
        subScores.diversity * getWeight("diversity");

    return {
        validator: v,
        subScores,
        compositeScore: Math.round(composite * 10000) / 100,
    };
}

function rankValidators(projectedScore) {
    const others = state.data.validators
        .filter((validator) => validator.public_key !== state.selected.public_key)
        .map((validator) => ({
            public_key: validator.public_key,
            domain: validator.domain,
            composite_score: validator.composite_score,
        }));

    others.push({
        public_key: state.selected.public_key,
        domain: state.selected.domain,
        composite_score: projectedScore,
    });

    others.sort((a, b) => {
        if (b.composite_score !== a.composite_score) {
            return b.composite_score - a.composite_score;
        }
        return a.public_key.localeCompare(b.public_key);
    });

    return others;
}

function getNeighbors(sorted, publicKey) {
    const index = sorted.findIndex((entry) => entry.public_key === publicKey);
    const above = index > 0 ? sorted[index - 1] : null;
    const below = index < sorted.length - 1 ? sorted[index + 1] : null;
    return { index, above, below };
}

function describeRankDelta(delta) {
    if (delta > 0) return `<span class="green-text">Up ${delta} positions</span>`;
    if (delta < 0) return `<span class="red-text">Down ${Math.abs(delta)} positions</span>`;
    return `<span class="delta-flat">No rank change</span>`;
}

function describeScoreDelta(delta) {
    if (delta > 0) return `<span class="green-text">+${delta.toFixed(2)} points</span>`;
    if (delta < 0) return `<span class="red-text">${delta.toFixed(2)} points</span>`;
    return `<span class="delta-flat">No score change</span>`;
}

function computeCrossedValidators(baselineRank, projectedRank, sortedProjected) {
    if (projectedRank === baselineRank) return [];
    if (projectedRank < baselineRank) {
        return sortedProjected.slice(projectedRank - 1, baselineRank - 1).filter((entry) => entry.public_key !== state.selected.public_key);
    }
    return sortedProjected.slice(baselineRank, projectedRank).filter((entry) => entry.public_key !== state.selected.public_key);
}

function renderMetricControls() {
    $("metric-controls").innerHTML = adjustableMetrics.map((metric) => {
        const baseline = getMetricBaselineValue(state.selected, metric);
        return `
            <div class="metric-control">
                <div class="metric-header">
                    <div class="metric-title">${metric.label}</div>
                    <div class="metric-baseline">Baseline: ${fmtControlValue(metric, baseline)}</div>
                </div>
                <div class="metric-slider-row">
                    <input
                        type="range"
                        min="${metric.min}"
                        max="${metric.max}"
                        step="${metric.step}"
                        value="${baseline}"
                        data-control="${metric.key}"
                        data-kind="range"
                    >
                    <input
                        type="number"
                        min="${metric.min}"
                        max="${metric.max}"
                        step="${metric.step}"
                        value="${baseline}"
                        data-control="${metric.key}"
                        data-kind="number"
                    >
                </div>
                <div class="metric-note">${metric.note}</div>
            </div>
        `;
    }).join("");

    document.querySelectorAll("[data-control]").forEach((input) => {
        input.addEventListener("input", handleControlChange);
    });
}

function renderAssumptions(projected) {
    const selected = state.selected;
    const metrics = selected.metrics;
    $("assumptions").innerHTML = `
        <div class="assumption-item">
            <strong>${displayName(selected)}</strong>
            <div class="muted">${truncateKey(selected.public_key)}</div>
        </div>
        <div class="assumption-item">
            <strong>Fixed assumptions</strong>
            <div class="muted">Poll success stays at ${fmtNumber(metrics.poll_success_pct, 1)}%, version stays at ${metrics.server_version || "unknown"}, diversity stays at ${fmtNumber(selected.sub_scores.diversity * 100, 0)}% score.</div>
        </div>
        <div class="assumption-item">
            <strong>Scorer baseline</strong>
            <div class="muted">Cohort max uptime: ${fmtNumber(state.cohort.maxUptime / 86400, 1)} days. Latest version: ${state.cohort.latestVersion || "unknown"}.</div>
        </div>
        <div class="assumption-item">
            <strong>Projected sub-scores</strong>
            <div class="muted">Agreement 24h ${fmtNumber(projected.subScores.agreement_24h * 100, 0)}%, uptime ${fmtNumber(projected.subScores.uptime * 100, 0)}%, latency ${fmtNumber(projected.subScores.latency * 100, 0)}%, peers ${fmtNumber(projected.subScores.peer_count * 100, 0)}%.</div>
        </div>
    `;
}

function renderNearby(projectedRank, sortedProjected) {
    const baselineRank = state.selectedRank;
    const crossed = computeCrossedValidators(baselineRank, projectedRank, sortedProjected);
    const neighbors = getNeighbors(sortedProjected, state.selected.public_key);

    const lines = [];
    if (crossed.length) {
        const action = projectedRank < baselineRank ? "You would overtake" : "You would fall behind";
        lines.push(`
            <div class="nearby-item">
                <strong>${action}</strong>
                <div class="muted">${crossed.map((entry) => displayName(entry)).join(", ")}</div>
            </div>
        `);
    }

    if (neighbors.above) {
        lines.push(`
            <div class="nearby-item">
                <strong>One spot ahead</strong>
                <div class="muted">#${neighbors.index} ${displayName(neighbors.above)} at ${fmtNumber(neighbors.above.composite_score, 2)}</div>
            </div>
        `);
    }

    if (neighbors.below) {
        lines.push(`
            <div class="nearby-item">
                <strong>One spot behind</strong>
                <div class="muted">#${neighbors.index + 2} ${displayName(neighbors.below)} at ${fmtNumber(neighbors.below.composite_score, 2)}</div>
            </div>
        `);
    }

    if (!lines.length) {
        lines.push(`
            <div class="nearby-item">
                <strong>No nearby rank changes</strong>
                <div class="muted">Your simulated score keeps you in the same relative position right now.</div>
            </div>
        `);
    }

    $("nearby-list").innerHTML = lines.join("");
}

function scoreWithSingleMetric(metricKey, value) {
    const original = state.controls[metricKey];
    state.controls[metricKey] = value;
    const projected = scoreSelectedValidator(state.selected);
    const sorted = rankValidators(projected.compositeScore);
    const rank = getNeighbors(sorted, state.selected.public_key).index + 1;
    state.controls[metricKey] = original;
    return { projected, rank };
}

function renderOpportunities() {
    const entries = adjustableMetrics.map((metric) => {
        const baseline = Number(state.controls[metric.key]);
        const target = idealTargets[metric.key];
        const result = scoreWithSingleMetric(metric.key, target);
        const rankGain = state.selectedRank - result.rank;
        return {
            key: metric.key,
            label: metric.label,
            baseline,
            target,
            projectedScore: result.projected.compositeScore,
            rank: result.rank,
            rankGain,
        };
    }).sort((a, b) => {
        if (b.rankGain !== a.rankGain) return b.rankGain - a.rankGain;
        return b.projectedScore - a.projectedScore;
    });

    const best = entries[0];
    const gainText = best.rankGain > 0 ? `gain ${best.rankGain} positions` : best.rankGain < 0 ? `lose ${Math.abs(best.rankGain)} positions` : `hold your current rank`;

    $("best-opportunity").innerHTML = `
        <h3>Biggest opportunity: ${best.label}</h3>
        <div>Moving ${best.label.toLowerCase()} from <strong>${fmtControlValue(adjustableMetrics.find((metric) => metric.key === best.key), best.baseline)}</strong> to <strong>${fmtControlValue(adjustableMetrics.find((metric) => metric.key === best.key), best.target)}</strong> would ${gainText} and move your score to <strong>${fmtNumber(best.projectedScore, 2)}</strong>.</div>
    `;

    $("opportunity-list").innerHTML = entries.map((entry) => `
        <div class="opportunity-item">
            <strong>${entry.label}</strong>
            <div class="muted">${fmtControlValue(adjustableMetrics.find((metric) => metric.key === entry.key), entry.baseline)} -> ${fmtControlValue(adjustableMetrics.find((metric) => metric.key === entry.key), entry.target)}</div>
            <div class="${entry.rankGain > 0 ? "green-text" : entry.rankGain < 0 ? "red-text" : "muted"}">Projected rank #${entry.rank} | ${entry.rankGain > 0 ? `+${entry.rankGain} positions` : entry.rankGain < 0 ? `${entry.rankGain} positions` : "no rank change"}</div>
        </div>
    `).join("");
}

function renderCompareGrid() {
    const validators = state.data.validators;
    const metrics = state.selected.metrics;
    const topByMetric = {
        agreement_1h: validators.reduce((best, validator) => (validator.metrics.agreement_1h ?? -1) > (best.metrics.agreement_1h ?? -1) ? validator : best, validators[0]),
        agreement_24h: validators.reduce((best, validator) => (validator.metrics.agreement_24h ?? -1) > (best.metrics.agreement_24h ?? -1) ? validator : best, validators[0]),
        agreement_30d: validators.reduce((best, validator) => (validator.metrics.agreement_30d ?? -1) > (best.metrics.agreement_30d ?? -1) ? validator : best, validators[0]),
        uptime_pct: validators.reduce((best, validator) => (validator.metrics.uptime_pct ?? -1) > (best.metrics.uptime_pct ?? -1) ? validator : best, validators[0]),
        latency_ms: validators.reduce((best, validator) => {
            const score = validator.metrics.latency_ms ?? Infinity;
            const bestScore = best.metrics.latency_ms ?? Infinity;
            return score < bestScore ? validator : best;
        }, validators[0]),
        peer_count: validators.reduce((best, validator) => (validator.metrics.peer_count ?? -1) > (best.metrics.peer_count ?? -1) ? validator : best, validators[0]),
    };

    $("compare-grid").innerHTML = adjustableMetrics.map((metric) => {
        const winner = topByMetric[metric.key];
        const myRaw = metric.key === "uptime_pct" ? metrics.uptime_pct : metrics[metric.key];
        const winnerRaw = metric.key === "uptime_pct" ? winner.metrics.uptime_pct : winner.metrics[metric.key];
        const formattedMine = metric.key.startsWith("agreement_") ? fmtMetric(metric.key, myRaw) : fmtMetric(metric.key, myRaw);
        const formattedWinner = metric.key.startsWith("agreement_") ? fmtMetric(metric.key, winnerRaw) : fmtMetric(metric.key, winnerRaw);
        let gapText = "";
        if (metric.key === "latency_ms") {
            gapText = myRaw !== null && winnerRaw !== null ? `${fmtNumber(myRaw - winnerRaw, 1)}ms slower than best` : "Gap unavailable";
        } else if (metric.key.startsWith("agreement_")) {
            gapText = myRaw !== null && winnerRaw !== null ? `${fmtNumber((winnerRaw - myRaw) * 100, 1)} points behind the best` : "Gap unavailable";
        } else {
            gapText = myRaw !== null && winnerRaw !== null ? `${fmtNumber(winnerRaw - myRaw, 1)} behind the best` : "Gap unavailable";
        }
        return `
            <div class="compare-item">
                <strong>${metric.label}</strong>
                <div class="compare-meta">You: ${formattedMine}</div>
                <div class="compare-meta">Best live validator: ${formattedWinner} (${displayName(winner)})</div>
                <div class="accent-text">${gapText}</div>
            </div>
        `;
    }).join("");
}

function renderSummary(projected, sortedProjected) {
    const projectedRank = getNeighbors(sortedProjected, state.selected.public_key).index + 1;
    const scoreDelta = projected.compositeScore - state.selected.composite_score;
    const rankDelta = state.selectedRank - projectedRank;

    $("baseline-score").textContent = fmtNumber(state.selected.composite_score, 2);
    $("baseline-rank").textContent = `Current rank #${state.selectedRank} of ${state.data.validators.length}`;
    $("projected-score").textContent = fmtNumber(projected.compositeScore, 2);
    $("score-delta").innerHTML = describeScoreDelta(scoreDelta);
    $("projected-rank").textContent = `#${projectedRank}`;
    $("rank-delta").innerHTML = describeRankDelta(rankDelta);
    $("validator-name").textContent = displayName(state.selected);
    $("validator-meta").textContent = state.selected.public_key;

    renderAssumptions(projected);
    renderNearby(projectedRank, sortedProjected);
    renderOpportunities();
    renderCompareGrid();
}

function renderSimulation() {
    const projected = scoreSelectedValidator(state.selected);
    const sortedProjected = rankValidators(projected.compositeScore);
    renderSummary(projected, sortedProjected);
}

function syncControlInputs(controlKey, value) {
    document.querySelectorAll(`[data-control="${controlKey}"]`).forEach((input) => {
        input.value = value;
    });
}

function handleControlChange(event) {
    const controlKey = event.target.dataset.control;
    const metric = adjustableMetrics.find((entry) => entry.key === controlKey);
    let value = Number(event.target.value);
    if (Number.isNaN(value)) value = metric.min;
    value = Math.max(metric.min, Math.min(metric.max, value));
    if (metric.key === "peer_count") value = Math.round(value);
    state.controls[controlKey] = value;
    syncControlInputs(controlKey, value);
    renderSimulation();
}

function populateControls() {
    adjustableMetrics.forEach((metric) => {
        state.controls[metric.key] = getMetricBaselineValue(state.selected, metric);
    });
    renderMetricControls();
}

function setError(message) {
    const banner = $("error-banner");
    if (!message) {
        banner.style.display = "none";
        banner.textContent = "";
        return;
    }
    banner.style.display = "block";
    banner.textContent = message;
}

function setLoading(active) {
    $("loading-card").style.display = active ? "block" : "none";
}

function updateShareUrl() {
    const params = new URLSearchParams(window.location.search);
    params.set("validator", state.selected.public_key);
    const url = `${window.location.origin}/simulator?${params.toString()}`;
    window.history.replaceState({}, "", url);
    return url;
}

async function fetchData() {
    const [scoresResp, methodologyResp] = await Promise.all([
        fetch(`${API_BASE}/api/scores`),
        fetch(`${API_BASE}/api/methodology`),
    ]);
    if (!scoresResp.ok) throw new Error(`Scores API failed with ${scoresResp.status}`);
    if (!methodologyResp.ok) throw new Error(`Methodology API failed with ${methodologyResp.status}`);
    const [scores, methodology] = await Promise.all([scoresResp.json(), methodologyResp.json()]);
    return { scores, methodology };
}

async function loadValidator() {
    const publicKey = $("validator-key").value.trim();
    if (!publicKey) {
        setError("Paste a validator public key to load the simulator.");
        return;
    }

    setError("");
    setLoading(true);
    $("simulator-app").style.display = "none";

    try {
        if (!state.data) {
            const { scores, methodology } = await fetchData();
            state.data = scores;
            state.methodology = methodology;
            state.cohort = buildCohort(scores.validators);
        }

        const index = state.data.validators.findIndex((validator) => validator.public_key === publicKey);
        if (index === -1) {
            throw new Error("Validator not found in the live scores API.");
        }

        state.selected = cloneValidator(state.data.validators[index]);
        state.selectedRank = index + 1;
        populateControls();
        renderSimulation();
        updateShareUrl();
        $("simulator-app").style.display = "block";
    } catch (error) {
        setError(error.message || "Failed to load simulator data.");
    } finally {
        setLoading(false);
    }
}

function resetToBaseline() {
    if (!state.selected) return;
    populateControls();
    renderSimulation();
}

async function copyShareLink() {
    if (!state.selected) {
        setError("Load a validator before copying a share link.");
        return;
    }
    const url = updateShareUrl();
    try {
        await navigator.clipboard.writeText(url);
        $("helper-text").textContent = `Share link copied: ${url}`;
    } catch (_) {
        $("helper-text").textContent = `Share this URL: ${url}`;
    }
}

function init() {
    $("load-button").addEventListener("click", loadValidator);
    $("reset-button").addEventListener("click", resetToBaseline);
    $("share-button").addEventListener("click", copyShareLink);
    $("validator-key").addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
            event.preventDefault();
            loadValidator();
        }
    });

    const params = new URLSearchParams(window.location.search);
    const prefilled = params.get("validator");
    if (prefilled) {
        $("validator-key").value = prefilled;
        loadValidator();
    }
}

init();
