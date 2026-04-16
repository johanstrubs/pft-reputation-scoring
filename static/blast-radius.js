const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function fmtDelta(value) {
    if (value == null) return "-";
    const sign = value > 0 ? "+" : "";
    return `${sign}${value.toFixed ? value.toFixed(2) : value}`;
}

function fmtDate(value) {
    return value ? new Date(value).toLocaleString() : "Ongoing";
}

function renderRisks(entries) {
    const root = $("risk-list");
    if (!entries.length) {
        root.innerHTML = `<div class="item"><strong>No enriched dependency concentrations available yet.</strong></div>`;
        return;
    }
    root.innerHTML = entries.map((entry) => `
        <div class="item">
            <h3>${escapeHtml(entry.dependency_type)}: ${escapeHtml(entry.dependency_value)}</h3>
            <div class="meta">
                <span class="chip">${escapeHtml(entry.affected_validators)} validators</span>
                <span class="chip">${escapeHtml(entry.network_pct)}% of network</span>
                ${entry.consensus_risk ? `<span class="chip risk">consensus risk heuristic</span>` : ""}
            </div>
            <div>If this dependency failed, approximately ${escapeHtml(entry.remaining_validators_if_failed)} validators would remain.</div>
            <div class="subtle">${escapeHtml(entry.mitigation_guidance)}</div>
        </div>
    `).join("");
}

function renderEvents(entries, rootId, emptyMessage) {
    const root = $(rootId);
    if (!entries.length) {
        root.innerHTML = `<div class="item"><strong>${escapeHtml(emptyMessage)}</strong></div>`;
        return;
    }
    root.innerHTML = entries.map((event) => `
        <div class="item">
            <h3>${escapeHtml(event.suspected_cause)}</h3>
            <div class="meta">
                <span class="chip ${escapeHtml(event.severity)}">${escapeHtml(event.severity)}</span>
                <span class="chip">${escapeHtml(event.status)}</span>
                ${event.consensus_risk ? `<span class="chip risk">consensus risk heuristic</span>` : ""}
                ${event.synthetic ? `<span class="chip warning">synthetic test</span>` : ""}
            </div>
            <div><strong>Dependency:</strong> ${escapeHtml(event.dependency_value)} | <strong>Affected:</strong> ${escapeHtml(event.affected_count)} validators (${escapeHtml(event.network_pct)}%)</div>
            <div><strong>Window:</strong> ${fmtDate(event.start_timestamp)} to ${fmtDate(event.end_timestamp || event.latest_timestamp)}</div>
            <div><strong>Peak affected:</strong> ${escapeHtml(event.peak_affected_count)} | <strong>Avg score drop:</strong> ${fmtDelta(event.avg_score_drop)}</div>
            <div><strong>Validators:</strong> ${escapeHtml((event.affected_validators || []).join(", "))}</div>
            <div class="subtle">${escapeHtml(event.mitigation_guidance)}</div>
        </div>
    `).join("");
}

async function loadBlastRadius() {
    try {
        const resp = await fetch("/api/blast-radius");
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.detail || "Failed to load blast radius report");

        $("round-id").textContent = String(data.round_id);
        $("active-count").textContent = String((data.active_correlations || []).length);
        $("validator-count").textContent = String(data.total_validators);
        $("json-link").href = data.json_report_url;

        renderRisks(data.concentration_risks || []);
        renderEvents(data.active_correlations || [], "active-list", "No active correlated events right now. The standing risk summary above remains the current blast-radius assessment.");
        renderEvents(data.historical_correlations || [], "history-list", "No closed correlated events recorded yet.");

        $("loading").hidden = true;
        $("app").hidden = false;
    } catch (err) {
        $("loading").hidden = true;
        $("error").hidden = false;
        $("error").textContent = err.message || "Failed to load blast radius report";
    }
}

loadBlastRadius();
