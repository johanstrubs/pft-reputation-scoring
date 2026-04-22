const $ = (id) => document.getElementById(id);

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function prettyJson(value) {
    return JSON.stringify(value, null, 2);
}

function shiftDate(isoDate, days) {
    const base = new Date(`${isoDate}T00:00:00Z`);
    base.setUTCDate(base.getUTCDate() + days);
    return base.toISOString().slice(0, 10);
}

function renderComponents(components) {
    const root = $("health-components");
    const entries = Object.entries(components || {});
    root.innerHTML = entries.map(([name, component]) => `
        <div class="component-item">
            <strong>${escapeHtml(name)}</strong>
            <div>Raw: ${escapeHtml(component.raw_value)} ${escapeHtml(component.raw_unit || "")}</div>
            <div>Normalized score: ${escapeHtml(component.normalized_score)}</div>
            <div>Weight: ${escapeHtml(component.weight)} | Contribution: ${escapeHtml(component.weighted_contribution)}</div>
            <div class="subtle">${escapeHtml(component.description || "")}</div>
        </div>
    `).join("");
}

function renderDocs(schema, latest, risk) {
    const docs = [
        {
            path: "/api/dataset/latest",
            note: "Most recent daily snapshot.",
            example: {
                snapshot_date: latest.snapshot_date,
                round_id: latest.round_id,
                validator_count: latest.validator_count,
                network_health_index: { score: latest.network_health_index.score },
            },
        },
        {
            path: "/api/dataset/snapshot/2026-04-20",
            note: "Specific daily snapshot by ISO UTC date.",
            example: { snapshot_date: "2026-04-20", round_id: latest.round_id },
        },
        {
            path: "/api/dataset/timeseries/nHBcLEB4S6moQGrhMjJo1jbp58WL5psHY9EMDWNAtdqykUYiA1rF?days=30",
            note: "Validator daily history.",
            example: { days: 30, history_entry_shape: { date: latest.snapshot_date, composite_score: 0, rank: 0 } },
        },
        {
            path: "/api/dataset/diff/2026-04-19/2026-04-20",
            note: "Diff between two daily snapshots.",
            example: { validators_added: [], validators_removed: [], score_changes: [] },
        },
        {
            path: "/api/dataset/schema",
            note: "Schema and formula documentation.",
            example: { dataset_schema_version: schema.dataset_schema_version, network_health_formula_version: schema.network_health_formula_version },
        },
        {
            path: "/api/risk",
            note: "Current network health index and 7-day trend.",
            example: { snapshot_date: risk.snapshot_date, score: risk.score, trend_7d: risk.trend_7d },
        },
    ];
    $("api-docs").innerHTML = docs.map((doc) => `
        <div class="doc-item">
            <div><a href="${doc.path}" target="_blank" rel="noreferrer">${doc.path}</a></div>
            <div class="subtle">${escapeHtml(doc.note)}</div>
            <pre class="code-block">${escapeHtml(prettyJson(doc.example))}</pre>
        </div>
    `).join("");

    $("schema-version").textContent = schema.dataset_schema_version || "-";
}

async function loadHashes() {
    const jsonResp = await fetch("/api/dataset/export?format=json", { method: "HEAD" });
    const csvResp = await fetch("/api/dataset/export?format=csv", { method: "HEAD" });
    $("json-hash").textContent = jsonResp.headers.get("x-content-sha256") || "unavailable";
    $("csv-hash").textContent = csvResp.headers.get("x-content-sha256") || "unavailable";
}

async function loadDatasetPage() {
    try {
        const [latestResp, schemaResp, riskResp] = await Promise.all([
            fetch("/api/dataset/latest"),
            fetch("/api/dataset/schema"),
            fetch("/api/risk"),
        ]);

        const latest = await latestResp.json();
        const schema = await schemaResp.json();
        const risk = await riskResp.json();

        if (!latestResp.ok) throw new Error(latest.detail || "Failed to load latest dataset snapshot");
        if (!schemaResp.ok) throw new Error(schema.detail || "Failed to load dataset schema");
        if (!riskResp.ok) throw new Error(risk.detail || "Failed to load risk report");

        const meta = latest.dataset_metadata || {};
        $("date-range").textContent = meta.date_range ? `${meta.date_range.start} to ${meta.date_range.end}` : "-";
        $("snapshot-count").textContent = String(meta.total_daily_snapshots ?? 0);
        $("record-count").textContent = String(meta.total_validator_day_score_records ?? 0);
        $("schema-version").textContent = latest.dataset_schema_version || "-";
        $("health-score").textContent = String(risk.score ?? "-");
        $("health-semantics").textContent = risk.score_semantics || "";
        $("json-link").href = "/api/dataset/latest";
        $("latest-link").href = "/api/dataset/latest";
        $("schema-link").href = "/api/dataset/schema";
        $("risk-link").href = "/api/risk";
        const coveredStart = meta.date_range?.start || latest.snapshot_date;
        const datedSnapshotDate = coveredStart === latest.snapshot_date ? latest.snapshot_date : shiftDate(latest.snapshot_date, -1);
        $("dated-snapshot-link").href = `/api/dataset/snapshot/${datedSnapshotDate}`;
        $("dated-snapshot-link").textContent = `/api/dataset/snapshot/${datedSnapshotDate}`;
        $("dated-snapshot-note").textContent = `Example dated snapshot from the current covered range (${coveredStart} to ${meta.date_range?.end || latest.snapshot_date}).`;
        $("snapshot-preview").textContent = prettyJson({
            snapshot_date: latest.snapshot_date,
            round_id: latest.round_id,
            validator_count: latest.validator_count,
            version_distribution: latest.version_distribution.slice(0, 5),
            concentration_metrics: latest.concentration_metrics,
            network_health_index: latest.network_health_index,
        });

        renderComponents(risk.components);
        renderDocs(schema, latest, risk);
        await loadHashes();

        $("loading").hidden = true;
        $("app").hidden = false;
    } catch (err) {
        $("loading").hidden = true;
        $("error").hidden = false;
        $("error").textContent = err.message || "Failed to load dataset page";
    }
}

loadDatasetPage();
