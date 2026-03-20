import logging
from collections import Counter
from datetime import datetime, timezone

from app.models import ValidatorSnapshot, ValidatorScore, ValidatorSubScores

logger = logging.getLogger(__name__)

# Weights must sum to 1.0
WEIGHTS = {
    "agreement_1h": 0.10,
    "agreement_24h": 0.15,
    "agreement_30d": 0.20,
    "uptime": 0.08,
    "poll_success": 0.07,
    "latency": 0.10,
    "peer_count": 0.10,
    "version": 0.10,
    "diversity": 0.10,
}


class ReputationScorer:
    def score(self, snapshots: list[ValidatorSnapshot]) -> list[ValidatorScore]:
        if not snapshots:
            return []

        # Pre-compute cohort-level stats needed for normalization
        max_uptime = max(
            (s.metrics.uptime_seconds for s in snapshots if s.metrics.uptime_seconds is not None),
            default=1,
        ) or 1

        latest_version = self._determine_latest_version(snapshots)
        asn_counts = self._count_asns(snapshots)
        total_with_asn = sum(1 for s in snapshots if s.metrics.asn is not None)

        now = datetime.now(timezone.utc).isoformat()
        results = []

        # Compute uptime_pct for all validators (relative to max observed)
        for snap in snapshots:
            if snap.metrics.uptime_seconds is not None:
                snap.metrics.uptime_pct = round(100.0 * snap.metrics.uptime_seconds / max_uptime, 2)

        for snap in snapshots:
            sub = ValidatorSubScores(
                agreement_1h=self._score_agreement(snap.metrics.agreement_1h, snap.metrics.agreement_1h_total),
                agreement_24h=self._score_agreement(snap.metrics.agreement_24h, snap.metrics.agreement_24h_total),
                agreement_30d=self._score_agreement(snap.metrics.agreement_30d, snap.metrics.agreement_30d_total),
                uptime=self._score_uptime(snap.metrics.uptime_seconds, max_uptime),
                poll_success=self._score_poll_success(snap.metrics.poll_success_pct),
                latency=self._score_latency(snap.metrics.latency_ms),
                peer_count=self._score_peer_count(snap.metrics.peer_count),
                version=self._score_version(snap.metrics.server_version, latest_version),
                diversity=self._score_diversity(snap.metrics.asn, asn_counts, total_with_asn),
            )

            composite = (
                sub.agreement_1h * WEIGHTS["agreement_1h"]
                + sub.agreement_24h * WEIGHTS["agreement_24h"]
                + sub.agreement_30d * WEIGHTS["agreement_30d"]
                + sub.uptime * WEIGHTS["uptime"]
                + sub.poll_success * WEIGHTS["poll_success"]
                + sub.latency * WEIGHTS["latency"]
                + sub.peer_count * WEIGHTS["peer_count"]
                + sub.version * WEIGHTS["version"]
                + sub.diversity * WEIGHTS["diversity"]
            ) * 100

            results.append(ValidatorScore(
                public_key=snap.public_key,
                domain=snap.domain,
                composite_score=round(composite, 2),
                metrics=snap.metrics,
                sub_scores=sub,
                last_updated=now,
            ))

        results.sort(key=lambda s: s.composite_score, reverse=True)
        logger.info(
            "Scored %d validators. Avg=%.1f, Min=%.1f, Max=%.1f",
            len(results),
            sum(r.composite_score for r in results) / len(results),
            min(r.composite_score for r in results),
            max(r.composite_score for r in results),
        )
        return results

    @staticmethod
    def _score_agreement(value: float | None, total: int | None = None) -> float:
        if value is None:
            return 0.0
        if total is not None and total == 0:
            return 0.5  # No data in this window (e.g. VHS 1h aggregation gap) — neutral
        if value < 0.8:
            return 0.0
        # Linear from 0.8 -> 0.0 to 1.0 -> 1.0
        return min(1.0, max(0.0, (value - 0.8) / 0.2))

    @staticmethod
    def _score_poll_success(pct: float | None) -> float:
        if pct is None:
            return 0.5  # No poll history yet — neutral
        if pct >= 95.0:
            return 1.0
        if pct < 70.0:
            return 0.0
        return (pct - 70.0) / 25.0

    @staticmethod
    def _score_uptime(seconds: int | None, max_uptime: int) -> float:
        if seconds is None:
            return 0.0
        return min(1.0, seconds / max_uptime)

    @staticmethod
    def _score_latency(ms: float | None) -> float:
        if ms is None:
            return 0.5  # neutral if unknown
        if ms <= 50:
            return 1.0
        if ms >= 500:
            return 0.0
        return 1.0 - (ms - 50) / 450

    @staticmethod
    def _score_peer_count(count: int | None) -> float:
        if count is None:
            return 0.5  # neutral if unknown
        if count >= 10:
            return 1.0
        if count < 3:
            return 0.0
        return (count - 3) / 7

    @staticmethod
    def _score_version(version: str | None, latest: str | None) -> float:
        if version is None or latest is None:
            return 0.5
        if version == latest:
            return 1.0
        # Try to compare version numbers
        try:
            v_parts = [int(x) for x in version.split(".")]
            l_parts = [int(x) for x in latest.split(".")]
            # Check if one minor version behind
            if len(v_parts) >= 2 and len(l_parts) >= 2:
                if v_parts[0] == l_parts[0] and l_parts[1] - v_parts[1] == 1:
                    return 0.8
        except (ValueError, IndexError):
            pass
        return 0.5

    @staticmethod
    def _score_diversity(asn: int | None, asn_counts: Counter, total_with_asn: int) -> float:
        if asn is None or total_with_asn == 0:
            return 0.5  # neutral if unknown
        concentration = asn_counts.get(asn, 0) / total_with_asn
        if concentration > 0.30:
            # Penalty: linear from 0.5 at 30% to 0.0 at 100%
            return max(0.0, 0.5 * (1.0 - (concentration - 0.30) / 0.70))
        # Reward uniqueness: lower concentration = higher score
        # Linear from 1.0 at unique to 0.5 at 30%
        return 1.0 - (concentration / 0.30) * 0.5

    @staticmethod
    def _determine_latest_version(snapshots: list[ValidatorSnapshot]) -> str | None:
        versions = [s.metrics.server_version for s in snapshots if s.metrics.server_version]
        if not versions:
            return None
        # Mode = most common version
        counter = Counter(versions)
        return counter.most_common(1)[0][0]

    @staticmethod
    def _count_asns(snapshots: list[ValidatorSnapshot]) -> Counter:
        return Counter(s.metrics.asn for s in snapshots if s.metrics.asn is not None)
