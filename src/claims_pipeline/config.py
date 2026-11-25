"""Pipeline configuration loader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

ENTITIES = ["members", "providers", "eligibility", "claims", "claim_lines", "pharmacy"]


@dataclass(frozen=True)
class Config:
    root: str
    zones: dict[str, str]
    phi: dict[str, Any]
    reference: dict[str, Any]
    quality_rules: dict[str, list[dict[str, Any]]]
    marts: dict[str, Any]

    def path(self, zone: str, *parts: str) -> str:
        """Build a path in a zone. Works for local paths and s3:// URIs."""
        return "/".join([self.root.rstrip("/"), self.zones[zone], *parts])

    def rules(self, entity: str) -> list[dict[str, Any]]:
        return self.quality_rules.get(entity, [])


def load_config(path: str | Path, root_override: str | None = None) -> Config:
    raw = yaml.safe_load(Path(path).read_text())
    root = root_override or os.environ.get("CLAIMS_ROOT") or raw["storage"]["root"]
    if "://" not in root:
        root = str(Path(root).resolve())
    return Config(
        root=root,
        zones=raw["storage"]["zones"],
        phi=raw["phi"],
        reference=raw.get("reference", {}),
        quality_rules=raw.get("quality_rules", {}),
        marts=raw.get("marts", {}),
    )


def normalize_batch_date(value: str) -> str:
    """Accept 'YYYY-MM-DD', or a schedule timestamp such as '2026-11-04T06:30:00Z'.

    A timestamp means "the scheduled run", which processes the previous day's batch.
    """
    if "T" not in value:
        return date.fromisoformat(value).isoformat()
    run_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (run_time.date() - timedelta(days=1)).isoformat()
