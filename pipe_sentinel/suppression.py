"""Pipeline suppression rules: temporarily silence alerts for specific pipelines."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class SuppressionRule:
    pipeline_name: str
    reason: str
    expires_at: Optional[datetime]  # None means indefinite

    def is_active(self, now: Optional[datetime] = None) -> bool:
        if self.expires_at is None:
            return True
        now = now or datetime.now(timezone.utc)
        return now < self.expires_at

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "reason": self.reason,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    @staticmethod
    def from_dict(d: dict) -> "SuppressionRule":
        expires_raw = d.get("expires_at")
        expires_at = datetime.fromisoformat(expires_raw) if expires_raw else None
        return SuppressionRule(
            pipeline_name=d["pipeline_name"],
            reason=d.get("reason", ""),
            expires_at=expires_at,
        )


@dataclass
class SuppressionStore:
    path: Path
    rules: Dict[str, SuppressionRule] = field(default_factory=dict)

    @staticmethod
    def load(path: Path) -> "SuppressionStore":
        store = SuppressionStore(path=path)
        if path.exists():
            raw = json.loads(path.read_text())
            for entry in raw.get("rules", []):
                rule = SuppressionRule.from_dict(entry)
                store.rules[rule.pipeline_name] = rule
        return store

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {"rules": [r.to_dict() for r in self.rules.values()]}
        self.path.write_text(json.dumps(data, indent=2))

    def suppress(self, rule: SuppressionRule) -> None:
        self.rules[rule.pipeline_name] = rule
        self.save()

    def unsuppress(self, pipeline_name: str) -> bool:
        if pipeline_name in self.rules:
            del self.rules[pipeline_name]
            self.save()
            return True
        return False

    def is_suppressed(self, pipeline_name: str, now: Optional[datetime] = None) -> bool:
        rule = self.rules.get(pipeline_name)
        return rule is not None and rule.is_active(now)

    def active_rules(self, now: Optional[datetime] = None) -> List[SuppressionRule]:
        return [r for r in self.rules.values() if r.is_active(now)]

    def prune_expired(self, now: Optional[datetime] = None) -> int:
        expired = [name for name, r in self.rules.items() if not r.is_active(now)]
        for name in expired:
            del self.rules[name]
        if expired:
            self.save()
        return len(expired)
