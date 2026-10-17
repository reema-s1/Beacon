"""Load runbooks (markdown with YAML front matter) from disk."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_FRONT_MATTER = re.compile(r"\A---\s*\n(.*?)\n---\s*\n(.*)\Z", re.S)


@dataclass(frozen=True)
class Runbook:
    id: str
    title: str
    root_cause: str
    symptoms: tuple[str, ...]
    body: str
    path: str = field(default="", compare=False)

    @property
    def fix_steps(self) -> list[str]:
        """Numbered steps under the '## Fix' heading."""
        m = re.search(r"^## Fix\s*\n(.*?)(?=^## |\Z)", self.body, re.S | re.M)
        if not m:
            return []
        return [re.sub(r"^\d+\.\s*", "", line).strip()
                for line in m.group(1).splitlines() if re.match(r"^\d+\.", line.strip())]

    @property
    def search_text(self) -> str:
        """Text that gets embedded: title, symptoms and the first paragraph."""
        intro = self.body.split("##", 1)[0]
        intro = re.sub(r"^#.*$", "", intro, flags=re.M).strip()
        return f"{self.title}. " + ". ".join(self.symptoms) + ". " + intro


def parse_runbook(text: str, path: str = "") -> Runbook:
    m = _FRONT_MATTER.match(text.replace("\r\n", "\n"))
    if not m:
        raise ValueError(f"runbook {path or '<text>'} has no YAML front matter")
    meta = yaml.safe_load(m.group(1)) or {}
    for key in ("id", "title", "root_cause", "symptoms"):
        if key not in meta:
            raise ValueError(f"runbook {path or meta.get('id')} is missing '{key}'")
    return Runbook(id=meta["id"], title=meta["title"], root_cause=meta["root_cause"],
                   symptoms=tuple(meta["symptoms"]), body=m.group(2).strip(), path=path)


def load_runbooks(directory: Path) -> list[Runbook]:
    books = [parse_runbook(p.read_text(encoding="utf-8"), str(p)) for p in sorted(Path(directory).glob("*.md"))]
    ids = [b.id for b in books]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate runbook ids in {directory}")
    return books
