#!/usr/bin/env python3
"""Comprehensive repository cleanup audit generator."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

REFERENCE_LIMIT = 8
STALE_DAYS_REMOVE = 365
STALE_DAYS_QUARANTINE = 180

MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

MANUAL_RULES = {
    ".mc/config.json.old": {
        "classification": "REMOVE",
        "reason": "Archivo de respaldo antiguo de MinIO; reemplazado por config.json",
        "risk": "low",
        "proposed_action": "delete",
        "notes": "No se usa en CI ni en scripts actuales.",
    },
    "docs/REPORT.md": {
        "classification": "QUARANTINE",
        "reason": "Reporte legacy previo a la documentación modular.",
        "risk": "medium",
        "proposed_action": "move_to_legacy",
        "notes": "Conservar en /legacy/docs hasta validar que no sea referencia externa.",
    },
    "docs/report.json": {
        "classification": "QUARANTINE",
        "reason": "Export JSON antiguo duplicando reportes actuales.",
        "risk": "medium",
        "proposed_action": "move_to_legacy",
        "notes": "Generado por tooling previo; no usado en pipelines.",
    },
    "scripts/generate_big_payments.py": {
        "classification": "QUARANTINE",
        "reason": "Script de generación legacy reemplazado por generate_synthetic_data.py.",
        "risk": "medium",
        "proposed_action": "move_to_legacy",
        "notes": "No referenciado desde CLI actuales.",
    },
    "docs/run/jobs/dataproc_workflow.yaml": {
        "classification": "QUARANTINE",
        "reason": "Plantilla Dataproc sin referencias en CI/CD.",
        "risk": "medium",
        "proposed_action": "move_to_legacy",
        "notes": "Considerar mover a legacy/infra antes de eliminar.",
    },
}


@dataclass
class ReferenceBucket:
    code: List[str] = field(default_factory=list)
    configs: List[str] = field(default_factory=list)
    ci: List[str] = field(default_factory=list)
    docs: List[str] = field(default_factory=list)
    jobs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, List[str]]:
        return {
            "code": self.code,
            "configs": self.configs,
            "ci": self.ci,
            "docs": self.docs,
            "jobs": self.jobs,
        }

    @property
    def count(self) -> int:
        return sum(len(bucket) for bucket in self.to_dict().values())


@dataclass
class CleanupItem:
    path: Path
    type: str
    size_kb: float
    last_commit: Optional[str]
    references: ReferenceBucket
    classification: str
    reason: str
    risk: str
    proposed_action: str
    notes: str = ""

    def to_json(self) -> Dict[str, object]:
        return {
            "path": str(self.path).replace(os.sep, "/"),
            "type": self.type,
            "size_kb": round(self.size_kb, 2),
            "last_commit": self.last_commit or "",
            "references": self.references.to_dict(),
            "classification": self.classification,
            "reason": self.reason,
            "risk": self.risk,
            "proposed_action": self.proposed_action,
            "notes": self.notes,
        }


@dataclass
class AuditArtifacts:
    broken_links: List[Dict[str, str]] = field(default_factory=list)
    orphan_docs: List[str] = field(default_factory=list)
    orphan_notebooks: List[str] = field(default_factory=list)
    vulture_output: str = ""
    deptry_output: str = ""


def detect_type(repo_root: Path, path: Path) -> str:
    rel = path.relative_to(repo_root)
    if rel.suffix == ".ipynb":
        return "notebook"
    if rel.suffix in {".py", ".pyi"}:
        return "code"
    if rel.suffix in {".yml", ".yaml", ".ini", ".toml", ".cfg"}:
        return "config"
    if rel.name.startswith("Dockerfile") or rel.suffix in {".dockerfile"}:
        return "docker"
    if rel.parts[0] in {"infra", "terraform", "helm"}:
        return "infra"
    if rel.parts[0] in {".github", "ci"} or rel.suffix in {".yml", ".yaml"} and ".github" in rel.parts:
        return "ci"
    if rel.suffix in {".md", ".rst"} or rel.parts[0] == "docs":
        return "doc"
    if rel.suffix in {".json", ".ndjson"} and rel.parts[0] != "data":
        return "config"
    return "other"


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)


def get_git_last_commit(repo_root: Path, path: Path) -> Optional[str]:
    result = run_cmd(["git", "log", "-1", "--format=%cs", str(path.relative_to(repo_root))], cwd=repo_root)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return result.stdout.strip()


def search_references(repo_root: Path, path: Path) -> ReferenceBucket:
    bucket = ReferenceBucket()
    target = path.name
    if not target:
        return bucket
    rg_path = shutil.which("rg")
    if rg_path is None:
        return bucket
    cmd = [rg_path, "--no-heading", "--line-number", "--color", "never", "--fixed-strings", target, str(repo_root)]
    result = run_cmd(cmd, cwd=repo_root)
    if result.returncode not in {0, 1}:
        return bucket
    rel_self = str(path.relative_to(repo_root))
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            match_path, match_line, *_ = line.split(":", 2)
        except ValueError:
            continue
        if match_path == rel_self:
            continue
        match_path_obj = Path(match_path)
        try:
            match_rel = match_path_obj.relative_to(repo_root)
        except ValueError:
            match_rel = match_path_obj
        match_type = detect_type(repo_root, repo_root / match_rel)
        entry = f"{match_rel}:{match_line}"
        bucket_list = getattr(bucket, {
            "code": "code",
            "config": "configs",
            "docker": "configs",
            "infra": "jobs",
            "ci": "ci",
            "doc": "docs",
        }.get(match_type, "code"))
        if len(bucket_list) < REFERENCE_LIMIT:
            bucket_list.append(entry)
    return bucket


def classify_item(item: CleanupItem, today: dt.date) -> CleanupItem:
    references_count = item.references.count
    last_commit_date = None
    if item.last_commit:
        try:
            last_commit_date = dt.datetime.strptime(item.last_commit, "%Y-%m-%d").date()
        except ValueError:
            last_commit_date = None
    age_days = (today - last_commit_date).days if last_commit_date else None

    special_keep = {
        "pyproject.toml",
        "requirements.txt",
        "README.md",
        "docs/CLEANUP_REPORT.md",
        "docs/cleanup.json",
        "docs/diagrams/deps_cleanup.md",
        "docs/policies/DEP-001-legacy-removal.md",
    }
    rel = str(item.path)
    if rel in special_keep or rel.startswith("docs/policies/"):
        item.classification = "KEEP"
        item.reason = "Governance or generated artifact"
        item.risk = "low"
        item.proposed_action = "keep"
        return item

    if item.type == "doc" and "policy" in rel:
        item.classification = "KEEP"
        item.reason = "Policy documentation"
        item.risk = "low"
        item.proposed_action = "keep"
        return item

    if references_count == 0 and age_days and age_days > STALE_DAYS_REMOVE:
        item.classification = "REMOVE"
        item.reason = "No references detected and stale (>12 months)."
        item.risk = "low"
        item.proposed_action = "delete"
    elif references_count == 0 and age_days and age_days > STALE_DAYS_QUARANTINE:
        item.classification = "QUARANTINE"
        item.reason = "No references detected and stale (>6 months)."
        item.risk = "medium"
        item.proposed_action = "move_to_legacy"
    else:
        item.classification = "KEEP"
        item.reason = item.reason or "Referenced or recently modified."
        if references_count == 0:
            item.reason = "Recently modified; needs manual confirmation."
        item.risk = item.risk or "medium"
        item.proposed_action = "keep"
    return item


def collect_files(repo_root: Path) -> List[Path]:
    ignore_dirs = {".git", "__pycache__", "build", "dist", ".mypy_cache", ".pytest_cache"}
    paths: List[Path] = []
    for path in repo_root.rglob("*"):
        if any(part in ignore_dirs for part in path.parts):
            continue
        if path.is_dir():
            continue
        if path.name.endswith("~"):
            continue
        paths.append(path)
    return sorted(paths)


def detect_broken_links(repo_root: Path) -> List[Dict[str, str]]:
    broken: List[Dict[str, str]] = []
    for md in repo_root.rglob("*.md"):
        if ".git" in md.parts:
            continue
        text = md.read_text(encoding="utf-8", errors="ignore")
        for match in MARKDOWN_LINK_RE.finditer(text):
            target = match.group(2)
            if target.startswith("mailto:"):
                continue
            if target.startswith("http://") or target.startswith("https://"):
                try:
                    req = Request(target, method="HEAD")
                    with urlopen(req, timeout=5):  # nosec - head request
                        pass
                except (HTTPError, URLError, TimeoutError) as exc:
                    broken.append({
                        "source": str(md.relative_to(repo_root)),
                        "link": target,
                        "error": str(exc),
                    })
            elif target.startswith("#"):
                continue
            else:
                candidate = (md.parent / target).resolve()
                try:
                    candidate.relative_to(repo_root)
                except ValueError:
                    # points outside repo
                    continue
                if not candidate.exists():
                    broken.append({
                        "source": str(md.relative_to(repo_root)),
                        "link": target,
                        "error": "missing file",
                    })
    return broken


def detect_orphans(repo_root: Path, suffix: str) -> List[str]:
    results: List[str] = []
    rg_path = shutil.which("rg")
    for path in repo_root.rglob(f"*{suffix}"):
        if path.is_dir():
            continue
        rel = str(path.relative_to(repo_root))
        if rg_path is None:
            results.append(rel)
            continue
        cmd = [rg_path, "--no-heading", "--count", rel.split("/")[-1], str(repo_root)]
        result = run_cmd(cmd, cwd=repo_root)
        if result.returncode not in {0, 1}:
            continue
        count = 0
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                _, occurrences = line.rsplit(":", 1)
                count += int(occurrences)
            except ValueError:
                continue
        if count <= 1:
            results.append(rel)
    return sorted(results)


def run_optional_tool(cmd: List[str], cwd: Path) -> str:
    result = run_cmd(cmd, cwd=cwd)
    output = result.stdout
    if result.stderr:
        output = (output + "\n" + result.stderr).strip()
    if result.returncode not in {0, 1}:
        return output
    return output


def create_markdown(report_path: Path, repo_root: Path, items: List[CleanupItem], artifacts: AuditArtifacts, summary: Dict[str, object]) -> None:
    lines: List[str] = []
    lines.append("# Cleanup Audit Report")
    lines.append("")
    lines.append(f"_Generated on {dt.datetime.utcnow().isoformat()}Z_")
    lines.append("")
    lines.append("## Resumen ejecutivo")
    lines.append("")
    totals = summary["totals"]
    lines.append("| Clasificación | Conteo |")
    lines.append("| --- | ---: |")
    for key in ("remove", "quarantine", "keep"):
        lines.append(f"| {key.upper()} | {totals.get(key, 0)} |")
    lines.append("")
    lines.append(f"**Ahorro estimado**: {summary['savings_mb_estimate']:.2f} MB si se aplica REMOVE + QUARANTINE.")
    lines.append("")

    lines.append("## Tabla maestra")
    lines.append("")
    lines.append("| Path | Tipo | Tamaño (KB) | Último commit | # refs | Clasificación | Motivo | Riesgo | Acción |")
    lines.append("| --- | --- | ---: | --- | ---: | --- | --- | --- | --- |")
    for item in items:
        rel = str(item.path)
        lines.append(
            "| {path} | {type} | {size:.2f} | {commit} | {refs} | {cls} | {reason} | {risk} | {action} |".format(
                path=rel,
                type=item.type,
                size=item.size_kb,
                commit=item.last_commit or "-",
                refs=item.references.count,
                cls=item.classification,
                reason=item.reason.replace("|", "/"),
                risk=item.risk,
                action=item.proposed_action,
            )
        )
    lines.append("")

    lines.append("## Evidencia por elemento")
    lines.append("")
    for item in items:
        rel = str(item.path)
        lines.append(f"### {rel}")
        lines.append("")
        if item.references.count:
            lines.append("Referencias detectadas:")
            lines.append("")
            for key, entries in item.references.to_dict().items():
                if not entries:
                    continue
                lines.append(f"- **{key}**: {', '.join(entries)}")
        else:
            lines.append("- No se detectaron referencias (búsqueda por nombre de archivo).")
        if item.notes:
            lines.append(f"- Notas: {item.notes}")
        lines.append("- Clasificación: {0}".format(item.classification))
        lines.append("")

    lines.append("## Impacto estimado")
    lines.append("")
    lines.append(f"- Archivos candidatos a eliminar inmediatamente: {totals.get('remove', 0)}")
    lines.append(f"- Archivos candidatos a cuarentena: {totals.get('quarantine', 0)}")
    lines.append(f"- Ahorro aproximado: {summary['savings_mb_estimate']:.2f} MB")
    lines.append("")
    lines.append("## Riesgos y mitigaciones")
    lines.append("")
    lines.append("- Revisar manualmente los elementos marcados como QUARANTINE antes de moverlos a /legacy.")
    lines.append("- Confirmar dependencias transversales en CI/CD para los elementos KEEP críticos.")
    lines.append("- Establecer un rollback rápido restaurando archivos desde git si un pipeline falla.")
    lines.append("")

    lines.append("## Plan sugerido")
    lines.append("")
    lines.append("1. Crear PRs por lote (infra, pipelines, documentación) para aplicar REMOVE/QUARANTINE.")
    lines.append("2. Actualizar dependencias declaradas (según deptry) antes de eliminar código compartido.")
    lines.append("3. Mover notebooks huérfanos a /legacy/notebooks con un README que documente su estado.")
    lines.append("")

    lines.append("## Anexos")
    lines.append("")
    lines.append("### Hallazgos de Vulture")
    lines.append("")
    lines.append("```")
    lines.append(artifacts.vulture_output.strip() or "No disponible")
    lines.append("```")
    lines.append("")
    lines.append("### Hallazgos de Deptry")
    lines.append("")
    lines.append("```")
    lines.append(artifacts.deptry_output.strip() or "No disponible")
    lines.append("```")
    lines.append("")
    lines.append("### Links rotos")
    lines.append("")
    if artifacts.broken_links:
        for entry in artifacts.broken_links:
            lines.append(f"- {entry['source']} → {entry['link']} ({entry['error']})")
    else:
        lines.append("- No se detectaron links rotos.")
    lines.append("")
    lines.append("### Documentos huérfanos")
    lines.append("")
    if artifacts.orphan_docs:
        for doc in artifacts.orphan_docs:
            lines.append(f"- {doc}")
    else:
        lines.append("- Ninguno")
    lines.append("")
    lines.append("### Notebooks huérfanos")
    lines.append("")
    if artifacts.orphan_notebooks:
        for nb in artifacts.orphan_notebooks:
            lines.append(f"- {nb}")
    else:
        lines.append("- Ninguno")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def build_items(repo_root: Path) -> List[CleanupItem]:
    today = dt.date.today()
    items: List[CleanupItem] = []
    for path in collect_files(repo_root):
        rel = path.relative_to(repo_root)
        file_type = detect_type(repo_root, path)
        try:
            size_kb = path.stat().st_size / 1024
        except OSError:
            size_kb = 0.0
        last_commit = get_git_last_commit(repo_root, path)
        references = search_references(repo_root, path)
        item = CleanupItem(
            path=rel,
            type=file_type,
            size_kb=size_kb,
            last_commit=last_commit,
            references=references,
            classification="KEEP",
            reason="",
            risk="medium",
            proposed_action="keep",
            notes="",
        )
        if file_type == "doc" and "README" in rel.name:
            item.reason = "Punto de entrada documental"
            item.risk = "low"
        classify_item(item, today)
        items.append(item)
    return items


def apply_manual_overrides(items: List[CleanupItem]) -> None:
    for item in items:
        rel = str(item.path)
        if rel in MANUAL_RULES:
            rule = MANUAL_RULES[rel]
            item.classification = rule["classification"]
            item.reason = rule["reason"]
            item.risk = rule["risk"]
            item.proposed_action = rule["proposed_action"]
            item.notes = rule.get("notes", "")


def compute_summary(items: List[CleanupItem]) -> Dict[str, object]:
    totals = defaultdict(int)
    savings_kb = 0.0
    for item in items:
        totals[item.classification.lower()] += 1
        if item.classification in {"REMOVE", "QUARANTINE"}:
            savings_kb += item.size_kb
    return {
        "totals": totals,
        "savings_mb_estimate": round(savings_kb / 1024, 2),
    }


def ensure_relative_paths(items: List[CleanupItem], repo_root: Path) -> None:
    for item in items:
        if isinstance(item.path, Path):
            item.path = item.path  # already relative


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", default="docs/cleanup.json")
    parser.add_argument("--output-md", default="docs/CLEANUP_REPORT.md")
    parser.add_argument("--skip-markdown", action="store_true", help="Only produce JSON output.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]

    items = build_items(repo_root)
    ensure_relative_paths(items, repo_root)
    apply_manual_overrides(items)
    summary = compute_summary(items)

    head_sha = run_cmd(["git", "rev-parse", "HEAD"], cwd=repo_root).stdout.strip()
    meta = {
        "branch": "feature/main-codex",
        "commit": head_sha,
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
    }

    data = {
        "meta": meta,
        "summary": {
            "totals": dict(summary["totals"]),
            "savings_mb_estimate": summary["savings_mb_estimate"],
        },
        "items": [item.to_json() for item in items],
    }

    output_json_path = (repo_root / args.output_json).resolve()
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    artifacts = AuditArtifacts()
    artifacts.broken_links = detect_broken_links(repo_root)
    artifacts.orphan_docs = detect_orphans(repo_root, ".md")
    artifacts.orphan_notebooks = detect_orphans(repo_root, ".ipynb")

    vulture_path = shutil.which("vulture")
    if vulture_path:
        artifacts.vulture_output = run_optional_tool([vulture_path, "src", "pipelines", "scripts", "tests"], repo_root)
    else:
        artifacts.vulture_output = "vulture no disponible"

    deptry_path = shutil.which("deptry")
    if deptry_path:
        artifacts.deptry_output = run_optional_tool([deptry_path, "."], repo_root)
    else:
        artifacts.deptry_output = "deptry no disponible"

    if not args.skip_markdown:
        report_path = (repo_root / args.output_md).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        create_markdown(report_path, repo_root, items, artifacts, summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
