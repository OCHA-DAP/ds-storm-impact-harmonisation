#!/usr/bin/env python3
"""Assemble the GitHub Pages site from per-product manifests.

Each product owns one directory under ``pages/products/`` containing a
``page.toml``. This script discovers them by globbing, validates them, copies
hosted content into the output tree, and renders the landing page.

The point of the layout is that adding a product creates files no other branch
touches, so contributions merge without conflict. There is deliberately no
central list of products to edit.

Standard library only (``tomllib`` is stdlib on the 3.11+ this repo requires),
so CI can run it without installing anything.

Usage:
    python pages/_build/assemble.py --out site
"""

from __future__ import annotations

import argparse
import html
import shutil
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCTS_DIR = REPO_ROOT / "pages" / "products"
TEMPLATE = Path(__file__).parent / "template.html"

VALID_STATUS = {"active", "deprecated"}
VALID_SOURCE = {"repo", "local"}


class ManifestError(Exception):
    """A manifest is invalid. Message names the offending file."""


@dataclass
class Product:
    slug: str          # directory name, used only for error messages
    title: str
    blurb: str
    order: int
    status: str
    path: str | None   # URL path for hosted products, None for external
    url: str | None    # absolute URL for external products
    url_label: str | None
    src_dir: Path | None
    include: list[str] | None

    @property
    def is_external(self) -> bool:
        return self.url is not None

    @property
    def href(self) -> str:
        return self.url if self.is_external else f"{self.path}/"

    @property
    def foot_label(self) -> str:
        if self.is_external:
            return f"{self.url_label} ↗" if self.url_label else "external ↗"
        return f"/{self.path}/"


def _require(data: dict, key: str, manifest: Path) -> object:
    if key not in data:
        raise ManifestError(f"{manifest}: missing required key '{key}'")
    return data[key]


def load_manifest(manifest: Path) -> Product:
    """Parse and validate one page.toml into a Product."""
    with manifest.open("rb") as fh:
        try:
            data = tomllib.load(fh)
        except tomllib.TOMLDecodeError as exc:
            raise ManifestError(f"{manifest}: invalid TOML - {exc}") from exc

    slug = manifest.parent.name
    title = str(_require(data, "title", manifest))
    blurb = str(_require(data, "blurb", manifest))
    order = int(data.get("order", 100))

    status = str(data.get("status", "active"))
    if status not in VALID_STATUS:
        raise ManifestError(
            f"{manifest}: status '{status}' must be one of {sorted(VALID_STATUS)}"
        )

    url = data.get("url")
    path = data.get("path")
    source = data.get("source")

    # A product is either external (a link out) or hosted (files we copy).
    if url and path:
        raise ManifestError(f"{manifest}: set 'url' or 'path', not both")
    if not url and not path:
        raise ManifestError(f"{manifest}: needs 'url' (external) or 'path' (hosted)")

    src_dir = None
    include = None

    if url:
        if source:
            raise ManifestError(f"{manifest}: external products take no [source]")
    else:
        # Nested paths are allowed ("slides/aac") so related products can share a
        # prefix. Each segment must be a plain directory name — no traversal, no
        # dotfiles, nothing that could write outside the output tree.
        path = str(path).strip("/")
        segments = path.split("/") if path else []
        if not segments or any(
            not s or s.startswith(".") or s in {".", ".."} or "\\" in s
            for s in segments
        ):
            raise ManifestError(
                f"{manifest}: path '{path}' must be one or more plain directory names"
            )
        if not source:
            raise ManifestError(f"{manifest}: hosted products need a [source] table")

        stype = str(_require(source, "type", manifest))
        if stype not in VALID_SOURCE:
            raise ManifestError(
                f"{manifest}: source.type '{stype}' must be one of {sorted(VALID_SOURCE)}"
            )

        raw_dir = str(source.get("dir", "."))
        base = REPO_ROOT if stype == "repo" else manifest.parent
        src_dir = (base / raw_dir).resolve()

        if not src_dir.is_dir():
            raise ManifestError(f"{manifest}: source.dir '{raw_dir}' not found at {src_dir}")
        if REPO_ROOT not in src_dir.parents and src_dir != REPO_ROOT:
            raise ManifestError(f"{manifest}: source.dir escapes the repo ({src_dir})")

        include = [str(i) for i in source.get("include", [])] or None

    return Product(
        slug=slug,
        title=title,
        blurb=blurb,
        order=order,
        status=status,
        path=path if not url else None,
        url=str(url) if url else None,
        url_label=str(data["url_label"]) if "url_label" in data else None,
        src_dir=src_dir,
        include=include,
    )


def discover() -> list[Product]:
    """Load every manifest, failing loudly on duplicates or invalid files."""
    manifests = sorted(PRODUCTS_DIR.glob("*/page.toml"))
    if not manifests:
        raise ManifestError(f"no page.toml found under {PRODUCTS_DIR}")

    products = [load_manifest(m) for m in manifests]

    seen: dict[str, str] = {}
    for p in products:
        if p.path is None:
            continue
        if p.path in seen:
            raise ManifestError(
                f"path collision: '{p.path}' claimed by both "
                f"'{seen[p.path]}' and '{p.slug}'"
            )
        seen[p.path] = p.slug

    return sorted(products, key=lambda p: (p.order, p.title))


def copy_hosted(product: Product, out: Path) -> None:
    """Copy a hosted product's files into out/<path>/."""
    dest = out / product.path
    dest.mkdir(parents=True, exist_ok=True)

    names = product.include or [c.name for c in product.src_dir.iterdir()]
    for name in names:
        if name == "page.toml":
            continue
        src = product.src_dir / name
        if not src.exists():
            raise ManifestError(f"{product.slug}: include '{name}' not found in {product.src_dir}")
        if src.is_dir():
            shutil.copytree(src, dest / name, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest / name)


def render_card(p: Product) -> str:
    e = html.escape
    classes = "k dep" if p.status == "deprecated" else "k"
    chip = ""
    if p.is_external:
        chip = '<span class="chip chip-ext">External</span>'
    elif p.status == "deprecated":
        chip = '<span class="chip chip-dep">Deprecated</span>'
    target = ' target="_blank" rel="noopener"' if p.is_external else ""

    return (
        f'<a class="{classes}" href="{e(p.href)}"{target}>\n'
        f'  <h2>{e(p.title)}{chip}</h2>\n'
        f'  <p>{e(p.blurb)}</p>\n'
        f'  <span class="foot"><em>{e(p.foot_label)}</em></span>\n'
        f'</a>'
    )


def render_landing(products: list[Product], out: Path) -> None:
    cards = "\n".join(render_card(p) for p in products)
    page = TEMPLATE.read_text(encoding="utf-8").replace("<!--CARDS-->", cards)
    (out / "index.html").write_text(page, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="site", help="output directory (default: site)")
    args = ap.parse_args()

    out = Path(args.out).resolve()

    try:
        products = discover()
    except ManifestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    try:
        for p in products:
            if p.is_external:
                print(f"  link   {p.title} -> {p.url}")
            else:
                copy_hosted(p, out)
                print(f"  copy   {p.title} -> /{p.path}/")
        render_landing(products, out)
    except ManifestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    hosted = sum(1 for p in products if not p.is_external)
    print(f"assembled {len(products)} products ({hosted} hosted) into {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
