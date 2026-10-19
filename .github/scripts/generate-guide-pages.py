#!/usr/bin/env python3
"""Copy the module guide documents into the MkDocs docs_dir for the site build.

The long-form guides live next to their module sources
(<module>/docs/GUIDE.md, plus any further guide documents a module
configures below) so they stay close to the code they describe. So that the site renders them instead of linking to GitHub,
this script copies each configured document to docs/guides/<module>/ -
along with every same-module NON-MARKDOWN file it references (the
activity diagram, the module-local configuration reference) - and
rewrites the relative links: targets already inside the docs_dir become
site-relative, another module's guide document becomes its generated
page, a same-directory markdown link stays as-is (both files become
pages side by side), and everything else (module READMEs, sources)
becomes a GitHub link.

The pages are GENERATED - they must never be checked in or edited by
hand (docs/guides/ is gitignored), otherwise they become a drifting
mirror of the module guides. MkDocs' --strict build fails if this
script did not run first, which is the intended guard.

Usage: python3 .github/scripts/generate-guide-pages.py
"""

import os
import re
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
OUT_BASE = DOCS / "guides"
MODULE_DOCS = {
    "veredium-core": ["GUIDE.md"],
}
# A guide shared by several modules would live in the docs_dir itself (docs/GUIDE.md), render on
# GitHub with relative links to the module guides and READMEs, be excluded from the build
# (mkdocs.yml exclude_docs) and generated to docs/guides/GUIDE.md like a module guide with
# src_dir = docs/ - the sibling project legatium does so. None yet.
SHARED_DOCS: list[str] = []
GITHUB_BLOB = "https://github.com/Inqudium/veredium/blob/main/"

# Inline markdown links: ](target) or ](target#anchor). The guides use
# no reference-style links or raw HTML links (a new one would surface as
# an unrewritten dead link in --strict).
LINK_RE = re.compile(r"\]\(([^)#]+)(#[^)]*)?\)")

# Every (module, doc) pair that becomes a page - so cross-module links
# to any of them can target the generated page instead of GitHub.
PAGE_SOURCES = {
    (REPO / module / "docs" / doc).resolve(): (module, doc)
    for module, docs in MODULE_DOCS.items()
    for doc in docs
}
PAGE_SOURCES.update({(DOCS / doc).resolve(): ("", doc) for doc in SHARED_DOCS})


def rewrite_doc(module: str, doc: str) -> None:
    src_dir = (REPO / module / "docs") if module else DOCS
    src = src_dir / doc
    out_dir = (OUT_BASE / module) if module else OUT_BASE
    out_dir.mkdir(parents=True, exist_ok=True)
    referenced_assets: set[Path] = set()

    def rewrite(match: re.Match) -> str:
        target, anchor = match.group(1), match.group(2) or ""
        if target.startswith(("http://", "https://", "mailto:")):
            return match.group(0)
        resolved = (src_dir / target).resolve()
        if not resolved.exists():
            sys.exit(f"{src}: relative link target does not exist: {target}")
        if resolved in PAGE_SOURCES and PAGE_SOURCES[resolved][0] == "":
            # The shared guide: its generated copy is the page, not its excluded source.
            new = os.path.relpath(OUT_BASE / PAGE_SOURCES[resolved][1], out_dir)
        elif resolved.is_relative_to(DOCS):
            # Already a site page/file: point at it site-relatively.
            new = os.path.relpath(resolved, out_dir)
        elif resolved.parent == src_dir and resolved.suffix != ".md":
            # Same-module companion file (diagram, config reference):
            # keep the same-directory link and copy the file alongside.
            referenced_assets.add(resolved)
            return match.group(0)
        elif resolved.parent == src_dir and resolved in PAGE_SOURCES:
            # A sibling guide document of the SAME module: both become
            # pages in the same output directory - the link holds as-is.
            return match.group(0)
        elif resolved in PAGE_SOURCES:
            # A guide document of ANOTHER module: link its generated page.
            other_module, other_doc = PAGE_SOURCES[resolved]
            new = os.path.relpath(OUT_BASE / other_module / other_doc, out_dir)
        else:
            # Everything else (module READMEs, sources) is not on the
            # site; send the reader to the file on GitHub.
            new = GITHUB_BLOB + resolved.relative_to(REPO).as_posix()
        return f"]({new}{anchor})"

    rewritten = LINK_RE.sub(rewrite, src.read_text(encoding="utf-8"))
    (out_dir / doc).write_text(rewritten, encoding="utf-8")
    for asset in referenced_assets:
        shutil.copy(asset, out_dir / asset.name)
    print(f"{out_dir / doc}: written, {len(referenced_assets)} companion file(s)")


def main() -> None:
    if OUT_BASE.exists():
        shutil.rmtree(OUT_BASE)
    for doc in SHARED_DOCS:
        rewrite_doc("", doc)
    for module, docs in MODULE_DOCS.items():
        for doc in docs:
            rewrite_doc(module, doc)


if __name__ == "__main__":
    main()
