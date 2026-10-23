#!/usr/bin/env python3
"""Fail on an unused import in any Kotlin source of the modules.

ktlint 1.8 does not report unused imports, and the Kotlin compiler does not either; an import whose name
only appears in a `//` comment is dead noise that looks like use. A name counts as used when it appears in
the code or in a KDoc comment - a KDoc `[Symbol]` link resolves through the imports, Dokka checks it.

Usage: python3 .github/scripts/check-unused-imports.py
"""

import re
import sys
from pathlib import Path

IMPORT_RE = re.compile(r"^import ([\w.`]+)(?: as (\w+))?\s*$", re.MULTILINE)
LINE_COMMENT_RE = re.compile(r"//.*$", re.MULTILINE)
STRING_RE = re.compile(r'"(?:\\.|[^"\\])*"')


def unused(source: str) -> list[str]:
    body = IMPORT_RE.sub("", source)
    # String contents and `//` comments do not use an import; KDoc does (its links resolve through them).
    code = LINE_COMMENT_RE.sub("", STRING_RE.sub('""', body))
    found = []
    for match in IMPORT_RE.finditer(source):
        name = (match.group(2) or match.group(1).split(".")[-1]).strip("`")
        if name == "*":
            continue
        if not re.search(rf"\b{re.escape(name)}\b", code):
            found.append(match.group(0).strip())
    return found


def main() -> None:
    problems = []
    for path in sorted(Path(".").glob("veredium-*/src/**/*.kt")):
        for line in unused(path.read_text(encoding="utf-8")):
            problems.append(f"{path}: {line}")
    if problems:
        sys.exit("error: unused imports:\n  " + "\n  ".join(problems))
    print("unused imports: none")


if __name__ == "__main__":
    main()
