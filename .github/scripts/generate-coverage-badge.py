#!/usr/bin/env python3
"""Generate a self-hosted coverage badge (SVG) from JaCoCo's CSV reports.

Reads every module's target/site/jacoco/jacoco.csv, computes the total
instruction coverage across all modules, and writes a shields-style flat
badge. The badge is published on the docs site by the Docs workflow and
referenced from the README - so the number the README shows is always
generated from a real test run, never hand-maintained.

Usage: python3 .github/scripts/generate-coverage-badge.py \
           [output_svg] [jacoco_csv...]
"""

import csv
import sys
from pathlib import Path

OUT_PATH = Path(sys.argv[1] if len(sys.argv) > 1 else "site/coverage/badge.svg")
CSV_PATHS = [Path(p) for p in sys.argv[2:]] or sorted(
    Path(".").glob("*/target/site/jacoco/jacoco.csv")
)

# shields.io flat-style colors, by coverage threshold.
COLORS = [(90, "#4c1"), (80, "#97ca00"), (70, "#a4a61d"), (60, "#dfb317"), (50, "#fe7d37"), (0, "#e05d44")]

BADGE = """<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="20" role="img" aria-label="coverage: {pct}%">
  <title>coverage: {pct}%</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r"><rect width="{width}" height="20" rx="3" fill="#fff"/></clipPath>
  <g clip-path="url(#r)">
    <rect width="{label_w}" height="20" fill="#555"/>
    <rect x="{label_w}" width="{value_w}" height="20" fill="{color}"/>
    <rect width="{width}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="110" text-rendering="geometricPrecision">
    <text x="{label_x}" y="150" transform="scale(.1)" fill="#010101" fill-opacity=".3" textLength="{label_tl}">coverage</text>
    <text x="{label_x}" y="140" transform="scale(.1)" textLength="{label_tl}">coverage</text>
    <text x="{value_x}" y="150" transform="scale(.1)" fill="#010101" fill-opacity=".3" textLength="{value_tl}">{pct}%</text>
    <text x="{value_x}" y="140" transform="scale(.1)" textLength="{value_tl}">{pct}%</text>
  </g>
</svg>
"""


def main() -> None:
    if not CSV_PATHS:
        sys.exit("error: no JaCoCo CSV reports found - run `mvn verify` first")
    missed = covered = 0
    for csv_path in CSV_PATHS:
        with csv_path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                missed += int(row["INSTRUCTION_MISSED"])
                covered += int(row["INSTRUCTION_COVERED"])
    total = missed + covered
    if total == 0:
        sys.exit("error: no instructions found in the JaCoCo CSVs")
    pct = round(100.0 * covered / total)
    color = next(c for threshold, c in COLORS if pct >= threshold)

    label_w = 61
    value_w = 24 + 8 * len(f"{pct}%")
    width = label_w + value_w
    svg = BADGE.format(
        width=width,
        label_w=label_w,
        value_w=value_w,
        color=color,
        pct=pct,
        label_x=label_w * 5,
        label_tl=(label_w - 10) * 10,
        value_x=(label_w + value_w / 2) * 10,
        value_tl=(value_w - 10) * 10,
    )
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(svg, encoding="utf-8")
    print(f"wrote {OUT_PATH}: coverage {pct}% ({covered}/{total} instructions)")


if __name__ == "__main__":
    main()
