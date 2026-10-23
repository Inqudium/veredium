#!/usr/bin/env python3
"""Generate the "Test evidence" docs page from Surefire results.

Reads every module's target/surefire-reports/TEST-*.xml (the
authoritative record of what actually ran) and enriches each test with
its rationale block extracted from the test sources
(*/src/test/kotlin/**/*Test.kt, the abstract *Contract.kt suites the
per-engine integration tests inherit from, and the Java tests under
*/src/test/java/**/*Test.java - the Jazzer targets among them): the "What is tested / Success
criteria / Why it matters" comment run at the top of the test body. A
test class that extends a contract (`class X : YContract()`) inherits
the contract's rationales for the methods it does not declare itself. Writes
docs/tests/test-evidence.md, which the Docs workflow feeds into MkDocs.

The page is GENERATED - it must never be checked in or edited by hand,
otherwise it becomes a drifting mirror of the suite. MkDocs' --strict
build fails if this script did not run first, which is the intended
guard.

Usage: python3 .github/scripts/generate-test-catalog.py [output_md]
       python3 .github/scripts/generate-test-catalog.py --check

--check reads the test sources of every module only - no Surefire reports needed - and fails on a
rationale block that breaks the CONTRIBUTING.md shape: a missing label, the labels out of order, a
fourth label, or no bare `//` line between the block and the first Given/When/Then stage line. The
CI job runs it on every pull request; the page generation applies the same checks.
"""

import html
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

CHECK_ONLY = sys.argv[1:] == ["--check"]
OUTPUT = Path(sys.argv[1] if len(sys.argv) > 1 and not CHECK_ONLY else "docs/tests/test-evidence.md")

# Kotlin test methods carry backticked sentence names (with modifiers such as
# `internal` in front, for a parameterized test over an internal type); the
# Java tests are plain `void name(...)` methods.
FUN_RE = re.compile(r"^\s*(?:\w+\s+)*fun `([^`]+)`\(")
JAVA_FUN_RE = re.compile(r"^\s*(?:public\s+)?void\s+(\w+)\(")
# Surefire names a Jazzer regression invocation `method(FuzzedDataProvider)[n]` and a
# parameterized Kotlin test `method$module(Type)[n]` (an `internal fun` is name-mangled
# with the module name); the rationale is keyed by the bare method name.
INVOCATION_SUFFIX_RE = re.compile(r"(\$\w+)?\(.*$")
# `class JettyConnectorIntegrationTest : ConnectorContract() {` - the subclass runs the
# contract's tests; Surefire reports them under the subclass name.
SUPERCLASS_RE = re.compile(r"^(?:abstract\s+)?class\s+(\w+)[^:\n]*:\s*(\w+)\(")
QUESTIONS = [
    ("What is tested?", re.compile(r"What is tested:")),
    ("How is success determined?", re.compile(r"Success criteria:")),
    ("Why does it matter?", re.compile(r"Why it matters:")),
]
# The rationale run is directly followed by the Given/When/Then comments; everything
# from the first stage line on is test structure, not rationale. A stage line is a
# comment consisting of stage labels only (`Given`, `Given/When/Then`, `Given / When`),
# optionally qualified `(...)` and optionally followed by `: text` - CONTRIBUTING's
# spelling is the bare label, the colon form is equally common in the suites. Matched
# per comment line, so a rationale sentence that happens to start with "When ..." is
# never mistaken for a stage label (it carries more words before any colon).
STAGE_LABEL = r"(?:Given|When|Then|And)"
STAGE_LINE_RE = re.compile(
    rf"^{STAGE_LABEL}(?:\s*/\s*{STAGE_LABEL})*\s*(?:\([^)]*\))?\s*(?::.*)?$"
)
# A line of the block that opens like a label of its own (`Why the second shape:`) - the three labels are
# the whole contract, a fourth one is not read and would vanish from the evidence page.
LABEL_LINE_RE = re.compile(r"^[A-Z][A-Za-z' ]{2,40}:(\s|$)")


def extract_rationales(module: Path) -> tuple[dict, dict]:
    """Map (top-level test class, test method name) -> {question: text},
    plus the map of test class -> superclass for contract inheritance.

    The rationale block is the run of '//' comment lines directly after
    the test function's opening line, cut off at the first Given/When/
    Then stage line (with or without a colon); method names are
    sentence-shaped and unique per file, so the file's class name plus
    the method name is a sufficient key. Kotlin tests under src/test/kotlin and the Java fuzz targets
    under src/test/java are read alike.

    A detected rationale must answer all three questions (the
    CONTRIBUTING.md contract); a partial block fails the run with the
    exact file and test, so incomplete evidence can never be published
    as documented.
    """
    rationales = {}
    superclasses = {}
    incomplete = []
    kotlin = module / "src" / "test" / "kotlin"
    sources = [(kt, FUN_RE) for kt in sorted(set(kotlin.rglob("*Test.kt")) | set(kotlin.rglob("*Contract.kt")))]
    sources += [(java, JAVA_FUN_RE) for java in sorted((module / "src" / "test" / "java").rglob("*Test.java"))]
    for kt, fun_re in sources:
        clazz = kt.stem
        lines = kt.read_text(encoding="utf-8").splitlines()
        for line in lines:
            m = SUPERCLASS_RE.match(line)
            if m:
                superclasses[m.group(1)] = m.group(2)
        for i, line in enumerate(lines):
            m = fun_re.match(line)
            if not m:
                continue
            # A parameterized test can spread its parameter list over several
            # lines; the body - and the rationale block at its top - starts
            # after the line that opens it.
            start = i
            while not lines[start].rstrip().endswith("{") and start + 1 < len(lines):
                start += 1
            comment = []
            raw = []
            reached_stage = False
            for follow in lines[start + 1:]:
                stripped = follow.strip()
                if stripped.startswith("//"):
                    body = stripped[2:].strip()
                    if STAGE_LINE_RE.match(body):
                        reached_stage = True
                        break
                    comment.append(body)
                    raw.append(stripped[2:])
                elif stripped == "" and not comment:
                    continue
                else:
                    break
            text = " ".join(comment)
            parsed = {}
            spans = [(q.search(text), label) for label, q in QUESTIONS]
            spans = [(mm.start(), mm.end(), label) for mm, label in spans if mm]
            spans.sort()
            for idx, (start, end, label) in enumerate(spans):
                stop = spans[idx + 1][0] if idx + 1 < len(spans) else len(text)
                answer = text[end:stop].strip()
                if answer:
                    parsed[label] = answer
            if parsed:
                where = f"{kt}: `{m.group(1)}`"
                if len(parsed) < len(QUESTIONS):
                    missing = [q.pattern for label, q in QUESTIONS if label not in parsed]
                    incomplete.append(f"{where} lacks {', '.join(missing)}")
                elif [label for _, _, label in spans] != [label for label, _ in QUESTIONS]:
                    incomplete.append(f"{where} has its labels out of order")
                extra = [
                    line.strip()
                    for line in raw
                    if not line.startswith("   ")
                    and LABEL_LINE_RE.match(line.strip())
                    and not any(q.match(line.strip()) for _, q in QUESTIONS)
                ]
                if extra:
                    incomplete.append(f"{where} has a label of its own: {extra[0]!r}")
                if reached_stage and comment and comment[-1] != "":
                    incomplete.append(f"{where} lacks the bare `//` line before its first stage line")
                rationales[(clazz, m.group(1))] = parsed
    if incomplete:
        sys.exit(
            "error: test rationale blocks that break the CONTRIBUTING.md shape (the three labels, in"
            " order, then a bare `//` line):\n  " + "\n  ".join(incomplete)
        )
    return rationales, superclasses


def find_rationale(rationales: dict, superclasses: dict, clazz: str, name: str):
    """The rationale of `clazz.name`, inherited along the superclass chain when the
    class does not declare the method itself; the Jazzer invocation suffix is stripped."""
    bare = INVOCATION_SUFFIX_RE.sub("", name)
    seen = set()
    while clazz and clazz not in seen:
        seen.add(clazz)
        found = rationales.get((clazz, name)) or rationales.get((clazz, bare))
        if found:
            return found
        clazz = superclasses.get(clazz)
    return None


def load_suites(reports_dir: Path):
    """Group testcases: top-level class -> nested group -> [test names]."""
    suites = defaultdict(lambda: defaultdict(list))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "time": 0.0}
    times = defaultdict(float)
    for report in sorted(reports_dir.glob("TEST-*.xml")):
        root = ET.parse(report).getroot()
        totals["tests"] += int(root.get("tests", 0))
        totals["failures"] += int(root.get("failures", 0))
        totals["errors"] += int(root.get("errors", 0))
        totals["skipped"] += int(root.get("skipped", 0))
        totals["time"] += float(root.get("time", 0.0))
        for case in root.iter("testcase"):
            classname = case.get("classname", "")
            simple = classname.rsplit(".", 1)[-1]
            top, _, group = simple.partition("$")
            suites[top][group].append(case.get("name", ""))
            times[top] += float(case.get("time", 0.0))
    return suites, totals, times


def md_escape(text: str) -> str:
    return html.escape(text, quote=False)


def main() -> None:
    if CHECK_ONLY:
        for module in sorted(p.parts[0] for p in Path(".").glob("*/src/test")):
            extract_rationales(Path(module))
        print("test rationale blocks: ok")
        return
    modules = sorted(p.parts[0] for p in Path(".").glob("*/target/surefire-reports"))
    if not modules:
        sys.exit("error: no target/surefire-reports found in any module - run `mvn verify` first")

    grand = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "time": 0.0}
    per_module = {}
    for module in modules:
        rationales, superclasses = extract_rationales(Path(module))
        suites, totals, times = load_suites(Path(module) / "target" / "surefire-reports")
        per_module[module] = (suites, totals, times, rationales, superclasses)
        for key in grand:
            grand[key] += totals[key]

    # Anchors the way python-markdown's toc extension assigns them: the
    # slug of the heading text, with _1, _2, ... appended for repeats in
    # DOCUMENT order. Two modules may contain equally named test classes,
    # so the summary table must link the exact per-module anchor, not just
    # the slug of the first occurrence.
    anchors = {}
    seen = defaultdict(int)
    for module in modules:
        for top in sorted(per_module[module][0]):
            slug = top.lower()
            anchors[(module, top)] = slug if seen[slug] == 0 else f"{slug}_{seen[slug]}"
            seen[slug] += 1

    out = []
    out.append("# Test evidence")
    out.append("")
    out.append(
        "> Generated by the Docs workflow from the Surefire results of the"
        " test run and the rationale comments in the test sources. Do not"
        " edit by hand."
    )
    out.append("")
    out.append(
        f"The run executed **{grand['tests']} tests**"
        f" ({grand['failures']} failures, {grand['errors']} errors,"
        f" {grand['skipped']} skipped) in {grand['time']:.1f}s across the"
        f" {len(modules)} modules listed below, without Docker or any external"
        " service - see"
        " [CONTRIBUTING](https://github.com/Inqudium/veredium/blob/main/CONTRIBUTING.md)."
    )
    out.append("")
    out.append("| Module | Component under test | Tests | Time |")
    out.append("|---|---|---:|---:|")
    for module in modules:
        suites, _, times, _, _ = per_module[module]
        for top in sorted(suites, key=lambda t: -sum(len(v) for v in suites[t].values())):
            count = sum(len(v) for v in suites[top].values())
            out.append(f"| `{module}` | [`{top}`](#{anchors[(module, top)]}) | {count} | {times[top]:.1f}s |")
    out.append("")

    for module in modules:
        suites, totals, _, rationales, superclasses = per_module[module]
        out.append(f"## {module}")
        out.append("")
        out.append(f"{totals['tests']} tests.")
        out.append("")
        for top in sorted(suites):
            count = sum(len(v) for v in suites[top].values())
            out.append(f"### {top}")
            out.append("")
            out.append(f"{count} tests.")
            out.append("")
            for group in sorted(suites[top], key=lambda g: (g == "", g)):
                if group:
                    out.append(f"#### {md_escape(group)}")
                    out.append("")
                for name in sorted(suites[top][group]):
                    out.append(f"**{md_escape(name)}**")
                    out.append("")
                    rationale = find_rationale(rationales, superclasses, top, name)
                    if rationale:
                        out.append('??? quote "Rationale"')
                        for label, answer in rationale.items():
                            out.append(f"    **{label}** {md_escape(answer)}")
                            out.append("")
                        out.append("")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text("\n".join(out) + "\n", encoding="utf-8")
    documented = sum(
        1
        for module in modules
        for top in per_module[module][0]
        for g in per_module[module][0][top]
        for n in per_module[module][0][top][g]
        if find_rationale(per_module[module][3], per_module[module][4], top, n)
    )
    print(f"wrote {OUTPUT}: {grand['tests']} tests, {documented} with rationale blocks")


if __name__ == "__main__":
    main()
