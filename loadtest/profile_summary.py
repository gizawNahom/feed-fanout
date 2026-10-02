"""
Summarize a py-spy profile (speedscope format) of the API: where its CPU time goes.

    python3 loadtest/profile_summary.py loadtest/results/profile-<time>.speedscope.json

Every sample is one stack, taken while a thread held the GIL (doing Python work). Two views:

- **Self time:** the function at the top of the stack, actually executing. Where CPU is burned.
- **Total time:** every function anywhere on the stack. Where CPU is spent *under* a call:
  a middleware's total includes everything it wraps.

Both are shown per library (starlette, sqlalchemy, our feed_fanout, ...) and per function, as
a share of all samples. Standard library only.
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

TOP = 25


def library(file: str) -> str:
    """Which library a frame's file belongs to."""
    if m := re.search(r"/site-packages/([^/]+)", file):
        return m.group(1).removesuffix(".py")
    if m := re.search(r"/(feed_fanout)/", file):
        return "feed_fanout (our code)"
    if m := re.search(r"/lib/python3\.\d+/([^/]+)", file):
        return "stdlib: " + m.group(1).removesuffix(".py")
    if file.startswith("<frozen"):
        return "stdlib: " + file.strip("<>").removeprefix("frozen ")
    if m := re.match(r"<(\w+) generated", file):  # e.g. SQLAlchemy's generated wrapper functions
        return m.group(1)
    return file or "?"


def short(file: str) -> str:
    """A readable path: from the library (or our package) down."""
    for marker in ("/site-packages/", "/app/", "/lib/python3.14/"):
        if marker in file:
            return file.split(marker, 1)[1]
    return file


def summarize(path: Path) -> str:
    data = json.loads(path.read_text())
    frames = data["shared"]["frames"]
    keys = [f"{f['name']} ({short(f.get('file', '?'))})" for f in frames]
    libs = [library(f.get("file", "")) for f in frames]

    total = 0.0  # py-spy weights each sample by the time it stands for, in seconds
    samples = 0
    self_fn: Counter[str] = Counter()
    self_lib: Counter[str] = Counter()
    total_fn: Counter[str] = Counter()
    total_lib: Counter[str] = Counter()
    for profile in data["profiles"]:
        for stack, weight in zip(profile["samples"], profile["weights"], strict=True):
            if not stack:
                continue
            total += weight
            samples += 1
            leaf = stack[-1]
            self_fn[keys[leaf]] += weight
            self_lib[libs[leaf]] += weight
            for key in {keys[i] for i in stack}:  # once per sample, even if recursive
                total_fn[key] += weight
            for lib in {libs[i] for i in stack}:
                total_lib[lib] += weight

    if not total:
        return f"# Profile: {path.name}\n\nNo samples: was the API busy while it was recorded?\n"

    def table(title: str, counts: Counter[str], what: str, n: int = TOP) -> list[str]:
        rows = [f"## {title}", "", f"| {what} | Share of CPU |", "|---|---|"]
        rows += [f"| `{k}` | {v / total:.1%} |" for k, v in counts.most_common(n)]
        return [*rows, ""]

    lines = [
        f"# Profile: {path.name}",
        "",
        f"{samples} samples (~{total:.0f} s of CPU) across {len(data['profiles'])} thread(s), each taken while"
        " a thread held the GIL.",
        "",
        *table("Self time by library: where CPU is burned", self_lib, "Library", n=15),
        *table("Total time by library: CPU spent in or under its code", total_lib, "Library", n=15),
        *table("Self time by function", self_fn, "Function"),
        *table("Total time by function", total_fn, "Function"),
    ]
    return "\n".join(lines)


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    print(summarize(Path(sys.argv[1])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
