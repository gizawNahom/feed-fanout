"""
Mutation testing gate for the core (domain/ + application/), run in CI and by `make mutation`.

mutmut changes the code in small ways (1 <= len becomes 2 <= len, a message becomes None, ...)
and runs the fast tests against each change. A "surviving" mutant is a change no test
noticed: either a missing test, or a harmless (equivalent) change. Fix the first kind with a
test; mark the second with `# pragma: no mutate` and a comment saying why.

Fails if the score (mutants detected / mutants tested) is below FLOOR, and lists the survivors.
"""

import json
import subprocess
import sys
from pathlib import Path

FLOOR = 1.0  # measured at 100% when introduced; every survivor so far was a real gap


def main() -> int:
    subprocess.run(["mutmut", "run"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["mutmut", "export-cicd-stats"], check=True, stdout=subprocess.DEVNULL)
    stats = json.loads(Path("mutants/mutmut-cicd-stats.json").read_text())

    tested = stats["total"] - stats["skipped"] - stats["no_tests"]
    detected = stats["killed"] + stats["timeout"]  # a timeout means the change broke something
    score = detected / tested if tested else 0.0
    print(
        f"mutation score: {score:.1%} ({detected}/{tested} detected; "
        f"survived {stats['survived']}, no tests {stats['no_tests']}, suspicious {stats['suspicious']})"
    )

    if stats["survived"] or stats["no_tests"]:
        results = subprocess.run(["mutmut", "results"], capture_output=True, text=True).stdout
        print("\nNot detected (inspect with: mutmut show <name>):")
        print("\n".join(line for line in results.splitlines() if "survived" in line or "no tests" in line))

    if score < FLOOR:
        print(f"\nFAIL: below the {FLOOR:.0%} floor")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
