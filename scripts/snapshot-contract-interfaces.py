#!/usr/bin/env python3
"""Write the snapshot of the contracts' interfaces that dipper's own declarations are tested against.

Dipper declares the contract functions, events and errors it uses by hand, most of them in
bin/dipper-service/src/chain_client/abi.rs. Tests there compare each declaration with the
snapshot this script writes, so a declaration that has drifted from the contracts fails the build.

Run it from the root of this repo, giving it a graphprotocol/contracts checkout with its
dependencies installed (`pnpm install`). It needs `forge`:

    scripts/snapshot-contract-interfaces.py ~/github/contracts

The script compiles the contracts itself, into a temporary folder, and refuses a checkout with
uncommitted changes, so the snapshot always holds what the commit it names compiles to.
"""

import argparse
import json
import pathlib
import subprocess
import sys
import tempfile

SNAPSHOT = pathlib.Path("bin/dipper-service/src/chain_client/contract_interfaces.txt")

# Each contract dipper calls: the package that holds it, and what to compile it from. An interface
# is listed beside its implementation because only the interface names every parameter.
SECTIONS = {
    "RecurringCollector": ("horizon", ["IRecurringCollector", "RecurringCollector"]),
    "RecurringAgreementManager": (
        "issuance",
        [
            "IRecurringAgreementManagement",
            "IRecurringAgreements",
            "IRecurringEscrowManagement",
            "RecurringAgreementManager",
        ],
    ),
}


def kind_of(item):
    """A parameter's type the way the snapshot writes it, a struct as a tuple of its fields."""
    kind = item["type"]
    if kind.startswith("tuple"):
        fields = ", ".join(f"{kind_of(field)} {field['name']}" for field in item["components"])
        kind = f"({fields}){kind[len('tuple'):]}"
    return kind


def declarations(abis):
    """Every function, event and error of the compiled contracts, 1 line each.

    A contract and its interface often name different parameters of the same function, so
    entries that agree on everything but names are merged, each parameter taking the name
    either gives it.
    """
    merged = {}
    for abi in abis:
        for entry in abi:
            kind = entry["type"]
            if kind not in ("function", "event", "error"):
                continue
            inputs = [
                (kind_of(item) + (" indexed" if item.get("indexed") else ""), item.get("name", ""))
                for item in entry["inputs"]
            ]
            outputs = [(kind_of(item), item.get("name", "")) for item in entry.get("outputs", [])]
            head = f"{kind} {entry['name']}"
            tail = ""
            if entry.get("stateMutability") in ("view", "pure"):
                tail = f" {entry['stateMutability']}"
            shape = (head, tail, tuple(k for k, _ in inputs), tuple(k for k, _ in outputs))
            variants = merged.setdefault(shape, [])
            for variant in variants:
                names = variant[0] + variant[1]
                if all(not a or not b or a == b for (_, a), (_, b) in zip(names, inputs + outputs)):
                    variant[0][:] = [(k, a or b) for (k, a), (_, b) in zip(variant[0], inputs)]
                    variant[1][:] = [(k, a or b) for (k, a), (_, b) in zip(variant[1], outputs)]
                    break
            else:
                variants.append((inputs, outputs))

    def listed(parameters):
        return ", ".join(f"{kind} {name}".strip() for kind, name in parameters)

    lines = set()
    for (head, tail, _, _), variants in merged.items():
        for inputs, outputs in variants:
            line = f"{head}({listed(inputs)}){tail}"
            if outputs:
                line += f" returns ({listed(outputs)})"
            lines.add(line)
    return lines


def compiled(contracts, package, out):
    """Compile 1 package of the contracts checkout into `out`, leaving the checkout untouched."""
    subprocess.run(
        ["forge", "build", "--skip", "test", "--skip", "script"]
        + ["--out", str(out / "out"), "--cache-path", str(out / "cache")],
        cwd=contracts / "packages" / package,
        check=True,
    )
    return out / "out"


def git(contracts, *args):
    result = subprocess.run(
        ["git", "-C", str(contracts), *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("contracts", type=pathlib.Path, help="a graphprotocol/contracts checkout")
    contracts = parser.parse_args().contracts.resolve()

    if git(contracts, "status", "--porcelain"):
        sys.exit(f"{contracts} has uncommitted changes: the snapshot must match a commit")
    commit = git(contracts, "rev-parse", "HEAD")

    text = [
        "# The functions, events and errors of the contracts dipper calls, as compiled from",
        f"# graphprotocol/contracts at {commit}.",
        "# Written by scripts/snapshot-contract-interfaces.py: do not edit it by hand.",
        "# The declarations in the chain client are tested against it.",
    ]
    with tempfile.TemporaryDirectory() as scratch:
        built = {}
        for section, (package, names) in SECTIONS.items():
            if package not in built:
                built[package] = compiled(contracts, package, pathlib.Path(scratch) / package)
            abis = [
                json.loads((built[package] / f"{name}.sol" / f"{name}.json").read_text())["abi"]
                for name in names
            ]
            text += ["", f"[{section}]", *sorted(declarations(abis))]
    SNAPSHOT.write_text("\n".join(text) + "\n")
    print(f"wrote {SNAPSHOT} from contracts {commit[:8]}")


if __name__ == "__main__":
    main()
