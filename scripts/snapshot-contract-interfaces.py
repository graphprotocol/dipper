#!/usr/bin/env python3
"""Write the snapshot of the contracts' interfaces that dipper's own declarations are tested against.

Dipper declares the contract functions, events and errors it uses by hand, in
bin/dipper-service/src/chain_client/abi.rs. A test there compares each declaration with the
snapshot this script writes, so a declaration that has drifted from the contracts fails the build.

Run it from the root of this repo after building the contracts (`forge build` in
packages/horizon and packages/issuance of a graphprotocol/contracts checkout):

    scripts/snapshot-contract-interfaces.py ~/github/contracts

Pass --horizon-out and --issuance-out when the build output is somewhere other than the
packages' own `build` and `forge-artifacts` folders.
"""

import argparse
import json
import pathlib
import subprocess

SNAPSHOT = pathlib.Path("bin/dipper-service/src/chain_client/contract_interfaces.txt")

# Each interface dipper declares, and the compiled contracts it is a subset of. An interface is
# listed beside its implementation because only the interface names every parameter.
SECTIONS = {
    "IRecurringCollector": ("horizon", ["IRecurringCollector", "RecurringCollector"]),
    "IRecurringAgreementManager": (
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


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("contracts", type=pathlib.Path, help="a graphprotocol/contracts checkout")
    parser.add_argument("--horizon-out", type=pathlib.Path, help="build output of packages/horizon")
    parser.add_argument("--issuance-out", type=pathlib.Path, help="build output of packages/issuance")
    args = parser.parse_args()

    out = {
        "horizon": args.horizon_out or args.contracts / "packages/horizon/build",
        "issuance": args.issuance_out or args.contracts / "packages/issuance/forge-artifacts",
    }
    commit = subprocess.run(
        ["git", "-C", str(args.contracts), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    text = [
        "# The functions, events and errors of the contracts dipper calls, as compiled from",
        f"# graphprotocol/contracts at {commit}.",
        "# Written by scripts/snapshot-contract-interfaces.py: do not edit it by hand.",
        "# The declarations in abi.rs are tested against it.",
    ]
    for section, (package, contracts) in SECTIONS.items():
        abis = [
            json.loads((out[package] / f"{contract}.sol" / f"{contract}.json").read_text())["abi"]
            for contract in contracts
        ]
        text += ["", f"[{section}]", *sorted(declarations(abis))]
    SNAPSHOT.write_text("\n".join(text) + "\n")
    print(f"wrote {SNAPSHOT} from contracts {commit[:8]}")


if __name__ == "__main__":
    main()
