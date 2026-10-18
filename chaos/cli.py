"""beacon-chaos: inject and reset faults on the demo platform.

  beacon-chaos list
  beacon-chaos inject <scenario> [--no-trigger]
  beacon-chaos reset
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import select

from beacon.store import FaultInjection, session, utcnow
from chaos.platform import Platform, PlatformError
from chaos.scenarios import SCENARIOS, Scenario, reset_all


def record_injection(scenario: Scenario) -> FaultInjection:
    with session() as db:
        row = FaultInjection(scenario=scenario.name, ground_truth_cause=scenario.ground_truth_cause,
                             expected_runbook=scenario.expected_runbook, target_dag=scenario.target_dag,
                             injected_at=utcnow())
        db.add(row)
        db.commit()
        return row


def mark_reset() -> int:
    with session() as db:
        rows = db.scalars(select(FaultInjection).where(FaultInjection.reset_at.is_(None))).all()
        for r in rows:
            r.reset_at = utcnow()
        db.commit()
        return len(rows)


def inject(platform: Platform, name: str, trigger: bool = True) -> tuple[FaultInjection, str | None]:
    scenario = SCENARIOS[name]
    injection = record_injection(scenario)
    scenario.inject(platform)
    run_id = platform.trigger_dag(scenario.target_dag) if trigger and scenario.target_dag else None
    return injection, run_id


def reset(platform: Platform) -> list[str]:
    steps = reset_all(platform)
    n = mark_reset()
    return steps + [f"{n} open injection(s) marked reset"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="beacon-chaos", description="Fault injection for the Beacon demo platform")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="list scenarios")
    p_inject = sub.add_parser("inject", help="inject a scenario")
    p_inject.add_argument("scenario", choices=sorted(SCENARIOS))
    p_inject.add_argument("--no-trigger", action="store_true", help="don't trigger the target DAG right away")
    sub.add_parser("reset", help="restore everything")
    args = parser.parse_args(argv)

    if args.cmd == "list":
        print(f"{'scenario':<22}{'ground truth':<28}{'runbook':<18}{'target dag':<20}")
        for s in SCENARIOS.values():
            print(f"{s.name:<22}{s.ground_truth_cause:<28}{s.expected_runbook:<18}{s.target_dag or '-':<20}")
        return 0

    platform = Platform()
    try:
        if args.cmd == "inject":
            injection, run_id = inject(platform, args.scenario, trigger=not args.no_trigger)
            print(f"injected {args.scenario} (fault_injection id={injection.id}, "
                  f"ground truth={injection.ground_truth_cause})")
            if run_id:
                print(f"triggered {SCENARIOS[args.scenario].target_dag} run {run_id}")
        elif args.cmd == "reset":
            for step in reset(platform):
                print(f"- {step}")
    except PlatformError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
