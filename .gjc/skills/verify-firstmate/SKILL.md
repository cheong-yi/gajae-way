---
name: verify-firstmate
description: "Drive a FirstMate lane on the Gajaeway test home and prove the plan-then-switch path. Use after changing the 0.20.0 lane-switch patch, work.switchAfterEdit, or the test home deployment, before claiming that path works."
---

# Verify FirstMate

This skill drives the running FirstMate test home the way a mention does. It does not launch a gateway. It does not cover a second product called prewalk. It verifies the order where the secondmate pane opens first and its ordinary work start creates the lane. The plan and the switch are one path, and that path is the feature `plan-and-switch`.

Two speeds. Never mix them.

- Deterministic. No model call and no Discord. Run it first.
- Live. One mention starts one lane. It is never part of the deterministic command.

Paths:

- `SKILL=.gjc/skills/verify-firstmate` in this checkout, the Gajaeway lane checkout.
- `TEST_HOME=/home/cheongnet/gajaeway/test-state-firstmate-20261015`.
- `SCRATCH=/home/cheongnet/gajaeway/verify-scratch/<run-id>`.

## Launch

There is nothing to start. The test gateway is the user unit `gajaeway-test-gateway.service`, with `GAJAEWAY_HOME=$TEST_HOME`. Ready means the doctor passes. A live PID alone is not ready. If the doctor fails, stop. Do not restart a service from this skill.

Do not drive the home at `/home/cheongnet/gajaeway/state`. Do not start a second gateway for the same test home.

## Doctor

```sh
.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh
```

Exit 0 and a final line `doctor: ok` are the only pass. It checks `work.switchAfterEdit`, the patch receipt, the daemon binary, and that the daemon started after `config.json`.

## Deterministic command

```sh
RUN=det-$(date -u +%Y%m%dT%H%M%SZ)
.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh &&
.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create /home/cheongnet/gajaeway/verify-scratch/$RUN/repo &&
.gjc/skills/verify-firstmate/scripts/disposable-repo.sh destroy /home/cheongnet/gajaeway/verify-scratch/$RUN/repo
```

This proves the doctor and the disposable repository. It does not prove a lane.

## Drive

The map is `features/README.md`. Drive one feature file at a time. A pass on one file does not cover the others.

No feature has a passing run behind the current order. Every file is uncovered until a run follows it and its own evidence command passes. Do not report one file as verified from another file's run.

The current order: `gajaeway work start --firstmate FILE` records the assignment and opens the task's secondmate pane. It creates no lane and no session. That secondmate's ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"` creates the lane, with the pane as parent. A pane that does not open is a held result. Do not treat the submission as a started lane.
The explicit local check for that order is `.gjc/skills/verify-firstmate/scripts/pane-order-check.sh`. Exit 0 means the named unit tests passed. Exit 1 means one failed. That command does not post to Discord, and it does not prove the live thread `parent_id`.

The mention in that file can be posted by an operator bot into channel `1511208447939706972`. That post is a stand-in for a human mention. Do not print the bot credential. A human mention remains unverified until a person posts one.

## Evidence

Proof is the worker session JSONL and the public work status, not the Discord reply.

```sh
.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE"
```

Exit codes: `0` pass, `1` the attempt ended with a missing or extra switch line, `2` warn line, `3` no worker session, `4` the attempt ended and the public end is missing or before the switch, `5` the attempt is still open, `64` usage. Exit `5` is not a result. A maintain pass waits, and does not record it as a failure.

Save the stdout at `$SCRATCH/evidence.txt`. The JSONL stays under `$TEST_HOME/broker/agent/sessions/` after cleanup.

## Cleanup

```sh
.gjc/skills/verify-firstmate/scripts/disposable-repo.sh destroy "$SCRATCH/repo"
```

Cleanup removes the disposable repository and the worktrees it registered. It does not delete session JSONL, `$SCRATCH/evidence.txt`, or `$TEST_HOME/lanes/`. After cleanup, rerun `live-evidence.sh --since "$SINCE"`. The same JSONL path must still print. A later status may drop `op`. The saved `evidence.txt` is the receipt proof. The rerun still passes when the attempt record ended after the switch.

## Helpers

All are executable and live in `.gjc/skills/verify-firstmate/scripts/`.

- `doctor-test-home.sh [test-home]` reads the deployment and writes nothing.
- `disposable-repo.sh create <absolute-path>` creates a repository with no remotes and one setup commit. `destroy` removes only a repository this helper created.
- `live-evidence.sh --since <UTC ISO> [test-home]` reads session logs and work status.
- `pane-order-check.sh` runs the local pane-order and pane-parent unit tests. Exit 0 is those tests. It does not prove a live Discord `parent_id`.

`VERIFY_GAJAEWAY_TEST_HOME` and `VERIFY_GAJAEWAY_GJC_LOG_DIR` override the test home and the log directory. The marker inside a created repository is `verify-gajaeway-disposable`, so destroy still recognizes repositories the older helper created.

## Maintenance

Use `/maintain-verification-skill` to keep this map honest. That pass may fix this skill. It may not edit the product to make a failed drive look passed.
