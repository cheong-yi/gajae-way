# FirstMate verification map

Read this index, then drive one feature file. A pass on one file does not cover the others.

## Baseline preconditions

- The test gateway is already running with `GAJAEWAY_HOME=/home/cheongnet/gajaeway/test-state-firstmate-20261015`.
- `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh` exits 0.
- Channel `1511208447939706972` is `mention-open` with audience `all` in that home.
- Do not drive `/home/cheongnet/gajaeway/state`.

## Driving conventions

- Deterministic steps make no model call and do not post to Discord.
- Start each live run with a new disposable repository and a new `SINCE` timestamp.
- Do not restart a service, open a database, or delete gateway lane state from a recipe.
- An operator-bot post is a stand-in for a human mention. Say so in the evidence. Do not print the credential.

## Proof and skip reporting

- Proof is the worker JSONL plus the public work status, not the chat reply.
- Record the helper command, its stdout, and its exit code.
- An uncovered feature stays uncovered. Do not report it as verified through `plan-and-switch`.

## Feature entry contract

Each feature file has an H1, one paragraph, then `Sub-features`, `How to get to it (user POV)`, `Driving it with verify-firstmate`, and `Gotchas`.

## Features

- [Plan and switch](./plan-and-switch.md) is the path this skill can prove. One mention, one lane, the strong model plans and edits, one switch, the cheap model finishes that turn, and the public attempt ends after the switch.
- [Same-lane follow-up](./same-lane-follow-up.md) asks a settled lane for a second change. Driven on `smoke-live-20261111T083049Z` while `awaiting_checks`. The correction was refused. Not a cheap-model pass.
- [New session starts strong](./new-session-starts-strong.md) starts a fresh lane after a switched one. Observed on `smoke-live-20261111T085741Z`. Operator-bot stand-in.
- [Failed-attempt correction](./failed-attempt-correction.md) is the old name. The gateway corrects an accepted completed attempt that is not awaiting checks. A failed attempt is not the predecessor. The refusal on `smoke-live-20261111T083049Z` is recorded. The accept path is uncovered.
- [Pane](./pane.md) is the second view of a running lane. Observed while `smoke-live-20261111T085741Z` was `in_flight`. Operator-bot stand-in.
- [Scoped closeout](./scoped-closeout.md) retires a FirstMate lane with a closeout file. Uncovered. No real closeout file, and plain retire was not rerun.
- [Awaiting checks](./awaiting-checks.md) is the pull-request checks cycle. Uncovered, and this scratch repository has no remote.
