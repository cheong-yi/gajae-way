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

- [Plan and switch](./plan-and-switch.md) first run passed on 2026-11-24 at 03:32:25 UTC on GJC 0.22.0. Evidence is `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261124T033034Z/evidence.txt`. One switch to `opencode-go/deepseek-v4.1-flash` with `thinkingLevel` `high`, then the public attempt ended in `awaiting_checks`. The mention was an operator-bot stand-in. The repository had no remote, so that wait is not a checks pass. The second run, the text-only plan, failed with exit 6 on `plan-switch-20261124T033034Z-noplan`: the lane never activated `todo_write`, so the gate was open at the first edit. This pass does not cover the other files.
- [Same-lane follow-up](./same-lane-follow-up.md) asks a settled lane for a second change. Driven on `smoke-live-20261111T083049Z` while `awaiting_checks`. The correction was refused, and a start is held for that wait. Not a cheap-model pass.
- [New session starts strong](./new-session-starts-strong.md) starts a fresh lane after a switched one. Observed on `smoke-live-20261111T085741Z`. Operator-bot stand-in.
- [Failed-attempt correction](./failed-attempt-correction.md) is the old name. The gateway corrects an accepted completed attempt that is not awaiting checks. A failed attempt is not the predecessor. The refusal on `smoke-live-20261111T083049Z` is recorded. The accept path is uncovered.
- [Pane](./pane.md) is the secondmate view: `--firstmate` opens the pane and creates no lane, the opening line is the bot's notice, and the pane's ordinary work start creates the lane. The local check is `scripts/pane-order-check.sh`. No passing live run behind this order.
- [Pane parent channel](./pane-parent-channel.md) records the matched `repo-<slug>` channel as the pane's child parent. The local adapter match is in the same check. The live Discord `parent_id` is uncovered.
- [Scoped closeout](./scoped-closeout.md) retires a FirstMate lane with a closeout file. Uncovered. No real closeout file, and plain retire was not rerun.
- [Awaiting checks](./awaiting-checks.md) is the pull-request checks cycle. Uncovered. A missing `origin` still starts that wait in this checkout and in upstream `dev`. That is a confirmed bug, not a passing checks cycle. GJC 0.22.0 does not have this state.
