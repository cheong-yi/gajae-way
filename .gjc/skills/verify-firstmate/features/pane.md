# Pane

The pane is the secondmate persona that starts and then watches one running lane. `gajaeway work start --firstmate FILE` opens it and creates no lane. Its own ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"` creates the lane, with the pane as parent. It can then inspect and report. It does not edit the worktree, and it does not decide the lane outcome.

## Sub-features

- `pane-opens` opens from the assignment, before the lane exists.
- `pane-starts-lane` starts the lane with one ordinary work start, not with `--firstmate`.
- `pane-does-not-edit` leaves the worktree commit unchanged by the pane session.

## How to get to it (user POV)

- Have the first mate submit the assignment. The secondmate pane opens first, then its ordinary work start creates the lane.

## Driving it with verify-firstmate

Preconditions:

- A lane is still running. A finished attempt is too late for this feature.
- This file has no passing evidence.

- **Identify the pane.** While `work status` shows the attempt running, find the pane session whose origin is the lane pane, not the worker cwd.
- **Evidence.** The pane session has no edit tool call. The worker commit is unchanged by the pane. The pane's report is not the public attempt result.

## Gotchas

- Ending the lane and then reading the pane does not prove the pane during the run.
- Observed while `smoke-live-20261111T085741Z` was `in_flight`. Pane session `17637edb-5373-4dda-a269-5f509f0a888e`, thread `1569999950589206611`, cwd the test workspace. Tools were `read` and `gajaeway work status` only. The worktree commit was still `098839f`. Snapshot: `/home/cheongnet/gajaeway/verify-scratch/live-20261111T085741Z/pane-while-running.json`. Operator-bot stand-in.
