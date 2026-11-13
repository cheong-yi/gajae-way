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

- This file has no passing live evidence.
- The local check does not post to Discord and does not restart a service.

- **Local check.** Run `.gjc/skills/verify-firstmate/scripts/pane-order-check.sh`. Exit 0 is the explicit check that `--firstmate` creates no lane, the opening line is a bot notice with no author, the pane's ordinary start creates the lane, and the recorded child parent is the channel the adapter reports. Exit 1 means one of those tests failed. This command does not prove a live Discord `parent_id`.
- **Live order.** A later drive still has to follow this file. Proof remains the worker JSONL plus public work status. A pass on `plan-and-switch` does not cover this file.

## Gotchas

- Ending the lane and then reading the pane does not prove the pane during the run.
- Observed while `smoke-live-20261111T085741Z` was `in_flight`. Pane session `17637edb-5373-4dda-a269-5f509f0a888e`, thread `1569999950589206611`, cwd the test workspace. Tools were `read` and `gajaeway work status` only. The worktree commit was still `098839f`. Snapshot: `/home/cheongnet/gajaeway/verify-scratch/live-20261111T085741Z/pane-while-running.json`. Operator-bot stand-in.
