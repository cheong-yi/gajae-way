# Plan and switch

A mention starts one FirstMate lane. The first mate records the assignment with `gajaeway work start --firstmate FILE`, which opens that task's secondmate pane and creates no lane. The secondmate's ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"` creates the lane, with the pane as parent. The lane plans on the strong model, then it records one lane model switch when planning ends. Edit tools stay blocked until that one attempt, and the cheap model finishes the turn. The public attempt ends after the switch.

## Sub-features

- `plan-same-session` plans and then edits in the lane session. It does not hand the files to a child agent.
- `switch-once` records one `lane_model_switch` line when planning ends, after a successful `todo_write` or after any reply with no tool calls during planning. `edit`, `write`, `apply_patch` and `ast_edit` are blocked until that one attempt. On failure the lane logs a warn line, keeps the current model, and does not retry.
- `cheap-finishes` runs the check and the commit after that switch.
- `public-end-after-switch` ends the public attempt after the switch, with a receipt and `end_turn`.

## How to get to it (user POV)

- Mention the test bot in a new thread of channel `1511208447939706972` and ask the first mate to assign one work lane on a local repository with no remotes. The submission opens the secondmate pane; the lane does not exist until that pane runs its ordinary `gajaeway work start`.

## Driving it with verify-firstmate

Preconditions:

- `doctor-test-home.sh` exits 0.
- Each run uses its own disposable repository with no remotes, and records `SINCE` before the mention.

### First run: todo_write ends planning

- **Doctor.** Run `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh`. The last line is `doctor: ok`.
- **Repository.** Run `.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create /home/cheongnet/gajaeway/verify-scratch/$RUN/repo`. The output reads `created <path> head=<sha> branch=main remotes=none`.
- **Window.** Run `SINCE=$(date -u +%FT%TZ)`. Keep it.
- **Mention.** Post the text below in a new thread of channel `1511208447939706972`, mentioning test bot `1557246454714736723`. An operator-bot post is a stand-in, not a human mention.
- **Assignment.** The first mate runs `gajaeway work start --firstmate ASSIGNMENT_JSON_FILE`. That records the task and asks Discord for the secondmate thread. It creates no lane. If the result is a hold, stop. Do not report a started lane.
- **Lane start.** The secondmate runs one ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"`. Only that creates the lane, with the pane as parent. A hold means the thread was not opened. Do not report a started lane.

```text
Start one work lane on the local repository <repo>.

Do the work in that lane session. Do not call the task tool. Do not start another agent. The planning reply is not the end. After it, continue in the same session. Do not stop until the commit exists. Do not push.

Do this in order:
1. Call todo_write with the remaining file changes before any edit, write, apply_patch, or ast_edit.
2. Add smoke_check.py. It reads README.md and exits 0 only if that file contains the exact line "smoke: <run>". Otherwise it exits 1.
3. Add a Smoke section to README.md that contains that exact line and says to run python3 smoke_check.py.
4. Run python3 smoke_check.py, then commit those files.
5. Stop only after that commit exists.
```

- **Evidence.** After the lane stops, run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE" | tee /home/cheongnet/gajaeway/verify-scratch/$RUN/evidence.txt`. Exit code `0`. Stdout contains one `PASS model_change` line, one `PASS edits after switch` line, and one `PASS public end` line whose time is after the switch.
- **Commit.** The worktree has a second commit and no remote. The setup commit is still the first commit.

### Second run: a reply with no tool calls ends planning

This run proves the no-tool-reply path. The mandate never names `todo_write` and asks for no planning tool call. Use a second repository and evidence path, `$RUN-noplan`.

- **Doctor.** Run `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh` against the same test home. The last line is `doctor: ok`.
- **Repository.** Run `.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create /home/cheongnet/gajaeway/verify-scratch/$RUN-noplan/repo`. The output reads `created <path> head=<sha> branch=main remotes=none`.
- **Window.** Run `SINCE=$(date -u +%FT%TZ)`. Keep it.
- **Mention.** Post the text below in a new thread of channel `1511208447939706972`, mentioning test bot `1557246454714736723`. An operator-bot post is a stand-in, not a human mention.
- **Assignment.** The first mate runs `gajaeway work start --firstmate ASSIGNMENT_JSON_FILE`. That records the task and asks Discord for the secondmate thread. It creates no lane. If the result is a hold, stop. Do not report a started lane.
- **Lane start.** The secondmate runs one ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"`. Only that creates the lane, with the pane as parent. A hold means the thread was not opened. Do not report a started lane.

```text
Start one work lane on the local repository <repo>.

Do the work in that lane session. Do not call the task tool. Do not start another agent. Reply with the plan in text only, with no tool call in that reply. Then continue in the same session without another prompt. Do not stop until the commit exists. Do not push.

Do this in order:
1. Add smoke_check.py. It reads README.md and exits 0 only if that file contains the exact line "smoke: <run>". Otherwise it exits 1.
2. Add a Smoke section to README.md that contains that exact line and says to run python3 smoke_check.py.
3. Run python3 smoke_check.py, then commit those files.
4. Stop only after that commit exists.
```

- **Evidence.** After the lane stops, run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE" | tee /home/cheongnet/gajaeway/verify-scratch/$RUN-noplan/evidence.txt`. Exit code `0`. Stdout contains one `PASS model_change` line, one `PASS edits after switch` line, and one `PASS public end` line whose time is after the switch. A missing switch is exit `1`, which is a failure.
- **Commit.** The worktree has a second commit and no remote. The setup commit is still the first commit.

## Gotchas

- In the first run the trigger names `todo_write`. That is a coached step. The switch fires when planning ends, so it does not need an edit.
- The second run has no planning tool call. A reply with no tool calls ends planning, so that run proves the no-tool-reply path.
- An edit-type call blocked before the switch is expected and is not a failure.
- Exit `6` means a successful `edit`, `write`, `apply_patch` or `ast_edit` result came before the switch or from the planning model.
- A child `task` call can stop the lane before the switch. This journey forbids that call. A run that uses `task` is a failure of this feature even if a later wake switches.
- `awaiting_checks` after `end_turn` is the public end of this attempt. It is not a passing pull-request check. This repository has no remote.
- A missing switch line is a failure. Do not accept the Discord reply as a substitute.
- Exit `4` means the attempt ended and the public end is missing or before the switch. Exit `5` means the attempt is still open. Wait. Do not record exit `5` as a failure.
- Passed on 2026-11-19 at 03:57:31 UTC, run `plan-switch-20261119T0353Z`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261119T0353Z/evidence.txt`. Operator-bot stand-in. The public end was `awaiting_checks` because the repository had no remote. That is not a checks pass. An earlier pass on 2026-11-18 at 13:02:33 UTC, run `plan-switch-20261118T130118Z`, was the previous binary. `plan-switch-20261118T124356Z` failed with no switch because GJC 0.21.1 had not yet been patched.
- The recorded passes on 2026-11-18 and 2026-11-19 drove the earlier binary that switched after the first edit, so they do not cover the plan gate. A new drive is needed for both runs.
