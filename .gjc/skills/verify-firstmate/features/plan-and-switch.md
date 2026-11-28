# Plan and switch

A mention starts one FirstMate lane. The first mate records the assignment with `gajaeway work start --firstmate FILE`, which opens that task's secondmate pane and creates no lane. The secondmate's ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"` creates the lane, with the pane as parent. The lane plans on the strong model. A hidden plan nudge asks for a complete plan and a todo list, and a hidden continue nudge keeps the lane moving when it stops. No edit is blocked. The lane records one lane model switch at its first successful edit batch after a todo list exists, and the switch model finishes the task. The public attempt ends after the switch.

## Sub-features

- `plan-same-session` plans and then edits in the lane session. It does not hand the files to a child agent.
- `switch-once` records one `lane_model_switch` line at the first completed turn whose batch holds a successful `edit`, `write`, `apply_patch` or `ast_edit` result, once a successful `todo_write` has opened the switch gate. No edit is refused before that. A `bash` run never triggers the switch. On failure the lane logs a warn line, keeps the current model, and does not retry.
- `cheap-finishes` runs the check and the commit after that switch.
- `public-end-after-switch` ends the public attempt after the switch, with a receipt and `end_turn`.

## How to get to it (user POV)

- Mention the test bot in a new thread of channel `1511208447939706972` and ask the first mate to assign one work lane on a local repository with no remotes. The submission opens the secondmate pane; the lane does not exist until that pane runs its ordinary `gajaeway work start`.

## Driving it with verify-firstmate

Preconditions:

- `doctor-test-home.sh` exits 0.
- Each run uses its own disposable repository with no remotes, and records `SINCE` before the mention.

### First run: a todo list opens the gate

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

- **Evidence.** After the lane stops, run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE" | tee /home/cheongnet/gajaeway/verify-scratch/$RUN/evidence.txt`. Exit code `0`. Stdout contains one `PASS model_change` line, one `PASS edits first on planning model` line, and one `PASS public end` line whose time is after the switch.
- **Commit.** The worktree has a second commit and no remote. The setup commit is still the first commit.

### Second run: a text-only plan gets the continue nudge

This run proves the text-only plan path. The mandate makes the first two replies text only and never names `todo_write`. The first text-only reply ends a turn with no edit, so it gets the hidden plan nudge. The next text-only reply, the answer to that nudge, gets the hidden continue nudge. A reply after the plan nudge that calls a tool does not get it. The lane then calls `todo_write`, and it switches only after the first edit batch once that `todo_write` has opened the gate. Use a second repository and evidence path, `$RUN-noplan`.

- **Doctor.** Run `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh` against the same test home. The last line is `doctor: ok`.
- **Repository.** Run `.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create /home/cheongnet/gajaeway/verify-scratch/$RUN-noplan/repo`. The output reads `created <path> head=<sha> branch=main remotes=none`.
- **Window.** Run `SINCE=$(date -u +%FT%TZ)`. Keep it.
- **Mention.** Post the text below in a new thread of channel `1511208447939706972`, mentioning test bot `1557246454714736723`. An operator-bot post is a stand-in, not a human mention.
- **Assignment.** The first mate runs `gajaeway work start --firstmate ASSIGNMENT_JSON_FILE`. That records the task and asks Discord for the secondmate thread. It creates no lane. If the result is a hold, stop. Do not report a started lane.
- **Lane start.** The secondmate runs one ordinary `gajaeway work start NAME --cwd WORKTREE "<mandate text>"`. Only that creates the lane, with the pane as parent. A hold means the thread was not opened. Do not report a started lane.

```text
Start one work lane on the local repository <repo>.

Use the text from here to the end as the lane mandate, word for word.

Do the work in that lane session. Do not call the task tool. Do not start another agent. Do not push.

This lane tests its planning turns. Follow these reply rules exactly.
- Reply 1 is text only. Write the plan from this message alone. Call no tool in reply 1, not even to read a file or look at the repository. End reply 1 after the plan.
- Reply 2 is text only. When a message asks you for a plan and a todo list, write the todo list as a numbered list in text and call no tool. End reply 2 after the list.
- From reply 3 on, use the tools that message asked for, then do the steps below with tools. Do not stop until the commit exists.

The steps:
1. Add smoke_check.py. It reads README.md and exits 0 only if that file contains the exact line "smoke: <run>". Otherwise it exits 1.
2. Add a Smoke section to README.md that contains that exact line and says to run python3 smoke_check.py.
3. Run python3 smoke_check.py, then commit those files.
4. Stop only after that commit exists.
```

- **Evidence.** After the lane stops, run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE" --expect-continue | tee /home/cheongnet/gajaeway/verify-scratch/$RUN-noplan/evidence.txt`. Exit code `0`. The `NUDGES` line names a `plan` index, a `continue` index, a `todo_write` index and a `switch` index, and ends with `order=continue<todo_write<switch`. Exit `8` means the continue nudge did not fire before the first successful `todo_write`, which is a failure of this run. Stdout contains one `PASS model_change` line, one `PASS edits first on planning model` line, and one `PASS public end` line whose time is after the switch. A missing switch is exit `1`, which is a failure.
- **Commit.** The worktree has a second commit and no remote. The setup commit is still the first commit.

## Gotchas

- The switch fires only on a successful `edit`, `write`, `apply_patch` or `ast_edit` result after a successful `todo_write`. A run that never edits never switches. The first run names `todo_write` as a coached step.
- The second run has no planning tool call. A reply with no tool calls no longer ends planning. The first turn with no edit gets the plan nudge, whether it is text or a `bash` call. The continue nudge fires only on a later text-only reply before the switch. The lane still switches only after the first edit batch following `todo_write`.
- The first mate writes the lane mandate from the mention. The mention asks it to pass the text word for word. Check the lane's first user message in the worker JSONL. A paraphrased mandate is a failed setup, not a nudge result.
- Edits are not blocked. An edit before `todo_write` is allowed and does not switch the model.
- Exit `6` means the first successful edit-type result did not come from the planning model after a successful `todo_write`, a result before the switch line came from the switch model, or a result after the switch line came from the planning model. File changes before the switch line are the first batch. The switch line is written after that turn finishes.
- A child `task` call can stop the lane before the switch. This journey forbids that call. A run that uses `task` is a failure of this feature even if a later wake switches.
- `awaiting_checks` after `end_turn` is the public end of this attempt. It is not a passing pull-request check. This repository has no remote.
- A missing switch line is a failure. Do not accept the Discord reply as a substitute.
- Exit `4` means the attempt ended and the public end is missing or before the switch. Exit `5` means the attempt is still open. Wait. Do not record exit `5` as a failure.
- The switch's `model_change` carries the executor role's configured level in `thinkingLevel`, and its `reason` is `lane_model_switch <provider>/<id>:<level>`. Exit `7` means that field is missing or differs from the level the switch applied, which is the level suffix on the next default `configured_model_chain` entry. The `model` field is the bare `<provider>/<id>`. Before the 0.22.0 patch the switch applied the level but recorded `thinkingLevel` null, as on `plan-switch-20261124T025018Z`.
- `todo_write` is a discoverable tool in GJC 0.21.1 and 0.22.0. The lane-switch patch activates it when the session factory creates a prewalk lane, before the first model call. If it is still inactive after that, the patch logs `lane_model_switch could not activate todo_write` and the switch gate is open, so the first successful edit switches without any todo list. The evidence script then exits `6`.
- Both runs passed on 2026-11-27 on GJC 0.22.0 with patch sha256 `ff8603b4a468077d3f7c6582632010b37c66e17d16f735f4ed1136046e7033c5`, which activates `todo_write` at session start. Operator-bot stand-in. First run `plan-switch-20261127T133802Z`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261127T133802Z/evidence.txt`: `todo_write`, then a write on `anthropic/claude-opus-5-5`, one switch at 13:39:41 UTC to `opencode-go/deepseek-v4.1-flash` with `thinkingLevel` `high`, public end 13:40:09 UTC in `awaiting_checks`. Second run `plan-switch-20261127T133802Z-noplan`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261127T133802Z-noplan/evidence.txt`: the lane called `todo_write` with no `search_tool_bm25` call, then wrote and edited on the planning model, one switch at 13:42:28 UTC with `thinkingLevel` `high`, public end 13:42:52 UTC in `awaiting_checks`. Its first reply was a `bash` call, not a text-only reply, so the continue nudge was not exercised; the plan nudge was. `live-evidence.sh` exited 0 for both before and after cleanup. Neither repository had a remote, so `awaiting_checks` is not a checks pass. An earlier pass the same day, `plan-switch-20261127T131221Z`, drove patch sha256 `b0eeaad894055febd2eab8279f26061b02a539f865b406e08f7f52527d60ce47`, before the explicit-empty tool selection guard.
- First run passed on 2026-11-24 at 03:32:25 UTC on GJC 0.22.0 with patch sha256 `0db922ee73fdf2e18ac691ef7a0993a33f9a8a0e7043d484f7a23ee2ee214a4c`, run `plan-switch-20261124T033034Z`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261124T033034Z/evidence.txt`. Operator-bot stand-in. The lane activated `todo_write` with `search_tool_bm25`, called it, then wrote two files on `anthropic/claude-opus-5-5`. One switch followed, to `opencode-go/deepseek-v4.1-flash` with `thinkingLevel` `high`. The public end was `awaiting_checks` because the repository had no remote. `live-evidence.sh` exited 0 before and after cleanup.
- Second run failed with exit `6` on 2026-11-24, run `plan-switch-20261124T033034Z-noplan`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261124T033034Z-noplan/evidence.txt`. The lane got the plan nudge, never activated or called `todo_write`, and switched after its first write and edit batch at 03:34:44 UTC, with `thinkingLevel` `high`. The switch and the public end are recorded. The todo gate was open only because the tool was inactive. That binary had no startup activation; the 2026-11-27 run above is the pass.
- Passed on 2026-11-19 at 03:57:31 UTC, run `plan-switch-20261119T0353Z`, evidence `/home/cheongnet/gajaeway/verify-scratch/plan-switch-20261119T0353Z/evidence.txt`. Operator-bot stand-in. The public end was `awaiting_checks` because the repository had no remote. That is not a checks pass. An earlier pass on 2026-11-18 at 13:02:33 UTC, run `plan-switch-20261118T130118Z`, was the previous binary. `plan-switch-20261118T124356Z` failed with no switch because GJC 0.21.1 had not yet been patched.
- The recorded passes on 2026-11-18 and 2026-11-19 drove the earlier binary that blocked edits and switched when planning ended, so they do not cover the plan nudge, the continue nudge, or the edit-triggered switch. A new drive is needed for both runs.
