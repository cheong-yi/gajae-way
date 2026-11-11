# Plan and switch

A mention starts one FirstMate lane. The lane plans on the strong model, edits in that same session, switches once, and the cheap model finishes the turn. The public attempt ends after the switch.

## Sub-features

- `plan-same-session` plans and then edits in the lane session. It does not hand the files to a child agent.
- `switch-once` records one `lane_model_switch` line after `todo_write` and a successful edit.
- `cheap-finishes` runs the check and the commit after that switch.
- `public-end-after-switch` ends the public attempt after the switch, with a receipt and `end_turn`.

## How to get to it (user POV)

- Mention the test bot in a new thread of channel `1511208447939706972` and ask it to start one work lane on a local repository with no remotes.

## Driving it with verify-firstmate

Preconditions:

- `doctor-test-home.sh` exits 0.
- The disposable repository exists, has no remotes, and `SINCE` was recorded before the mention.

- **Doctor.** Run `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh`. The last line is `doctor: ok`.
- **Repository.** Run `.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create /home/cheongnet/gajaeway/verify-scratch/$RUN/repo`. The output reads `created <path> head=<sha> branch=main remotes=none`.
- **Window.** Run `SINCE=$(date -u +%FT%TZ)`. Keep it.
- **Mention.** Post this text in a new thread, mentioning test bot `1557246454714736723`. An operator-bot post is a stand-in, not a human mention.

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

- **Evidence.** After the lane stops, run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE" | tee /home/cheongnet/gajaeway/verify-scratch/$RUN/evidence.txt`. Exit code `0`. Stdout contains one `PASS model_change` line and one `PASS public end` line whose time is after the switch.
- **Commit.** The worktree has a second commit and no remote. The setup commit is still the first commit.

## Gotchas

- The trigger names `todo_write`. That is a coached edit. It does not prove an uncoached FirstMate edit.
- A child `task` call can stop the lane before the switch. This journey forbids that call. A run that uses `task` is a failure of this feature even if a later wake switches.
- `awaiting_checks` after `end_turn` is the public end of this attempt. It is not a passing pull-request check. This repository has no remote.
- A missing switch line is a failure. Do not accept the Discord reply as a substitute.
- Exit `4` means the attempt ended and the public end is missing or before the switch. Exit `5` means the attempt is still open. Wait. Do not record exit `5` as a failure.
