# Secondmate correction

After a lane's attempt settles completed and the lane stays bound, the owner adds to the same task in the first mate's thread. The first mate relays that addition to the secondmate pane as a wake, to the `paneOriginKey` that `work start --firstmate` returned. The secondmate runs `gajaeway work start --firstmate-correction FILE` into its own lane. The correction resumes the same session in place, appends a new attempt, and reports its result to the pane. The first mate does not submit the correction, and the secondmate does not approve.

## Sub-features

- `relay-not-submit` has the first mate wake the pane and run no `work start --firstmate-correction` itself.
- `same-session` resumes the lane's session id in place. No replacement session is created or bound.
- `op-appended` appends a new attempt after the settled one, in the same session JSONL.
- `report-to-pane` delivers the correction's result to the pane, and the secondmate reports it there.
- `pane-binary` records which `gajaeway` the pane's shell runs, and its `GAJAEWAY_HOME`.

## How to get to it (user POV)

- Mention the test bot with a small committed task, wait until its lane has settled, then ask the first mate in the same thread for one more change to that task.

## Driving it with verify-firstmate

Preconditions:

- `doctor-test-home.sh` exits 0.
- The repository has an empty local origin, from `disposable-repo.sh create --local-origin`. With no origin at all, the gateway starts an `awaiting_checks` wait and refuses the correction with `correction_predecessor_not_settled` until that wait times out. That refusal belongs to `failed-attempt-correction`, not to this file.

Steps:

- **Doctor.** Run `.gjc/skills/verify-firstmate/scripts/doctor-test-home.sh`. The last line is `doctor: ok`.
- **Repository.** Run `.gjc/skills/verify-firstmate/scripts/disposable-repo.sh create --local-origin /home/cheongnet/gajaeway/verify-scratch/$RUN/repo`. The output ends `remotes=origin=<path>.origin.git(empty)`.
- **Window.** Run `SINCE=$(date -u +%FT%TZ)`. Keep it.
- **Mention.** Post the first text below in a new thread of channel `1511208447939706972`, mentioning test bot `1557246454714736723`. An operator-bot post is a stand-in, not a human mention. Save the `work start --firstmate` result, which names `paneOriginKey`.
- **First attempt.** Wait until `work status <lane>` shows the attempt ended `completed` and the state is `attempt_ended` or `done`, not `awaiting_checks`. Save its `sessionId` as `SESSION` and its `attempt.opRef` as `PRIOR`. Run `live-evidence.sh --since "$SINCE"` and save it as `$SCRATCH/first-evidence.txt`.
- **Window.** Run `SINCE2=$(date -u +%FT%TZ)` after the first attempt settled. Keep it.
- **Owner addition.** Post the second text below in the first mate's thread, mentioning the test bot. It also asks the secondmate for its shell's `gajaeway` path and `GAJAEWAY_HOME`. The test-home binary is `/home/cheongnet/gajaeway/test-state-firstmate-20261015/bin/gajaeway`.
- **First evidence.** `first-evidence.txt` can exit `1` with no switch when the lane writes `NOTES.md` through `bash`. This file does not need a switch.
- **Evidence.** After the pane reports, run `set -o pipefail; .gjc/skills/verify-firstmate/scripts/correction-evidence.sh --lane <lane> --session "$SESSION" --prior "$PRIOR" --since "$SINCE2" | tee $SCRATCH/correction-evidence.txt`. Exit code `0`. Stdout has `PASS no other session JSONL`, `PASS appended in place`, `PASS same session`, and `PASS attempt ... completed`. Save the pane thread's messages after `SINCE2` as `$SCRATCH/pane-after.json`. One of them reports the correction's result.
- **First mate.** The first mate's session JSONL after `SINCE2` has a `[WAKE:<paneOriginKey>]` reply and no `work start --firstmate-correction` bash call. The pane's session JSONL after `SINCE2` has that bash call.
- **Commit.** The worktree has the setup commit, the first lane commit, and one correction commit. The local origin is still empty.

```text
Assign one FirstMate work lane on the local repository <repo>. Use the text from here to the end as the lane mandate, word for word.

Do the work in that lane session. Do not call the task tool. Do not start another agent. Do not push.

Add NOTES.md with the exact line "first: <run>". Commit it. Stop after that commit exists.
```

```text
Owner addition to that same task, within its mandate: also add the exact line "correction: <run>" to NOTES.md and commit it. Do not push. Relay this to the secondmate; the secondmate sends it into the same lane as a correction. In the same relay, ask the secondmate to run exactly `command -v gajaeway; echo "GAJAEWAY_HOME=$GAJAEWAY_HOME"` in its shell and put those two output lines in its pane report.
```

## Gotchas

- A correction while the job is `awaiting_checks` is refused with `correction_predecessor_not_settled`. That is why this drive uses a local origin. The refusal is a hold, not a pass.
- If a replacement session JSONL for the lane's cwd starts after `SINCE2`, or the lane's binding names a new session before a refusal, stop and report it. That is review finding 5. Do not fix it from this skill.
- The wake's source message id is the correction's `messageId`. Two corrections relayed from one owner message reuse that id, and the second is refused as `correction_conflict`.
- The correction attempt runs on the lane's recorded model. Whether it stays on the switch model belongs to `same-lane-follow-up`.
- On 2026-11-28 an operator-bot post in the pane thread did not start a pane turn, with or without a mention. The pane read those posts only as unread context on its next turn. Ask for the shell check through the first mate's relay, as above.
- Passed again on 2026-11-28, run `fm-correction-20261128T120617Z`, on the build of `79054b971`, the final pane notice. Operator-bot stand-in. First attempt `gw-work-fm-correction-20261128t120617z-01m8h0tqmj9gvfgdc1tkvr5dc0` ended `attempt_ended` at 12:07:47 UTC in session `f9b52d91-2954-411f-8bbe-3818f2464d61`. The first mate, session `f2f5f4e9-3480-46d4-9a42-936ed1ee47e2`, woke the pane once and ran no correction. The pane, session `f260e8e0-d349-41d6-b64e-a3825e411f64`, ran the correction once and got `started: true` in the same lane session. The new attempt `gw-work-fm-correction-20261128t120617z-01m8h0xeaz5y0ntkvyjfcvz1z4` ended completed at 12:09:03 UTC with commit `d6aa7bb`, and the pane reported it at 12:09:18 UTC. The pane's shell printed `/home/cheongnet/gajaeway/test-state-firstmate-20261015/bin/gajaeway` and `GAJAEWAY_HOME=/home/cheongnet/gajaeway/test-state-firstmate-20261015`. The local origin stayed empty. `correction-evidence.sh` exited 0 before and after cleanup. Evidence is in `/home/cheongnet/gajaeway/verify-scratch/fm-correction-20261128T120617Z/`, with the pane's calls and results in `pane-calls.txt` and `pane-results.txt`. Its `first-evidence.txt` exits `1` with no switch: the lane wrote `NOTES.md` through `bash`, so no edit-type result opened the switch. That is a `plan-and-switch` result, not a correction failure. Because the lane never switched, the continue nudge also fired on the correction attempt's text replies, and the pane thread shows those turns.
- Passed on 2026-11-28, run `fm-correction-20261128T114915Z`, on the build of `7a73e0eb2` with GJC 0.22.0 patch sha256 `ff8603b4`. Operator-bot stand-in. The first attempt `gw-work-fm-correction-20261128t114915z-01m8gzv4571rcgxh4ysj446adm` ended `attempt_ended` at 11:50:17 UTC with no checks wait. The first mate read `paneOriginKey` from its saved `work start --firstmate` result, woke the pane, and ran no correction. The pane ran `gajaeway work start --firstmate-correction` at 11:54:08 UTC with `messageId` `1576204424286568609`, the owner message the wake came from. The result was `started: true` in session `cb364bec-8477-4600-b60d-439c01bbe04f`, the same session as the first attempt. The new attempt `gw-work-fm-correction-20261128t114915z-01m8h02xgtptd7rjmsdmqnwd9g` ended completed with `end_turn` at 11:54:19 UTC. Its user message was appended to the same JSONL, and no other session started for that cwd. The lane report reached the pane at 11:54:20 UTC, and the pane reported commit `2f70643` at 11:54:32 UTC. The pane's shell printed `/home/cheongnet/gajaeway/test-state-firstmate-20261015/bin/gajaeway` and `GAJAEWAY_HOME=/home/cheongnet/gajaeway/test-state-firstmate-20261015`. The local origin stayed empty. `correction-evidence.sh` exited 0 before and after cleanup. Evidence is in `/home/cheongnet/gajaeway/verify-scratch/fm-correction-20261128T114915Z/`: `first-evidence.txt`, `correction-evidence.txt`, `correction-evidence-after-cleanup.txt`, `pane-after.json` and `firstmate-after.json`. No refusal happened, so this run says nothing about review finding 5.
