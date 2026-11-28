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
- **Pane binary.** Post the second text below in the pane thread. Save the reply. It names the `gajaeway` path and `GAJAEWAY_HOME` the pane's shell resolved. The test-home binary is `/home/cheongnet/gajaeway/test-state-firstmate-20261015/bin/gajaeway`.
- **Window.** Run `SINCE2=$(date -u +%FT%TZ)` after the first attempt settled. Keep it.
- **Owner addition.** Post the third text below in the first mate's thread, mentioning the test bot.
- **Evidence.** After the pane reports, run `.gjc/skills/verify-firstmate/scripts/correction-evidence.sh --lane <lane> --session "$SESSION" --prior "$PRIOR" --since "$SINCE2" | tee $SCRATCH/correction-evidence.txt`. Exit code `0`. Stdout has `PASS no other session JSONL`, `PASS appended in place`, `PASS same session`, and `PASS attempt ... completed`. Save the pane thread's messages after `SINCE2` as `$SCRATCH/pane-after.json`. One of them reports the correction's result.
- **First mate.** The first mate's session JSONL after `SINCE2` has a `[WAKE:<paneOriginKey>]` reply and no `work start --firstmate-correction` bash call. The pane's session JSONL after `SINCE2` has that bash call.
- **Commit.** The worktree has the setup commit, the first lane commit, and one correction commit. The local origin is still empty.

```text
Assign one FirstMate work lane on the local repository <repo>. Use the text from here to the end as the lane mandate, word for word.

Do the work in that lane session. Do not call the task tool. Do not start another agent. Do not push.

Add NOTES.md with the exact line "first: <run>". Commit it. Stop after that commit exists.
```

```text
Before anything else, run exactly `command -v gajaeway; echo "GAJAEWAY_HOME=$GAJAEWAY_HOME"` in your shell and reply with its two output lines only. Do not steer, correct or start anything.
```

```text
Owner addition to that same task, within its mandate: also add the exact line "correction: <run>" to NOTES.md and commit it. Do not push. Relay this to the secondmate; the secondmate sends it into the same lane as a correction.
```

## Gotchas

- A correction while the job is `awaiting_checks` is refused with `correction_predecessor_not_settled`. That is why this drive uses a local origin. The refusal is a hold, not a pass.
- If a replacement session JSONL for the lane's cwd starts after `SINCE2`, or the lane's binding names a new session before a refusal, stop and report it. That is review finding 5. Do not fix it from this skill.
- The wake's source message id is the correction's `messageId`. Two corrections relayed from one owner message reuse that id, and the second is refused as `correction_conflict`.
- The correction attempt runs on the lane's recorded model. Whether it stays on the switch model belongs to `same-lane-follow-up`.
- This file has no passing evidence yet.
