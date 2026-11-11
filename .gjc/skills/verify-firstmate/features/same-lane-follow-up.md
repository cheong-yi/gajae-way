# Same-lane follow-up

After a plan-and-switch attempt has ended, a second change on that same lane stays on the cheap model and does not switch again.

## Sub-features

- `reopen-cheap` uses the recorded cheap model for the second change.
- `no-second-switch` records no new `lane_model_switch` line.

## How to get to it (user POV)

- In the same thread as a finished plan-and-switch lane, ask for one more committed line on that same lane.

## Driving it with verify-firstmate

Preconditions:

- A plan-and-switch attempt has ended after its switch, and its session id is known.
- This file has no passing evidence. Do not report it from the first journey.

- **Confirm the first end.** Run `.gjc/skills/verify-firstmate/scripts/live-evidence.sh --since "$SINCE"`. Exit code must be `0` before the follow-up is posted.
- **Follow-up.** Post in the same thread: continue the same lane, add one line `follow-up: <run>` to README.md, commit it, do not push, and do not start another lane.
- **Evidence.** The same session id makes the commit. Its later model calls stay on the cheap model. `live-evidence.sh` still finds one switch line, not two. A new session id fails this feature and belongs to `new-session-starts-strong`.

## Gotchas

- A continuation while the job is `awaiting_checks` may be held instead of started. Record that hold. Do not call it a cheap-model pass.
- Driven once. Message `1569999562418954303` asked the same lane to add `follow-up: live-20261111T083049Z`. At 08:59:11 UTC `work start --firstmate-correction` exited 1 with `correction_predecessor_not_settled`. No second attempt, and the head stayed `0a52564`. That is a hold, not a pass.
- There is no cheap-reopen command while the job is `awaiting_checks`. A correction is the wrong command for this feature. The pass still requires the same session to commit the follow-up on the cheap model, with one switch line.
