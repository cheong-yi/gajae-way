# Accepted-attempt correction

A correction continues one task only after the gateway accepts the predecessor. A failed attempt is not that predecessor. The filename keeps the old name so existing links still resolve.

## Sub-features

- `wrong-predecessor-refused` refuses a correction while the job is `awaiting_checks`, or when acceptance evidence is missing.
- `correction-accepted` accepts a correction only when the job is `done` or `attempt_ended`, the tail `endState` is `completed`, the terminal reason is `end_turn`, acceptance evidence exists, and `awaitingChecks` is absent.
- `identity-kept` keeps the same task and thread on an accepted correction.

## How to get to it (user POV)

- After a lane is accepted and is no longer awaiting checks, ask for a correction of that same task.

## Driving it with verify-firstmate

Preconditions:

- The lane name, task, thread, and prior op ref are known.
- `correction-accepted` has no passing evidence. Do not invent an acceptance or a closeout file.

- **Show the state.** Run `work status <name>` on the test socket. If the state is `awaiting_checks`, or the last attempt is not a completed `end_turn` with acceptance evidence, do not expect an accepted correction.
- **Refusal.** A submitted `work start --firstmate-correction` that exits 1 with `correction_predecessor_not_settled` is the refusal result. Record it. It is not a pass of `correction-accepted`.
- **Acceptance.** Submit the test home's correction command only when the predecessor meets the gateway gate above. The new attempt must keep the task and thread.
- **Evidence.** A refused correction is not a pass. An accepted correction is a pass only when the new attempt keeps the task and thread.

## Gotchas

- Do not use a failed attempt, or a completed `end_turn` that is still `awaiting_checks`, as the accepted predecessor.
- Observed refusal: `smoke-live-20261111T083049Z` at 08:59:11 UTC, exit 1, `correction_predecessor_not_settled`. The job was `awaiting_checks`.
