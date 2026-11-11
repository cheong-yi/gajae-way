# Scoped closeout

A FirstMate lane is retired with a closeout file. A plain retire is refused.

## Sub-features

- `plain-retire-refused` refuses `work retire <name>` for a FirstMate lane.
- `scoped-retire` retires only through `work retire --firstmate <closeout-file>`.

## How to get to it (user POV)

- After a lane has settled, an operator retires it with the FirstMate closeout.

## Driving it with verify-firstmate

Preconditions:

- The lane name and a real closeout file are known. Do not invent the closeout.
- This file has no passing evidence.

- **Plain retire.** Run `work retire <name>` on the test socket. The result says `firstmate_scoped_release_required`.
- **Scoped retire.** Run `work retire --firstmate <closeout-file>` only when that file is the lane's real closeout. The status then shows the lane retired.
- **Evidence.** Keep the session JSONL. Retire does not delete it.

## Gotchas

- An `operator_force` log line is not this closeout.
- Not driven. `reason=operator_force` in the gateway log is also what `forceRetire` prints for a dead-session sweep. It is not this closeout. Plain retire was not rerun.
