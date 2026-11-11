# Awaiting checks

After a pushed branch has an open pull request, the lane waits for checks. Green settles it. Red re-steers it, up to the bound. No pull request means this feature cannot pass.

## Sub-features

- `checks-hold` stays in `awaiting_checks` while checks are pending.
- `checks-green` settles done when the open pull request is green.
- `checks-red` re-steers on red, then stops at the bound.

## How to get to it (user POV)

- A lane pushes a branch and opens a pull request. The operator waits for the checks.

## Driving it with verify-firstmate

Preconditions:

- The lane repository has a remote and an open pull request. The disposable repository from `plan-and-switch` has no remote, so this precondition is unmet.
- This file has no passing evidence.

- **Show the miss.** Run `git -C <repo> remote`. Empty output means stop. Report `awaiting-checks` uncovered because there is no remote and no pull request.
- **Do not weaken.** Do not treat `awaiting_checks` on a no-remote repository as a passing checks cycle.

## Gotchas

- The plan-and-switch run can sit in `awaiting_checks` with no pull request. That state is not this feature.
- A missing `origin` is a confirmed bug, not this feature. `git remote get-url --push origin` fails, push is recorded as null, and null is not treated as not pushed, so the lane enters `awaiting_checks` with no pull request and can sit until the 60-minute timeout. A missing `origin` should count as `pushed=false`, and then this wait would not start. A steer of an ended attempt is intended. `work start`, including resume, is held while the wait is open, so the two together block another start for up to an hour.
- A later dead-session sweep can end that wait. The log line `reason=operator_force` is the sweep label in `forceRetire`, not a checks result and not a scoped closeout. Observed on `smoke-live-20261111T085741Z` at 09:31:42 UTC, with `awaiting_checks_settled` reason `lane force-retired before checks settled`.
