# New session starts strong

A new lane session starts on the strong model even after another lane has switched.

## Sub-features

- `fresh-strong` records a strong model on the new session before any edit.
- `not-the-old-session` uses a new session id.

## How to get to it (user POV)

- Mention the test bot in a new thread and ask for a new lane on a new repository.

## Driving it with verify-firstmate

Preconditions:

- A previous lane has already switched. Its session id is known.
- This file has no passing evidence.

- **New repository.** Run `disposable-repo.sh create` on a new absolute path. Record a new `SINCE`.
- **New thread.** Post a plan-and-switch mention against that new repository.
- **Evidence.** The worker session id differs from the earlier lane. Its first model is the strong model, not `opencode-go/deepseek-v4.1-flash:high`.

## Gotchas

- Reusing the old thread can reopen the old session. That is the follow-up feature, not this one.
- Observed on `live-20261111T085741Z`, an operator-bot stand-in. Worker `3ff9854b-1121-4d72-adec-42935fc8bca8` is not `a376f240-3f63-42ad-af24-fd831bee5542`. The first model change at 08:59:10 UTC was `anthropic/claude-opus-5-5`, before any edit. The cheap switch at 09:00:28 UTC came after the edit.
