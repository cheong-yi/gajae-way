#!/usr/bin/env python3
"""Zero-LLM pull request watch. Posts only when the review signal changes.

Stops when the pull request closes or merges, or when its one-day limit passes, whichever comes first.

Usage:
  pr-watch.py --repo OWNER/NAME --pr N --channel SNOWFLAKE --mention SNOWFLAKE --owner SNOWFLAKE \\
    --state /path/to/state.json [--timer unit.timer] [--max-age-seconds N] [--thread-name TEXT] [--dry-run]
"""

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.dont_write_bytecode = True

API = "https://discord.com/api/v10"
SNOWFLAKE = re.compile(r"[1-9][0-9]{0,19}")
REPO_NAME = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
FAILED = ("FAILURE", "TIMED_OUT", "ACTION_REQUIRED")
MAX_AGE_SECONDS = 86400


class WatchError(Exception):
    pass


def run(args, timeout):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise WatchError(error.__class__.__name__) from None
    if result.returncode != 0:
        raise WatchError("command_failed")
    return result.stdout


def gh_json(args):
    try:
        return json.loads(run(["gh", *args], 25))
    except ValueError:
        raise WatchError("github_invalid") from None


def failed_names(checks):
    return sorted(
        item["name"]
        for item in checks
        if item["conclusion"] in FAILED
        and item["name"]
        and not item["name"].startswith("(")
        and "github.event" not in item["name"]
    )


def fingerprint(body):
    signal = {
        "state": body["state"],
        "draft": body["draft"],
        "merged": body["merged"],
        "head": body["head"],
        "review": body["review"],
        "failed": failed_names(body["checks"]),
        "reviews": sorted((item.get("id"), item.get("state")) for item in body["reviews"] if isinstance(item, dict)),
        "review_comments": sorted(item.get("id") for item in body["review_comments"] if isinstance(item, dict)),
        "issue_comments": sorted(item.get("id") for item in body["issue_comments"] if isinstance(item, dict)),
    }
    return hashlib.sha256(json.dumps(signal, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def snapshot(repo, pr):
    view = gh_json([
        "pr", "view", pr, "--repo", repo,
        "--json", "state,isDraft,mergedAt,headRefOid,reviewDecision,statusCheckRollup,url",
    ])
    reviews = gh_json(["api", f"repos/{repo}/pulls/{pr}/reviews", "--jq", "[.[] | {id, user: .user.login, state}]"])
    comments = gh_json(["api", f"repos/{repo}/pulls/{pr}/comments", "--jq", "[.[] | {id, user: .user.login, path}]"])
    issue_comments = gh_json(["api", f"repos/{repo}/issues/{pr}/comments", "--jq", "[.[] | {id, user: .user.login}]"])
    if not isinstance(view, dict) or not all(isinstance(item, list) for item in (reviews, comments, issue_comments)):
        raise WatchError("github_invalid")
    checks = []
    for item in view.get("statusCheckRollup") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")[:120]
        if not name or name.startswith("(") or "github.event" in name:
            continue
        checks.append({"name": name, "conclusion": item.get("conclusion") or ""})
    checks.sort(key=lambda item: item["name"])
    head = view.get("headRefOid")
    state = view.get("state")
    if not isinstance(head, str) or not re.fullmatch(r"[0-9a-f]{40}", head) or state not in ("OPEN", "CLOSED", "MERGED"):
        raise WatchError("github_invalid")
    body = {
        "state": state,
        "draft": bool(view.get("isDraft")),
        "merged": bool(view.get("mergedAt")),
        "head": head,
        "review": view.get("reviewDecision") or "",
        "checks": checks,
        "reviews": reviews,
        "review_comments": comments,
        "issue_comments": issue_comments,
        "url": view.get("url") or f"https://github.com/{repo}/pull/{pr}",
    }
    body["fingerprint"] = fingerprint(body)
    return body


def read_token(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        raise WatchError("discord_token_unavailable") from None
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        os.close(fd)
        raise WatchError("discord_token_unavailable")
    with os.fdopen(fd, "r", encoding="ascii") as handle:
        token = handle.read(4097).strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,4096}", token):
        raise WatchError("discord_token_unavailable")
    return token


def discord(token, method, route, payload=None):
    config = f'header = "Authorization: Bot {token}"\nheader = "Content-Type: application/json"\n'
    if payload is not None:
        config += "data = " + json.dumps(json.dumps(payload, ensure_ascii=False), ensure_ascii=False) + "\n"
    try:
        result = subprocess.run(
            ["/usr/bin/curl", "-q", "--silent", "--show-error", "--proto", "=https",
             "--connect-timeout", "5", "--max-time", "15", "--max-filesize", "65536",
             "--retry", "0", "--request", method, "--config", "-", "--write-out", "\n%{http_code}", API + route],
            input=config, capture_output=True, text=True, check=False, timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise WatchError("discord_unconfirmed") from None
    if result.returncode:
        raise WatchError("discord_unconfirmed")
    raw, separator, status = result.stdout.rpartition("\n")
    if not separator or status not in ("200", "201"):
        raise WatchError(f"discord_rejected_{status or 'none'}")
    try:
        data = json.loads(raw)
    except ValueError:
        raise WatchError("discord_unconfirmed") from None
    if not isinstance(data, dict):
        raise WatchError("discord_unconfirmed")
    return data


def load_state(path, repo, pr):
    if not path.exists():
        return {"version": 1, "repo": repo, "pr": int(pr)}
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("version") != 1 or str(data.get("pr")) != str(int(pr)) or data.get("repo", repo) != repo:
        raise WatchError("state_invalid")
    data["repo"] = repo
    return data


def save_state(path, state):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def age_label(seconds):
    if seconds == 86400:
        return "one day"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return "1 hour" if hours == 1 else f"{hours} hours"
    return f"{seconds} seconds"


def parse_time(value):
    if not isinstance(value, str):
        raise WatchError("state_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise WatchError("state_invalid") from None
    if parsed.tzinfo is None:
        raise WatchError("state_invalid")
    return parsed.astimezone(timezone.utc)


def ensure_clock(state, now, max_age):
    if "started_at" not in state:
        state["started_at"] = now.isoformat()
        state["deadline"] = (now + timedelta(seconds=max_age)).isoformat()
        return True
    parse_time(state["started_at"])
    return False


def note(kind, body, mention, previous=None, owner=None, max_age=MAX_AGE_SECONDS):
    failed = ", ".join(failed_names(body["checks"])[:8]) or "none"
    if kind == "timeout":
        return (
            f"<@{owner}> {body['url']} is still open after {age_label(max_age)} and has not merged. "
            f"Head {body['head']}. Review decision {body['review'] or 'none'}. "
            f"Failed checks: {failed}. This watch is stopping."
        )
    if kind == "closed":
        return f"<@{mention}> {body['url']} is {body['state'].lower()}. This watch is stopping after this note."
    if kind == "introduced":
        return (
            f"<@{mention}> Could you confirm you can take the review watch for {body['url']}?\n\n"
            f"Head is {body['head']}. State is {body['state']}, draft {body['draft']}. "
            f"Review decision is {body['review'] or 'none'}. Failed checks: {failed}. "
            f"Review comments: {len(body['review_comments'])}. Reviews: {len(body['reviews'])}.\n\n"
            "Please address review claims on that pull request, and tell the owner only when a claim needs an owner decision. "
            "Please do not merge it or restart a service. This watch posts again only when the head, a review, a comment, "
            "a failed check, or the open state changes. It stops when the pull request closes or merges, or after "
            f"{age_label(max_age)}, whichever comes first."
        )
    old_reviews = {item.get("id") for item in (previous or {}).get("reviews", [])}
    old_comments = {item.get("id") for item in (previous or {}).get("review_comments", [])}
    new_reviews = ", ".join(
        f"{item.get('user')} {item.get('state')}" for item in body["reviews"] if item.get("id") not in old_reviews
    ) or "none"
    new_comments = ", ".join(
        f"{item.get('user')} {item.get('path')}" for item in body["review_comments"] if item.get("id") not in old_comments
    ) or "none"
    return (
        f"<@{mention}> The pull request changed. {body['url']}\n"
        f"Head {body['head']}. State {body['state']}, draft {body['draft']}. "
        f"Failed checks: {failed}. New reviews: {new_reviews}. New review comments: {new_comments}."
    )


def post(token, state, channel, thread_name, mention, text):
    thread = state.get("thread_id")
    if not (isinstance(thread, str) and SNOWFLAKE.fullmatch(thread)):
        created = discord(token, "POST", f"/channels/{channel}/threads", {
            "name": thread_name[:100],
            "type": 11,
            "auto_archive_duration": 1440,
        })
        thread = str(created.get("id") or "")
        if not SNOWFLAKE.fullmatch(thread):
            raise WatchError("discord_unconfirmed")
        state["thread_id"] = thread
    message = discord(token, "POST", f"/channels/{thread}/messages", {"content": text[:1900]})
    mentions = {item.get("id") for item in message.get("mentions") or [] if isinstance(item, dict)}
    if not SNOWFLAKE.fullmatch(str(message.get("id") or "")) or mention not in mentions:
        raise WatchError("discord_mention_missing")
    return thread


def decide(state, body, now, max_age):
    terminal = body["state"] in ("CLOSED", "MERGED") or body["merged"]
    if terminal and not state.get("closed_posted"):
        return "closed"
    started = state.get("started_at")
    if started and not state.get("timeout_posted") and now >= parse_time(started) + timedelta(seconds=max_age):
        return "timeout"
    if state.get("fingerprint") == body["fingerprint"] or state.get("timeout_posted") or state.get("closed_posted"):
        return "quiet"
    return "introduced" if "fingerprint" not in state else "delta"


def self_test():
    checks = [
        {"name": "build", "conclusion": "SUCCESS"},
        {"name": "Affected path validation", "conclusion": "FAILURE"},
        {"name": "(github.event_name == 'pull_request')", "conclusion": "FAILURE"},
    ]
    base = {
        "state": "OPEN", "draft": False, "merged": False, "head": "a" * 40, "review": "CHANGES_REQUESTED",
        "checks": checks, "reviews": [{"id": 1, "state": "CHANGES_REQUESTED"}],
        "review_comments": [], "issue_comments": [], "url": "https://example.invalid/1",
    }
    other = dict(base, checks=checks + [{"name": "Windows native build toolchain path", "conclusion": "SKIPPED"}])
    assert fingerprint(base) == fingerprint(other)
    ignored = dict(base, checks=[{"name": "(github.event_name == 'pull_request')", "conclusion": "FAILURE"}])
    assert failed_names(ignored["checks"]) == []
    base["fingerprint"] = fingerprint(base)
    now = datetime(2026, 11, 5, tzinfo=timezone.utc)
    fresh = (now - timedelta(hours=23)).isoformat()
    expired = (now - timedelta(days=1, seconds=1)).isoformat()
    assert decide({}, base, now, 86400) == "introduced"
    assert decide({"fingerprint": fingerprint(base), "started_at": fresh}, base, now, 86400) == "quiet"
    assert decide({"fingerprint": "stale", "started_at": fresh}, base, now, 86400) == "delta"
    assert decide({"fingerprint": fingerprint(base), "started_at": fresh}, dict(base, state="MERGED", merged=True), now, 86400) == "closed"
    assert decide({"fingerprint": fingerprint(base), "started_at": expired}, base, now, 86400) == "timeout"
    exact = (now - timedelta(seconds=86400)).isoformat()
    assert decide({"fingerprint": fingerprint(base), "started_at": exact}, base, now, 86400) == "timeout"
    assert decide({"fingerprint": fingerprint(base), "started_at": expired, "timeout_posted": True}, base, now, 86400) == "quiet"
    assert decide({"fingerprint": fingerprint(base), "started_at": expired}, dict(base, state="MERGED", merged=True), now, 86400) == "closed"
    text = note("delta", base, "1557246454714736723", {"reviews": [], "review_comments": []})
    assert "Affected path validation" in text and "github.event" not in text
    timeout = note("timeout", base, "1557246454714736723", owner="163670548818296832")
    assert "<@163670548818296832>" in timeout and "has not merged" in timeout and "one day" in timeout
    print("self-test=ok")


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Watch one pull request and post only when its review signal changes.")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--repo")
    parser.add_argument("--pr")
    parser.add_argument("--channel")
    parser.add_argument("--mention")
    parser.add_argument("--owner")
    parser.add_argument("--max-age-seconds", type=int, default=MAX_AGE_SECONDS)
    parser.add_argument("--state")
    parser.add_argument("--thread-name")
    parser.add_argument("--timer")
    parser.add_argument("--token-file", default="/run/user/1000/gajaeway-discord/token")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        return args
    if not args.repo or not REPO_NAME.fullmatch(args.repo):
        parser.error("--repo must be OWNER/NAME")
    if not args.pr or not args.pr.isdigit():
        parser.error("--pr must be a number")
    if not args.channel or not SNOWFLAKE.fullmatch(args.channel):
        parser.error("--channel must be a snowflake")
    if not args.mention or not SNOWFLAKE.fullmatch(args.mention):
        parser.error("--mention must be a snowflake")
    if not args.owner or not SNOWFLAKE.fullmatch(args.owner):
        parser.error("--owner must be a snowflake")
    if not 60 <= args.max_age_seconds <= MAX_AGE_SECONDS:
        parser.error("--max-age-seconds must be from 60 to 86400")
    if not args.state:
        parser.error("--state is required")
    args.thread_name = args.thread_name or f"PR {args.pr} review watch"
    return args


def stop_timer(timer):
    if timer:
        run(["systemctl", "--user", "disable", "--now", timer], 20)


def main(argv):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    os.environ.setdefault("XDG_RUNTIME_DIR", "/run/user/1000")
    now = datetime.now(timezone.utc)
    body = snapshot(args.repo, args.pr)
    state_path = Path(args.state)
    state = load_state(state_path, args.repo, args.pr)
    clock_missing = ensure_clock(state, now, args.max_age_seconds)
    action = decide(state, body, now, args.max_age_seconds)
    if state.get("closed_posted") or state.get("timeout_posted"):
        if not args.dry_run:
            stop_timer(args.timer)
        print(f"action=quiet head={body['head'][:12]}")
        return
    if args.dry_run or action == "quiet":
        if clock_missing and action == "quiet" and not args.dry_run:
            save_state(state_path, state)
        print(f"action={action} head={body['head'][:12]}")
        return
    token = read_token(args.token_file)
    me = discord(token, "GET", "/users/@me")
    expected = args.owner if action == "timeout" else args.mention
    if me.get("bot") is not True or me.get("id") in (args.mention, expected) or not SNOWFLAKE.fullmatch(str(me.get("id") or "")):
        raise WatchError("discord_author_mismatch")
    thread = post(
        token, state, args.channel, args.thread_name, expected,
        note(action, body, args.mention, state, owner=args.owner, max_age=args.max_age_seconds),
    )
    state["fingerprint"] = body["fingerprint"]
    state["reviews"] = body["reviews"]
    state["review_comments"] = body["review_comments"]
    if action == "closed":
        state["closed_posted"] = True
    if action == "timeout":
        state["timeout_posted"] = True
    save_state(state_path, state)
    if action in ("closed", "timeout"):
        stop_timer(args.timer)
    print(f"action={action} thread={thread}")


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except WatchError as error:
        print(f"error={error}")
        sys.exit(1)
