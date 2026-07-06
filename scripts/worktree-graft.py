#!/usr/bin/env -S uv run
# /// script
# dependencies = ["click", "rapidhash"]
# ///
"""
graft: Optimize a new git worktree by reusing cached state from another worktree.

Usage as a worktrunk pre-start hook in `.config/wt.toml`:
  [[pre-start]]
  warm-build-state = "wt step copy-ignored"

  [[pre-start]]
  repair-worktree-metadata = "scripts/worktree-graft.py {{ worktree_path }}"

What it does:
  - Copies tracked file timestamps (preserves build cache validity)
  - Copies populated submodules with their .git directories (avoids network fetches)
  - Copies or fixes CMake build directories and repairs embedded paths
  - Fixes ninja build files (.ninja_log hashes, .ninja_deps paths)

When a repo opts in via `.worktreeinclude`, pair this with
`wt step copy-ignored` as a separate hook step before running graft.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import click
import rapidhash

if TYPE_CHECKING:
    pass

# =============================================================================
# Configuration
# =============================================================================

# Build directory patterns to detect (relative to worktree root)
BUILD_DIR_PATTERNS = [
    "build/*/*",  # Nested presets: build/xcode/release, build/clang/debug
    "build/*",  # Flat presets: build/release, build/debug, build/RelWithDebInfo
    "build",  # Single build directory
    ".build",  # Alternative convention
    "_build",  # Another alternative
]

# Precompiled headers embed absolute source paths, so copied artifacts must be
# rebuilt locally in the target worktree. We restore their original mtimes after
# rebuilding so dependent object files stay cache-valid.
PRECOMPILED_HEADER_PATTERNS = [
    "*.pch",
    "*.gch",
]

# Lock handling for transient git index contention.
INDEX_LOCK_RETRY_ATTEMPTS = 8
INDEX_LOCK_RETRY_DELAY_SECONDS = 0.25

# Stale lock cleanup is opt-in to avoid deleting a lock owned by a live git process.
STALE_LOCK_MAX_AGE_SECONDS = 15 * 60
STALE_LOCK_CLEANUP_ENV = "GRAFT_REMOVE_STALE_INDEX_LOCK"


# =============================================================================
# UI Module: ANSI codes, logging, spinner
# =============================================================================

ANSI_ESCAPE = re.compile(
    r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\][^\x07]*\x07|\x1b[<>=?]?[0-9;]*[a-zA-Z]"
)

# ANSI codes
DIM = "\033[2m"
RESET = "\033[0m"
BLUE = "\033[34m"
GREEN = "\033[32m"
RED = "\033[31m"
MOVE_UP = "\033[A"
CLEAR_LINE = "\033[2K"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"

# Symbols
CHECK = "\u2714"
CROSS = "\u2718"
CIRCLE = "\u25cb"


# =============================================================================
# Logging
# =============================================================================


class _GraftHandler(logging.Handler):
    """Handler that integrates with the spinner."""

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        if _active_spinner:
            _active_spinner._clear_for_log()
        if record.levelno >= logging.INFO:
            print(f"{GREEN}{CHECK}{RESET} {message}", flush=True)
        else:
            print(f"{DIM}{CIRCLE} {message}{RESET}", flush=True)


_LOGGER = logging.getLogger("graft")
_LOGGER.addHandler(_GraftHandler())
_LOGGER.propagate = False
_LOGGER.setLevel(logging.INFO)


def _is_verbose() -> bool:
    """Check if verbose logging is enabled."""
    return _LOGGER.isEnabledFor(logging.DEBUG)


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text."""
    return ANSI_ESCAPE.sub("", text)


_active_spinner: Spinner | None = None


class Spinner:
    """A spinner that can track multiple concurrent tasks."""

    frames = "\u280b\u2819\u2839\u2838\u283c\u2834\u2826\u2827\u2807\u280f"

    def __init__(self) -> None:
        self._frame = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # Tasks: {name: {"status": "pending"|"active"|"done", "msg": str}}
        self._tasks: dict[str, dict[str, str]] = {}
        self._task_order: list[str] = []
        self._num_lines = 0
        # Tracks whether the cursor is positioned just below spinner output.
        # Logging can clear spinner lines and leave the cursor at the top.
        self._cursor_below_spinner = False

    def _render(self) -> list[str]:
        """Render all task lines."""
        lines = []
        for name in self._task_order:
            task = self._tasks.get(name)
            if not task:
                continue
            status = task["status"]
            msg = task["msg"]
            if status == "done":
                lines.append(f"{GREEN}{CHECK}{RESET} {DIM}{msg}{RESET}")
            elif status == "active":
                spinner_char = self.frames[self._frame % len(self.frames)]
                lines.append(f"{BLUE}{spinner_char}{RESET} {msg}")
            else:  # pending
                lines.append(f"  {DIM}{msg}{RESET}")
        return lines

    def _clear_for_log(self) -> None:
        """Clear all lines for logging output."""
        with self._lock:
            if self._num_lines > 0 and self._cursor_below_spinner:
                # Only clear when we know the cursor is below the spinner block.
                # This prevents clearing unrelated scrollback lines.
                for _ in range(self._num_lines):
                    print(f"{MOVE_UP}{CLEAR_LINE}", end="")
                print("\r", end="", flush=True)
                self._cursor_below_spinner = False

    def _spin(self) -> None:
        while not self._stop.wait(0.12):
            if _is_verbose():
                # In verbose mode, don't animate - just wait
                continue

            with self._lock:
                lines = self._render()
                prev_num_lines = self._num_lines

                # Move cursor up to overwrite previous output only when we are
                # currently positioned below the spinner block.
                if self._cursor_below_spinner and prev_num_lines > 0:
                    print(f"\033[{prev_num_lines}A", end="")

                # Print each line, clearing to end
                for line in lines:
                    print(f"{CLEAR_LINE}{line}", flush=True)

                # Clear any extra lines from previous render
                for _ in range(prev_num_lines - len(lines)):
                    print(CLEAR_LINE, flush=True)

                # Move cursor back up to end of current content
                extra_lines = prev_num_lines - len(lines)
                if extra_lines > 0:
                    print(f"\033[{extra_lines}A", end="", flush=True)

                self._num_lines = len(lines)
                self._frame += 1
                self._cursor_below_spinner = True

    def start(self, msg: str = "") -> None:
        """Start the spinner with an optional initial single task."""
        global _active_spinner
        if not _is_verbose():
            print(HIDE_CURSOR, end="", flush=True)
        if msg:
            self._tasks["main"] = {"status": "active", "msg": msg}
            self._task_order = ["main"]
        self._stop.clear()
        self._cursor_below_spinner = False
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        _active_spinner = self

    def update(self, msg: str) -> None:
        """Update the main task message (single-task mode)."""
        with self._lock:
            if "main" in self._tasks:
                self._tasks["main"]["msg"] = msg

    def add_task(self, name: str, msg: str, status: str = "active") -> None:
        """Add a new task to track."""
        with self._lock:
            self._tasks[name] = {"status": status, "msg": msg}
            if name not in self._task_order:
                self._task_order.append(name)

    def update_task(
        self, name: str, msg: str | None = None, status: str | None = None
    ) -> None:
        """Update a specific task's message or status."""
        with self._lock:
            if name in self._tasks:
                if msg is not None:
                    self._tasks[name]["msg"] = msg
                if status is not None:
                    self._tasks[name]["status"] = status

    def complete_task(self, name: str, msg: str | None = None) -> None:
        """Mark a task as completed."""
        self.update_task(name, msg=msg, status="done")

    def set_tasks(
        self, tasks: list[tuple[str, str]], active: list[str] | None = None
    ) -> None:
        """Set multiple tasks at once. tasks is a list of (name, msg) tuples.
        active is an optional list of task names to mark as initially active."""
        active = active or []
        with self._lock:
            self._tasks = {
                name: {"status": "active" if name in active else "pending", "msg": msg}
                for name, msg in tasks
            }
            self._task_order = [name for name, _ in tasks]

    def start_task(self, name: str) -> None:
        """Mark a task as active/running."""
        self.update_task(name, status="active")

    def stop(self) -> None:
        """Stop the spinner and print final state."""
        global _active_spinner
        if self._stop.is_set():
            return  # Already stopped
        self._stop.set()
        if self._thread:
            self._thread.join()
        _active_spinner = None

        with self._lock:
            num_lines = self._num_lines
            cursor_below_spinner = self._cursor_below_spinner

        # Clear spinner lines
        if num_lines > 0:
            # If logging already cleared the spinner block, the cursor is already
            # at the top of it and we must not move further up.
            if cursor_below_spinner:
                print(f"\033[{num_lines}A", end="")
            for _ in range(num_lines):
                print(f"{CLEAR_LINE}")
            print(f"\033[{num_lines}A", end="")
            with self._lock:
                self._num_lines = 0
                self._cursor_below_spinner = False
        print(SHOW_CURSOR, end="", flush=True)
        # Log completed tasks
        for name in self._task_order:
            task = self._tasks.get(name)
            if task and task["status"] == "done":
                _LOGGER.info(task["msg"])


@contextlib.contextmanager
def spinner(msg: str):
    """Show a spinner while a block executes, then clear the line."""
    s = Spinner()
    s.start(msg)
    try:
        yield s
    finally:
        s.stop()


# =============================================================================
# Git Utilities
# =============================================================================


def find_primary_worktree(worktree_path: Path) -> Path | None:
    """Find the primary worktree (source) for a given worktree."""
    # Get the git common dir - this points to the shared .git directory
    result = run_git(["rev-parse", "--git-common-dir"], cwd=worktree_path)
    if result.returncode != 0:
        return None

    # List all worktrees to find the primary one
    result = run_git(["worktree", "list", "--porcelain"], cwd=worktree_path)
    if result.returncode != 0:
        return None

    # Parse worktree list - first entry is typically the primary
    # Format: worktree /path/to/worktree\nHEAD ...\nbranch ...\n\n
    current_worktree = None
    for line in result.stdout.split("\n"):
        if line.startswith("worktree "):
            path = Path(line[9:])
            if current_worktree is None:
                # First worktree is the primary (main worktree)
                if path.resolve() != worktree_path.resolve():
                    return path
                # If the first worktree IS our target, keep looking
            current_worktree = path
        elif line == "":
            current_worktree = None

    # If we only found one worktree (ours), there's no source to copy from
    return None


def validate_worktrees(source: Path, target: Path) -> str | None:
    """Validate that source and target are valid worktrees of the same repo.

    Returns an error message if validation fails, None if valid.
    """
    # Check both paths exist
    if not source.exists():
        return f"Source path does not exist: {source}"
    if not target.exists():
        return f"Target path does not exist: {target}"

    # Check they're not the same path
    if source.resolve() == target.resolve():
        return "Source and target are the same path"

    # Check both are git worktrees
    for path, name in [(source, "Source"), (target, "Target")]:
        result = run_git(["rev-parse", "--git-dir"], cwd=path)
        if result.returncode != 0:
            return f"{name} is not a git repository: {path}"

    # Check they share the same git common dir (same repo)
    def get_common_dir(path: Path) -> Path | None:
        result = run_git(["rev-parse", "--git-common-dir"], cwd=path)
        if result.returncode != 0:
            return None
        return (path / result.stdout.strip()).resolve()

    source_common = get_common_dir(source)
    target_common = get_common_dir(target)

    if source_common is None:
        return f"Cannot determine git common dir for source: {source}"
    if target_common is None:
        return f"Cannot determine git common dir for target: {target}"
    if source_common != target_common:
        return "Source and target are not worktrees of the same repository"

    return None


@dataclass
class SubmoduleInfo:
    """Information about a submodule."""

    path: str


@dataclass(frozen=True)
class HookContext:
    """Subset of worktrunk hook context used by graft."""

    branch: str | None = None
    base: str | None = None
    base_worktree_path: Path | None = None
    primary_worktree_path: Path | None = None


_INDEX_LOCK_ERROR_RE = re.compile(
    r"(index\.lock|Unable to create .*\.lock|Another git process seems to be running)",
    re.IGNORECASE,
)


def _stderr_text(stderr: str | bytes | None) -> str:
    """Normalize stderr from subprocess for error matching/logging."""
    if stderr is None:
        return ""
    if isinstance(stderr, bytes):
        return stderr.decode(errors="replace")
    return stderr


def run_git(
    args: list[str],
    *,
    cwd: Path,
    text: bool = True,
    retry_on_index_lock: bool = False,
) -> subprocess.CompletedProcess:
    """Run a git command with optional retries on transient index.lock contention."""
    cmd = ["git", *args]
    attempts = INDEX_LOCK_RETRY_ATTEMPTS if retry_on_index_lock else 1
    result: subprocess.CompletedProcess | None = None

    for attempt in range(attempts):
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=text)
        if result.returncode == 0:
            return result

        stderr = _stderr_text(result.stderr)
        if not retry_on_index_lock or not _INDEX_LOCK_ERROR_RE.search(stderr):
            return result

        # Last attempt, return failure as-is.
        if attempt == attempts - 1:
            return result

        delay = INDEX_LOCK_RETRY_DELAY_SECONDS * (attempt + 1)
        _LOGGER.debug(
            f"Retrying git command after index lock contention ({attempt + 1}/{attempts - 1}): "
            f"`git {' '.join(args)}` in {cwd}"
        )
        time.sleep(delay)

    # Should be unreachable, but keeps type-checkers satisfied.
    if result is None:
        raise RuntimeError(f"git command produced no result: {' '.join(cmd)}")
    return result


def get_submodule_info(worktree_path: Path) -> list[SubmoduleInfo]:
    """Parse .gitmodules and return submodule information."""
    result = run_git(
        ["config", "--file", ".gitmodules", "--get-regexp", r"^submodule\..*\.path$"],
        cwd=worktree_path,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return []

    submodule_paths: list[str] = []
    for line in result.stdout.strip().split("\n"):
        # Format: submodule.<name>.path <path>
        _, path = line.split(maxsplit=1)
        submodule_paths.append(path)

    return [SubmoduleInfo(path=path) for path in submodule_paths]


def get_git_modules_dir(worktree_path: Path) -> Path | None:
    """Get the shared .git/modules directory for a worktree."""
    result = run_git(["rev-parse", "--git-common-dir"], cwd=worktree_path)
    if result.returncode != 0:
        return None
    return (worktree_path / result.stdout.strip() / "modules").resolve()


def get_worktree_git_dir(worktree_path: Path) -> Path | None:
    """Get the .git directory for a worktree."""
    result = run_git(["rev-parse", "--git-dir"], cwd=worktree_path)
    if result.returncode == 0:
        return (worktree_path / result.stdout.strip()).resolve()
    return None


def _hook_context_str(value: object) -> str | None:
    """Normalize optional string values from JSON hook context."""
    return value if isinstance(value, str) and value else None


def _hook_context_path(value: object) -> Path | None:
    """Normalize optional path values from JSON hook context."""
    text = _hook_context_str(value)
    return Path(text).expanduser() if text else None


def load_hook_context() -> HookContext | None:
    """Load worktrunk hook context from stdin when available."""
    if sys.stdin.isatty():
        return None

    try:
        raw = sys.stdin.read()
    except OSError as e:
        _LOGGER.debug(f"Could not read hook context from stdin: {e}")
        return None

    if not raw.strip():
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        _LOGGER.debug("stdin did not contain JSON hook context; continuing without it")
        return None

    if not isinstance(payload, dict):
        _LOGGER.debug("hook context on stdin was not a JSON object")
        return None

    return HookContext(
        branch=_hook_context_str(payload.get("branch")),
        base=_hook_context_str(payload.get("base")),
        base_worktree_path=_hook_context_path(payload.get("base_worktree_path")),
        primary_worktree_path=_hook_context_path(payload.get("primary_worktree_path")),
    )


def resolve_source_worktree(
    worktree_path: Path,
    source: str | None,
    hook_context: HookContext | None,
) -> Path | None:
    """Pick the best source worktree for grafting."""
    if source:
        return Path(source)

    if hook_context:
        # Keep this aligned with `wt step copy-ignored`, which defaults to the
        # primary worktree as its source. If this picked the base worktree first,
        # creating a branch from another feature worktree would copy ignored
        # files from primary but then repair paths as if they came from base.
        for candidate in (
            hook_context.primary_worktree_path,
            hook_context.base_worktree_path,
        ):
            if (
                candidate
                and candidate.exists()
                and candidate.resolve() != worktree_path.resolve()
            ):
                return candidate

    return find_primary_worktree(worktree_path)


def get_tracked_files(worktree_path: Path) -> list[Path]:
    """Return tracked regular files for a worktree."""
    result = run_git(["ls-files", "-z"], cwd=worktree_path, text=False)
    if result.returncode != 0:
        _LOGGER.debug(f"Could not list tracked files in {worktree_path}")
        return []

    tracked_files = []
    for entry in result.stdout.split(b"\x00"):
        if not entry:
            continue
        path = worktree_path / entry.decode(errors="surrogateescape")
        try:
            if path.is_file() and not path.is_symlink():
                tracked_files.append(path)
        except OSError:
            continue
    return tracked_files


def remove_stale_lock(worktree_path: Path) -> bool:
    """Optionally remove stale index.lock.

    Auto-removal is disabled by default because a long-running git process can
    legitimately hold the lock. To enable cleanup for very old lock files, set
    GRAFT_REMOVE_STALE_INDEX_LOCK=1.
    """
    git_dir = get_worktree_git_dir(worktree_path)
    if not git_dir:
        return False

    lock_file = git_dir / "index.lock"
    if not lock_file.exists():
        return False

    try:
        lock_age = time.time() - lock_file.stat().st_mtime
    except OSError as e:
        _LOGGER.debug(f"Could not stat lock file {lock_file}: {e}")
        return False

    if lock_age < STALE_LOCK_MAX_AGE_SECONDS:
        _LOGGER.debug(
            f"Detected active/recent index.lock ({lock_age:.0f}s old): {lock_file}"
        )
        return False

    if os.getenv(STALE_LOCK_CLEANUP_ENV) != "1":
        _LOGGER.warning(
            f"Found stale-looking index.lock ({lock_age:.0f}s old): {lock_file}. "
            f"Skipping auto-removal. Set {STALE_LOCK_CLEANUP_ENV}=1 to enable cleanup."
        )
        return False

    _LOGGER.warning(f"Removing stale index.lock ({lock_age:.0f}s old): {lock_file}")
    try:
        lock_file.unlink()
    except OSError as e:
        _LOGGER.warning(f"Failed to remove lock file {lock_file}: {e}")
        return False
    return True


def warn_if_index_lock_present(worktree_path: Path) -> None:
    """Log a warning if the target worktree currently has an index.lock file."""
    git_dir = get_worktree_git_dir(worktree_path)
    if not git_dir:
        return
    lock_file = git_dir / "index.lock"
    if not lock_file.exists():
        return

    age = "unknown"
    with contextlib.suppress(OSError):
        age = f"{time.time() - lock_file.stat().st_mtime:.0f}s"
    _LOGGER.warning(f"index.lock present in target worktree ({age} old): {lock_file}")

    # Best-effort cleanup for stale locks if explicitly enabled.
    remove_stale_lock(worktree_path)


# =============================================================================
# Ninja Utilities
# =============================================================================


def replace_path_once(content: str, src: str, dst: str) -> str:
    """Replace source paths while keeping already-fixed target paths intact."""
    if src == dst:
        return content
    placeholder = f"__GRAFT_TARGET_{rapidhash.rapidhash(dst.encode()):x}__"
    while placeholder in content:
        placeholder += "_"
    return content.replace(dst, placeholder).replace(src, dst).replace(placeholder, dst)


def replace_path_once_bytes(content: bytes, src: bytes, dst: bytes) -> bytes:
    """Binary variant of replace_path_once."""
    if src == dst:
        return content
    placeholder = f"__GRAFT_TARGET_{rapidhash.rapidhash(dst):x}__".encode()
    while placeholder in content:
        placeholder += b"_"
    return content.replace(dst, placeholder).replace(src, dst).replace(placeholder, dst)


def discover_ninja_build_dirs(root: Path) -> list[Path]:
    """Return all Ninja build roots under a copied/fixed build directory."""
    candidates = [root, *(path.parent for path in root.rglob("build.ninja"))]
    seen: set[Path] = set()
    result: list[Path] = []

    for candidate in candidates:
        marker = candidate / "build.ninja"
        if not marker.is_file():
            continue
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        result.append(candidate)

    return sorted(result)


def _record_ninja_command(
    output_to_command: dict[str, str], build_dir: Path, output: str, command: str
) -> None:
    """Store command lookups for both relative and absolute output paths."""
    output_to_command[output] = command
    if not output.startswith("/"):
        output_to_command[str(build_dir / output)] = command
    with contextlib.suppress(ValueError):
        rel_output = str(Path(output).relative_to(build_dir))
        output_to_command[rel_output] = command


def _read_ninja_logical_lines(path: Path) -> list[str]:
    """Read build.ninja while folding Ninja line continuations."""
    logical_lines: list[str] = []
    current = ""
    continuing = False

    for raw_line in path.read_text().splitlines():
        if continuing:
            current += raw_line.lstrip()
        else:
            current = raw_line

        if current.endswith("$"):
            current = current[:-1]
            continuing = True
            continue

        logical_lines.append(current)
        continuing = False

    if continuing:
        logical_lines.append(current)
    return logical_lines


def get_ninja_commands(build_dir: Path) -> dict[str, str]:
    """Get expanded commands for compile and custom Ninja edges."""
    output_to_command: dict[str, str] = {}

    # First gather compile commands from Ninja's compilation database.
    result = subprocess.run(
        ["ninja", "-t", "compdb"],
        cwd=build_dir,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        _LOGGER.debug(
            f"`ninja -t compdb` failed in {build_dir}: "
            f"{_stderr_text(result.stderr).strip()}"
        )
    else:
        try:
            entries = json.loads(result.stdout)
            for entry in entries:
                output = entry.get("output", "")
                command = entry.get("command", "")
                if output and command:
                    _record_ninja_command(output_to_command, build_dir, output, command)
        except json.JSONDecodeError as e:
            _LOGGER.debug(
                f"Could not parse `ninja -t compdb` output in {build_dir}: {e}"
            )

    # Then parse build.ninja for custom commands with an explicit COMMAND field.
    ninja_file = build_dir / "build.ninja"
    if not ninja_file.exists():
        return output_to_command

    lines = _read_ninja_logical_lines(ninja_file)
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.startswith("build "):
            i += 1
            continue

        header = line[len("build ") :]
        if ":" not in header:
            i += 1
            continue

        outputs_part, _rest = header.split(":", 1)
        outputs: list[str] = []
        for token in outputs_part.split():
            if token in {"|", "||"}:
                break
            outputs.append(token)

        i += 1
        command: str | None = None
        while i < len(lines) and lines[i].startswith("  "):
            variable = lines[i].strip()
            if variable.startswith("COMMAND = "):
                command = variable[len("COMMAND = ") :]
            i += 1

        if command:
            for output in outputs:
                _record_ninja_command(output_to_command, build_dir, output, command)

    return output_to_command


def fix_ninja_log(
    ninja_log_path: Path, build_dir: Path, output_to_command: dict[str, str]
) -> None:
    """Recompute command hashes and mtimes in .ninja_log based on actual files."""
    if not ninja_log_path.exists():
        return

    build_dir_str = str(build_dir) + "/"
    lines = ninja_log_path.read_text().split("\n")
    new_lines = []

    for line in lines:
        if line.startswith("#") or not line.strip():
            new_lines.append(line)
            continue

        parts = line.split("\t")
        if len(parts) != 5:
            new_lines.append(line)
            continue

        start, end, old_mtime, output, old_hash = parts

        # Get the actual file mtime
        if output.startswith("/"):
            output_path = Path(output)
            lookup_key = output
        else:
            output_path = build_dir / output
            # Try both relative and absolute paths for command lookup
            lookup_key = build_dir_str + output

        if output_path.exists():
            new_mtime = output_path.stat().st_mtime_ns
        else:
            new_mtime = old_mtime

        # Look up command for this output and recompute hash
        # Try the original output path first, then the converted absolute path
        command = output_to_command.get(output) or output_to_command.get(lookup_key)
        if command:
            new_hash = rapidhash.rapidhash(command.encode())
            new_lines.append(f"{start}\t{end}\t{new_mtime}\t{output}\t{new_hash:x}")
        else:
            new_lines.append(f"{start}\t{end}\t{new_mtime}\t{output}\t{old_hash}")

    ninja_log_path.write_text("\n".join(new_lines))


def fix_ninja_deps(ninja_deps_path: Path, src_str: str, dst_str: str) -> None:
    """Fix paths in .ninja_deps binary file.

    Format: header line + 4-byte version + mixed records. Path records store
    the path bytes, 0-3 NUL padding bytes, and a 4-byte checksum. Dependency
    records set the high bit of the size field and contain integer IDs and an
    mtime; copy them unchanged.
    """
    if not ninja_deps_path.exists():
        return

    content = ninja_deps_path.read_bytes()
    src_bytes = src_str.encode()
    dst_bytes = dst_str.encode()
    result = bytearray()
    changed = False

    # Copy header (first line + version)
    with contextlib.suppress(ValueError):
        header_end = content.index(b"\n") + 1 + 4
        if header_end <= len(content):
            result.extend(content[:header_end])
            i = header_end
        else:
            return
    if not result:
        return

    while i + 4 <= len(content):
        raw_rec_len = int.from_bytes(content[i : i + 4], "little")
        is_dependency_record = (raw_rec_len & 0x80000000) != 0
        rec_len = raw_rec_len & 0x7FFFFFFF
        if rec_len < 4 or i + 4 + rec_len > len(content):
            result.extend(content[i:])
            break

        rec_data = content[i + 4 : i + 4 + rec_len]
        if is_dependency_record:
            result.extend(content[i : i + 4 + rec_len])
            i += 4 + rec_len
            continue

        # Path records contain path bytes, up to three NUL padding bytes, and a
        # 4-byte checksum. There is no required NUL terminator when the path is
        # already 4-byte aligned.
        path_with_padding = rec_data[:-4]
        checksum = rec_data[-4:]
        padding_len = 0
        while (
            padding_len < 3
            and padding_len < len(path_with_padding)
            and path_with_padding[-(padding_len + 1)] == 0
        ):
            padding_len += 1
        old_path = path_with_padding[: len(path_with_padding) - padding_len]
        new_path = replace_path_once_bytes(old_path, src_bytes, dst_bytes)

        if new_path != old_path:
            changed = True
            new_padding_len = (-len(new_path)) % 4
            new_rec_len = len(new_path) + new_padding_len + 4
            result.extend(new_rec_len.to_bytes(4, "little"))
            result.extend(new_path)
            result.extend(b"\x00" * new_padding_len)
            result.extend(checksum)
        else:
            result.extend(content[i : i + 4 + rec_len])
        i += 4 + rec_len

    if changed:
        ninja_deps_path.write_bytes(bytes(result))


# =============================================================================
# Task System
# =============================================================================


class Task(ABC):
    """Base class for graft tasks."""

    name: str
    description: str

    @abstractmethod
    def should_run(self, source: Path, target: Path) -> bool:
        """Check if this task should run for the given worktrees."""
        ...

    @abstractmethod
    def run(self, source: Path, target: Path, spinner: Spinner) -> None:
        """Execute the task."""
        ...

    def get_subtasks(self) -> list[tuple[str, str]]:
        """Return list of (name, description) tuples for subtasks.

        Override this if the task has subtasks that should be shown in the spinner.
        """
        return [(self.name, self.description)]


class TimestampTask(Task):
    """Copy file timestamps from source to target worktree."""

    name = "timestamps"
    description = "copying file timestamps"

    def should_run(self, source: Path, target: Path) -> bool:
        # Always run if source exists
        return source.exists()

    def run(self, source: Path, target: Path, spinner: Spinner) -> None:
        def copy_timestamp(src_file: Path) -> None:
            rel_path = src_file.relative_to(source)
            dst_file = target / rel_path
            if dst_file.exists():
                try:
                    src_stat = src_file.stat()
                    os.utime(dst_file, (src_stat.st_atime, src_stat.st_mtime))
                except OSError:
                    pass  # Skip files that can't be stat'd or utime'd

        files = get_tracked_files(source)
        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(copy_timestamp, files))

        spinner.complete_task(self.name)


class SubmoduleTask(Task):
    """Copy populated submodules from source to target worktree."""

    name = "submodules"
    description = "copying submodules"

    def __init__(self) -> None:
        self._submodules_to_copy: list[tuple[str, Path, Path]] = []
        self._git_modules_dir: Path | None = None

    def should_run(self, source: Path, target: Path) -> bool:
        # Check if target has .gitmodules
        gitmodules = target / ".gitmodules"
        return gitmodules.exists()

    def get_subtasks(self) -> list[tuple[str, str]]:
        return [
            (self.name, self.description),
            ("submodules_update", "updating submodules"),
        ]

    @staticmethod
    def _has_entries(path: Path) -> bool:
        try:
            return path.is_dir() and any(path.iterdir())
        except OSError:
            return False

    def _prepare(self, source: Path, target: Path) -> None:
        """Prepare submodule lists for copying."""
        self._submodules_to_copy = []
        self._git_modules_dir = get_git_modules_dir(source)

        for info in get_submodule_info(target):
            src = source / info.path
            dst = target / info.path
            src_populated = self._has_entries(src)
            dst_empty = not self._has_entries(dst)

            if src_populated and dst_empty:
                self._submodules_to_copy.append((info.path, src, dst))
            elif not src_populated:
                _LOGGER.debug(
                    f"Skipping unpopulated submodule {info.path}; "
                    "run `git submodule update --init` explicitly if needed"
                )

    def run(self, source: Path, target: Path, spinner: Spinner) -> None:
        self._prepare(source, target)
        self._copy_submodules(source)
        spinner.complete_task(self.name)

        spinner.start_task("submodules_update")
        self._update_submodule_commits(target)
        spinner.complete_task("submodules_update")

    def _copy_submodules(self, source: Path) -> None:
        """Copy submodule working trees and .git directories."""
        if not self._submodules_to_copy:
            return

        def ignore_git(directory: str, files: list[str]) -> list[str]:
            return [".git"] if ".git" in files else []

        def copy_submodule(args: tuple[str, Path, Path]) -> None:
            submodule_path, src, dst = args
            # Safety check: never modify source directory
            if dst.is_relative_to(source):
                raise RuntimeError(f"Refusing to copy into source tree: {dst}")
            if dst.exists():
                shutil.rmtree(dst)
            _LOGGER.debug(f"Copying submodule {submodule_path}")

            # Copy working tree files (exclude .git) with retry
            for attempt in range(3):
                try:
                    shutil.copytree(src, dst, symlinks=True, ignore=ignore_git)
                    break
                except (shutil.Error, OSError) as e:
                    if attempt == 2:
                        raise
                    _LOGGER.debug(f"Retrying copy of {submodule_path}: {e}")
                    if dst.exists():
                        shutil.rmtree(dst)
                    time.sleep(0.1)

            # Copy git data from shared modules to create standalone .git/ directory
            if self._git_modules_dir:
                src_git_dir = self._git_modules_dir / submodule_path
                dst_git_dir = dst / ".git"
                if src_git_dir.exists():
                    for attempt in range(3):
                        try:
                            shutil.copytree(src_git_dir, dst_git_dir, symlinks=True)
                            break
                        except (shutil.Error, OSError) as e:
                            if attempt == 2:
                                raise
                            _LOGGER.debug(
                                f"Retrying .git copy for {submodule_path}: {e}"
                            )
                            if dst_git_dir.exists():
                                shutil.rmtree(dst_git_dir)
                            time.sleep(0.1)

                    # Make all files writable (git pack files are read-only)
                    for f in dst_git_dir.rglob("*"):
                        if f.is_file():
                            with contextlib.suppress(OSError):
                                f.chmod(f.stat().st_mode | 0o200)

                    # Remove core.worktree - not needed when .git is inside working tree
                    config_file = dst_git_dir / "config"
                    # Run from a safe repo cwd. If core.worktree is stale, running
                    # git commands from `dst` itself can fail before config editing.
                    unset_result = run_git(
                        [
                            "config",
                            "-f",
                            str(config_file),
                            "--unset-all",
                            "core.worktree",
                        ],
                        cwd=source,
                    )
                    if unset_result.returncode != 0:
                        _LOGGER.debug(
                            f"Could not unset core.worktree in {config_file}: "
                            f"{_stderr_text(unset_result.stderr).strip()}"
                        )

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(copy_submodule, self._submodules_to_copy))

    def _update_submodule_commits(self, target: Path) -> None:
        """Checkout the correct commit for each submodule.

        Uses ls-tree to get the commit the branch expects, not current state.
        Does not use `git submodule update` which modifies shared .git/modules config.
        """
        result = run_git(["ls-tree", "-rz", "HEAD"], cwd=target, text=False)
        if result.returncode != 0:
            _LOGGER.debug(
                f"Could not list submodule commits in {target}: "
                f"{_stderr_text(result.stderr).strip()}"
            )
            return

        for entry in result.stdout.split(b"\x00"):
            if not entry:
                continue
            # Format: <mode> <type> <object>\t<path>\0
            if b"\t" not in entry:
                continue
            meta, path_bytes = entry.split(b"\t", 1)
            parts = meta.decode(errors="replace").split()
            if len(parts) < 3 or parts[1] != "commit":
                continue
            commit = parts[2]
            path = path_bytes.decode(errors="surrogateescape")
            submodule_dir = target / path
            if not self._has_entries(submodule_dir):
                continue
            checkout_result = run_git(
                ["checkout", "--detach", "--quiet", commit],
                cwd=submodule_dir,
                retry_on_index_lock=True,
            )
            if checkout_result.returncode != 0:
                _LOGGER.warning(
                    f"Failed to checkout submodule {path} at {commit}: "
                    f"{_stderr_text(checkout_result.stderr).strip()}"
                )


class BuildTask(Task):
    """Copy build directories from source to target and fix paths."""

    name = "build"
    description = "processing build directory"

    def __init__(self) -> None:
        self._build_dirs_to_copy: list[tuple[Path, Path]] = []
        self._build_dirs_to_fix: list[Path] = []

    def should_run(self, source: Path, target: Path) -> bool:
        self._detect_build_dirs(source, target)
        return len(self._build_dirs_to_copy) > 0 or len(self._build_dirs_to_fix) > 0

    def get_subtasks(self) -> list[tuple[str, str]]:
        return [
            (self.name, self.description),
            ("cmake_paths", "fixing CMake paths"),
            ("ninja_files", "fixing ninja files"),
            ("pch_files", "refreshing precompiled headers"),
        ]

    def _detect_build_dirs(self, source: Path, target: Path) -> None:
        """Detect build directories to copy and existing directories to fix."""
        self._build_dirs_to_copy = []
        self._build_dirs_to_fix = []

        for pattern in BUILD_DIR_PATTERNS:
            if "*" in pattern:
                # Check source dirs to copy
                for src_dir in source.glob(pattern):
                    if src_dir.is_dir():
                        rel_path = src_dir.relative_to(source)
                        dst_dir = target / rel_path
                        if not dst_dir.exists():
                            self._build_dirs_to_copy.append((src_dir, dst_dir))
                # Check target dirs to fix (including target-only dirs)
                for dst_dir in target.glob(pattern):
                    if dst_dir.is_dir() and dst_dir not in self._build_dirs_to_fix:
                        self._build_dirs_to_fix.append(dst_dir)
            else:
                src_dir = source / pattern
                dst_dir = target / pattern
                if src_dir.is_dir() and not dst_dir.exists():
                    self._build_dirs_to_copy.append((src_dir, dst_dir))
                if dst_dir.is_dir() and dst_dir not in self._build_dirs_to_fix:
                    self._build_dirs_to_fix.append(dst_dir)

        # Remove parent directories if children are already in the lists
        # (e.g., don't copy/fix "build" if "build/release" is already listed)
        dst_paths = {dst for _, dst in self._build_dirs_to_copy}
        self._build_dirs_to_copy = [
            (src, dst)
            for src, dst in self._build_dirs_to_copy
            if not any(
                other != dst and other.is_relative_to(dst) for other in dst_paths
            )
        ]

        fix_paths = set(self._build_dirs_to_fix)
        self._build_dirs_to_fix = [
            dst
            for dst in self._build_dirs_to_fix
            if not any(
                other != dst and other.is_relative_to(dst) for other in fix_paths
            )
        ]

    def run(self, source: Path, target: Path, spinner: Spinner) -> None:
        src_str = str(source)
        dst_str = str(target)

        # Symlink CMakeUserPresets.json if it exists
        self._symlink_cmake_presets(source, target)

        # Copy all detected build directories
        copied_dirs = self._copy_build_dirs()
        spinner.complete_task(self.name)

        # Fix paths in both copied and existing directories
        all_dirs_to_fix = copied_dirs + self._build_dirs_to_fix

        if not all_dirs_to_fix:
            spinner.complete_task("cmake_paths")
            spinner.complete_task("ninja_files")
            spinner.complete_task("pch_files")
            return

        spinner.start_task("cmake_paths")
        self._fix_cmake_paths(all_dirs_to_fix, src_str, dst_str)
        spinner.complete_task("cmake_paths")

        spinner.start_task("ninja_files")
        self._fix_ninja_files(all_dirs_to_fix, src_str, dst_str)
        spinner.complete_task("ninja_files")

        spinner.start_task("pch_files")
        self._refresh_precompiled_headers(all_dirs_to_fix)
        spinner.complete_task("pch_files")

    def _symlink_cmake_presets(self, source: Path, target: Path) -> None:
        """Symlink CMakeUserPresets.json if it exists in source parent."""
        user_presets_src = source.parent / "CMakeUserPresets.json"
        user_presets_dst = target / "CMakeUserPresets.json"
        if user_presets_src.exists() and not user_presets_dst.exists():
            with contextlib.suppress(OSError):
                user_presets_dst.symlink_to(user_presets_src)

    def _copy_build_dirs(self) -> list[Path]:
        """Copy all detected build directories. Returns list of copied destination dirs."""
        copied = []
        for src_build, dst_build in self._build_dirs_to_copy:
            _LOGGER.debug(f"Copying {src_build.name}")
            for attempt in range(3):
                try:
                    shutil.copytree(src_build, dst_build, symlinks=True)
                    copied.append(dst_build)
                    break
                except (shutil.Error, OSError) as e:
                    if attempt == 2:
                        raise
                    _LOGGER.debug(f"Retrying build copy: {e}")
                    if dst_build.exists():
                        shutil.rmtree(dst_build)
                    time.sleep(0.1)
        return copied

    def _refresh_precompiled_headers(self, build_dirs: list[Path]) -> None:
        """Rebuild copied precompiled headers and restore their original mtimes."""
        for build_dir in build_dirs:
            for ninja_build_dir in discover_ninja_build_dirs(build_dir):
                pch_outputs = sorted(
                    {
                        path
                        for pattern in PRECOMPILED_HEADER_PATTERNS
                        for path in ninja_build_dir.rglob(pattern)
                        if path.is_file() and not path.is_symlink()
                    }
                )
                if not pch_outputs:
                    continue

                backups: list[tuple[Path, Path, os.stat_result]] = []
                for output in pch_outputs:
                    backup = output.with_name(f"{output.name}.graft-backup")
                    with contextlib.suppress(FileNotFoundError):
                        backup.unlink()
                    shutil.move(output, backup)
                    backups.append((output, backup, backup.stat()))

                try:
                    command = [
                        "ninja",
                        *[
                            str(output.relative_to(ninja_build_dir))
                            for output, _, _ in backups
                        ],
                    ]
                    kwargs: dict[str, object] = {"cwd": ninja_build_dir, "check": True}
                    if not _is_verbose():
                        kwargs["stdout"] = subprocess.DEVNULL
                        kwargs["stderr"] = subprocess.PIPE
                    subprocess.run(command, **kwargs)
                except (OSError, subprocess.CalledProcessError):
                    for output, backup, _ in backups:
                        if backup.exists():
                            with contextlib.suppress(OSError):
                                shutil.move(backup, output)
                    raise
                else:
                    for output, backup, stat in backups:
                        os.utime(output, (stat.st_atime, stat.st_mtime))
                        with contextlib.suppress(FileNotFoundError):
                            backup.unlink()

    def _fix_cmake_paths(
        self, build_dirs: list[Path], src_str: str, dst_str: str
    ) -> None:
        """Fix paths in all CMake-generated files."""

        def fix_file(path: Path) -> None:
            try:
                content = path.read_text()
                new_content = replace_path_once(content, src_str, dst_str)
                if new_content != content:
                    stat = path.stat()
                    path.write_text(new_content)
                    os.utime(path, (stat.st_atime, stat.st_mtime))
            except (UnicodeDecodeError, OSError):
                pass  # Skip binary files

        for build_dir in build_dirs:
            files = [
                p for p in build_dir.rglob("*") if p.is_file() and not p.is_symlink()
            ]
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(fix_file, files))

    def _fix_ninja_files(
        self, build_dirs: list[Path], src_str: str, dst_str: str
    ) -> None:
        """Fix ninja log hashes and deps paths in all build directories."""
        for build_dir in build_dirs:
            ninja_build_dirs = discover_ninja_build_dirs(build_dir)
            if not ninja_build_dirs:
                _LOGGER.debug(f"No Ninja build directories found under {build_dir}")
                continue

            for ninja_build_dir in ninja_build_dirs:
                # Run ninja_deps fixing and get_ninja_commands in parallel.
                with ThreadPoolExecutor(max_workers=2) as executor:
                    deps_future = executor.submit(
                        fix_ninja_deps,
                        ninja_build_dir / ".ninja_deps",
                        src_str,
                        dst_str,
                    )
                    commands_future = executor.submit(
                        get_ninja_commands, ninja_build_dir
                    )
                    deps_future.result()
                    output_to_command = commands_future.result()

                # Recompute command hashes in .ninja_log.
                ninja_log = ninja_build_dir / ".ninja_log"
                if ninja_log.exists():
                    fix_ninja_log(ninja_log, ninja_build_dir, output_to_command)


# =============================================================================
# Task Runner
# =============================================================================


@dataclass
class TaskRunner:
    """Orchestrates parallel execution of graft tasks."""

    source: Path
    target: Path
    tasks: list[Task] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Register all task types
        self.tasks = [
            TimestampTask(),
            SubmoduleTask(),
            BuildTask(),
        ]

    def get_applicable_tasks(self) -> list[Task]:
        """Return list of tasks that should run for this worktree pair."""
        return [t for t in self.tasks if t.should_run(self.source, self.target)]

    def run(self) -> None:
        """Run all applicable tasks in parallel."""
        # Report lock state up front and optionally clean stale lock files.
        warn_if_index_lock_present(self.target)

        applicable = self.get_applicable_tasks()
        if not applicable:
            _LOGGER.debug("No tasks to run")
            return

        # Collect all subtasks for spinner display
        all_subtasks: list[tuple[str, str]] = []
        initial_active: list[str] = []
        for task in applicable:
            subtasks = task.get_subtasks()
            all_subtasks.extend(subtasks)
            # Mark first subtask of each task as initially active
            if subtasks:
                initial_active.append(subtasks[0][0])

        s = Spinner()
        s.set_tasks(all_subtasks, active=initial_active)
        s.start()

        try:
            # Run all tasks in parallel
            # Tasks operate on disjoint paths:
            # - TimestampTask: existing tracked files only
            # - SubmoduleTask: <submodule>/ directories
            # - BuildTask: build/*/ directories
            with ThreadPoolExecutor(max_workers=len(applicable)) as executor:
                futures = {
                    executor.submit(task.run, self.source, self.target, s): task
                    for task in applicable
                }
                error = None
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        if error is None:
                            error = (futures[future], e)
                if error:
                    s.stop()
                    task, exc = error
                    print(f"{RED}{CROSS}{RESET} {task.description}")
                    print(f"  {exc}")
                    sys.exit(1)
        finally:
            s.stop()


# =============================================================================
# CLI
# =============================================================================


@click.command()
@click.argument("worktree_path", type=click.Path(exists=True, resolve_path=True))
@click.option(
    "-s",
    "--source",
    type=click.Path(exists=True, resolve_path=True),
    help="Source worktree path (default: auto-detect primary worktree)",
)
@click.option(
    "--remote-url",
    hidden=True,
    help="Deprecated compatibility option; ignored.",
)
@click.option(
    "--branch",
    hidden=True,
    help="Deprecated compatibility option; ignored.",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def main(
    worktree_path: str,
    source: str | None,
    remote_url: str | None,
    branch: str | None,
    verbose: bool,
) -> None:
    """Optimize a new git worktree by grafting cached state from a source worktree."""
    _LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)

    target = Path(worktree_path)
    hook_context = load_hook_context()

    # Find source worktree
    source_path = resolve_source_worktree(target, source, hook_context)
    if source_path is None:
        _LOGGER.debug("No source worktree found, nothing to graft")
        sys.exit(0)

    # Validate worktrees
    validation_error = validate_worktrees(source_path, target)
    if validation_error:
        print(f"{RED}{CROSS}{RESET} {validation_error}")
        sys.exit(1)

    _LOGGER.debug(f"Source: {source_path}")
    _LOGGER.debug(f"Target: {target}")

    # Run all applicable tasks
    runner = TaskRunner(
        source=source_path,
        target=target,
    )
    runner.run()


if __name__ == "__main__":
    main()
