#!/usr/bin/env python3
"""
rewrite_alt_operators.py

Conservatively rewrite C++ logical operators to their alternative spellings:

    !   -> not
    &&  -> and
    ||  -> or
    !!  -> !!
    ! ! -> ! !

Rules:
- Skip comments, strings, char literals, raw strings, and preprocessor lines.
- Rewrite && only when surrounded by whitespace.
- Rewrite || only when surrounded by whitespace.
- Rewrite ! when:
  - it is not followed by '='
  - it is not part of a double negation '!!' or '! !'
  - it is not part of an overloaded operator spelling
  - it is followed by optional whitespace and then something that looks like
    an expression start
- Normalize '!x', '! x', '!   x' all to 'not x'.

This is intentionally conservative and formatter-like, not a full C++ parser.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Literal


type ChunkKind = Literal["code", "opaque"]
type Chunk = tuple[ChunkKind, str]

OPERATOR = "operator"


def is_ws(ch: str) -> bool:
    return ch.isspace()


def is_expr_start(ch: str) -> bool:
    return ch.isalnum() or ch == "_" or ch in "([{\"'~+-*&#:"


def is_ident(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def is_digit_separator_at(src: str, quote_index: int) -> bool:
    if quote_index == 0 or quote_index + 1 >= len(src):
        return False
    if not src[quote_index - 1].isalnum() or not src[quote_index + 1].isalnum():
        return False

    token_start = quote_index - 1
    while token_start >= 0 and (
        src[token_start].isalnum() or src[token_start] in "_'."
    ):
        token_start -= 1
    token_prefix = src[token_start + 1 : quote_index].replace("'", "")
    return token_prefix[:1].isdigit() or (
        len(token_prefix) >= 2 and token_prefix[0] == "." and token_prefix[1].isdigit()
    )


def is_operator_symbol_at(code: str, symbol_index: int) -> bool:
    j = symbol_index - 1
    while j >= 0 and code[j].isspace():
        j -= 1

    operator_start = j - len(OPERATOR) + 1
    if operator_start < 0 or code[operator_start : j + 1] != OPERATOR:
        return False

    before = code[operator_start - 1] if operator_start > 0 else ""
    return not before or not is_ident(before)


def split_code_and_opaque(src: str) -> Iterator[Chunk]:
    i = 0
    n = len(src)
    buf: list[str] = []

    def flush_code() -> Iterator[Chunk]:
        nonlocal buf
        if buf:
            yield ("code", "".join(buf))
            buf = []

    while i < n:
        ch = src[i]

        # Preprocessor directive at logical line start.
        if ch == "#":
            j = i - 1
            while j >= 0 and src[j] in " \t":
                j -= 1
            if j < 0 or src[j] == "\n":
                yield from flush_code()
                start = i
                i += 1
                while i < n:
                    if src[i] == "\n":
                        k = i - 1
                        backslashes = 0
                        while k >= start and src[k] == "\\":
                            backslashes += 1
                            k -= 1
                        i += 1
                        if backslashes % 2 == 0:
                            break
                    else:
                        i += 1
                yield ("opaque", src[start:i])
                continue

        # Line comment
        if ch == "/" and i + 1 < n and src[i + 1] == "/":
            yield from flush_code()
            start = i
            i += 2
            while i < n and src[i] != "\n":
                i += 1
            if i < n:
                i += 1
            yield ("opaque", src[start:i])
            continue

        # Block comment
        if ch == "/" and i + 1 < n and src[i + 1] == "*":
            yield from flush_code()
            start = i
            i += 2
            while i + 1 < n and not (src[i] == "*" and src[i + 1] == "/"):
                i += 1
            i = min(i + 2, n)
            yield ("opaque", src[start:i])
            continue

        # Raw string literal: R"delim(... )delim"
        if ch == "R" and i + 1 < n and src[i + 1] == '"':
            yield from flush_code()
            start = i
            j = i + 2
            while j < n and src[j] != "(":
                j += 1
            if j >= n:
                yield ("opaque", src[start:])
                return
            delim = src[i + 2 : j]
            close = ")" + delim + '"'
            k = src.find(close, j + 1)
            if k == -1:
                yield ("opaque", src[start:])
                return
            i = k + len(close)
            yield ("opaque", src[start:i])
            continue

        # Normal string literal
        if ch == '"':
            yield from flush_code()
            start = i
            i += 1
            while i < n:
                if src[i] == "\\":
                    i += 2
                elif src[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            yield ("opaque", src[start:i])
            continue

        # Character literal
        if ch == "'":
            if is_digit_separator_at(src, i):
                buf.append(ch)
                i += 1
                continue
            yield from flush_code()
            start = i
            i += 1
            while i < n:
                if src[i] == "\\":
                    i += 2
                elif src[i] == "'":
                    i += 1
                    break
                else:
                    i += 1
            yield ("opaque", src[start:i])
            continue

        buf.append(ch)
        i += 1

    yield from flush_code()


def rewrite_code(code: str) -> str:
    out: list[str] = []
    i = 0
    n = len(code)

    while i < n:
        if code.startswith("&&", i):
            if is_operator_symbol_at(code, i):
                out.append("&&")
                i += 2
                continue
            prev_ch = code[i - 1] if i > 0 else ""
            next_ch = code[i + 2] if i + 2 < n else ""
            if prev_ch and next_ch and is_ws(prev_ch) and is_ws(next_ch):
                out.append("and")
                i += 2
                continue

        if code.startswith("||", i):
            if is_operator_symbol_at(code, i):
                out.append("||")
                i += 2
                continue
            prev_ch = code[i - 1] if i > 0 else ""
            next_ch = code[i + 2] if i + 2 < n else ""
            if prev_ch and next_ch and is_ws(prev_ch) and is_ws(next_ch):
                out.append("or")
                i += 2
                continue

        if code.startswith("!!", i):
            out.append("!!")
            i += 2
            continue

        if code[i] == "!":
            if is_operator_symbol_at(code, i):
                out.append("!")
                i += 1
                continue

            j = i + 1
            while j < n and code[j].isspace():
                j += 1
            if j < n and code[j] == "!" and (j + 1 == n or code[j + 1] != "="):
                out.append(code[i : j + 1])
                i = j + 1
                continue

            next_ch = code[i + 1] if i + 1 < n else ""
            if next_ch == "=":
                out.append("!")
                i += 1
                continue

            after_ws = code[j] if j < n else ""
            if after_ws and is_expr_start(after_ws):
                out.append("not ")
                i = j
                continue

        out.append(code[i])
        i += 1

    return "".join(out)


def rewrite_text(src: str) -> str:
    parts: list[str] = []
    for kind, text in split_code_and_opaque(src):
        match kind:
            case "opaque":
                parts.append(text)
            case "code":
                parts.append(rewrite_code(text))
    return "".join(parts)


def process_path(path: Path, *, in_place: bool, check_only: bool) -> int:
    try:
        original = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        print(f"error: failed to read {path}: {error}", file=sys.stderr)
        return 2

    rewritten = rewrite_text(original)

    if check_only:
        if rewritten != original:
            print(path)
            return 1
        return 0

    if in_place:
        if rewritten != original:
            try:
                path.write_text(rewritten, encoding="utf-8")
            except OSError as error:
                print(f"error: failed to write {path}: {error}", file=sys.stderr)
                return 2
        return 0

    sys.stdout.write(rewritten)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", help="Files to process")
    parser.add_argument(
        "-i", "--in-place", action="store_true", help="Rewrite files in place"
    )
    parser.add_argument(
        "--check", action="store_true", help="Exit 1 if any file would change"
    )
    parser.add_argument(
        "--stdin", action="store_true", help="Read from stdin, write to stdout"
    )
    args = parser.parse_args(argv)

    if args.stdin:
        if args.paths:
            print("error: --stdin cannot be combined with file paths", file=sys.stderr)
            return 2
        if args.in_place:
            print("error: --stdin cannot be combined with --in-place", file=sys.stderr)
            return 2
        original = sys.stdin.read()
        rewritten = rewrite_text(original)
        if args.check:
            return 1 if rewritten != original else 0
        sys.stdout.write(rewritten)
        return 0

    if not args.paths:
        print("error: no input files", file=sys.stderr)
        return 2

    rc = 0
    for p in args.paths:
        rc = max(
            rc, process_path(Path(p), in_place=args.in_place, check_only=args.check)
        )
    return rc


def test_rewrites_logical_operators_conservatively() -> None:
    source = "\n".join(
        [
            "if (!foo && bar || !   (baz) || a&&b || c||d) {}",
            "if (!!foo || !! foo || !!(foo) || ! !foo || ! ! foo) {}",
            "",
        ]
    )

    assert rewrite_text(source) == "\n".join(
        [
            "if (not foo and bar or not (baz) or a&&b or c||d) {}",
            "if (!!foo or !! foo or !!(foo) or ! !foo or ! ! foo) {}",
            "",
        ]
    )


def test_rewrites_after_digit_separators() -> None:
    source = "auto high = 0b1000'0000; if (high && !::foo()) {}\n"

    assert (
        rewrite_text(source)
        == "auto high = 0b1000'0000; if (high and not ::foo()) {}\n"
    )


def test_leaves_opaque_regions_unchanged() -> None:
    source = "\n".join(
        [
            "#if !defined(FOO) && BAR",
            'auto text = "(!foo && bar)";',
            'auto raw = R"cpp(!foo && bar)cpp";',
            "auto ch = '!';",
            "/* !foo && bar */",
            "// !foo && bar",
            "if (!foo && bar) {}",
            "",
        ]
    )

    assert rewrite_text(source) == "\n".join(
        [
            "#if !defined(FOO) && BAR",
            'auto text = "(!foo && bar)";',
            'auto raw = R"cpp(!foo && bar)cpp";',
            "auto ch = '!';",
            "/* !foo && bar */",
            "// !foo && bar",
            "if (not foo and bar) {}",
            "",
        ]
    )


def test_leaves_operator_overload_declarations_unchanged() -> None:
    source = "\n".join(
        [
            "auto operator&&(T, T) -> T;",
            "auto operator && (T, T) -> T;",
            "auto operator||(T, T) -> T;",
            "auto operator || (T, T) -> T;",
            "auto operator!() -> bool;",
            "auto operator ! () -> bool;",
            "auto operator!=(T) -> bool;",
            "auto operator != (T) -> bool;",
            "",
        ]
    )

    assert rewrite_text(source) == source


def test_stdin_cannot_be_combined_with_in_place(capsys) -> None:
    assert main(["--stdin", "--in-place"]) == 2
    assert "cannot be combined with --in-place" in capsys.readouterr().err


if __name__ == "__main__":
    raise SystemExit(main())
