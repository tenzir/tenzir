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
- Do not rewrite && when it is an rvalue ref-qualifier for a member function.
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
CV_QUALIFIERS = frozenset({"const", "volatile"})
CONTROL_KEYWORDS_BEFORE_PAREN = frozenset(
    {
        "catch",
        "for",
        "if",
        "not",
        "noexcept",
        "requires",
        "return",
        "sizeof",
        "switch",
        "while",
    }
)
REF_QUALIFIER_FOLLOWER_KEYWORDS = frozenset(
    {"final", "noexcept", "override", "requires", "try"}
)
REF_QUALIFIER_FOLLOWER_CHARS = frozenset("{;=")


def is_ws(ch: str) -> bool:
    return ch.isspace()


def is_expr_start(ch: str) -> bool:
    return ch.isalnum() or ch == "_" or ch in "([{\"'~+-*&#:"


def is_ident(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def skip_ws_forward(code: str, index: int) -> int:
    while index < len(code) and code[index].isspace():
        index += 1
    return index


def skip_ws_backward(code: str, index: int) -> int:
    while index >= 0 and code[index].isspace():
        index -= 1
    return index


def read_identifier_backward(code: str, end: int) -> tuple[str, int]:
    start = end
    while start >= 0 and is_ident(code[start]):
        start -= 1
    return code[start + 1 : end + 1], start


def read_identifier_forward(code: str, start: int) -> tuple[str, int]:
    end = start
    while end < len(code) and is_ident(code[end]):
        end += 1
    return code[start:end], end


def find_matching_open_paren(code: str, close_paren_index: int) -> int | None:
    depth = 0
    for index in range(close_paren_index, -1, -1):
        if code[index] == ")":
            depth += 1
        elif code[index] == "(":
            depth -= 1
            if depth == 0:
                return index
    return None


def skip_balanced_parens_forward(code: str, open_paren_index: int) -> int | None:
    depth = 0
    for index in range(open_paren_index, len(code)):
        if code[index] == "(":
            depth += 1
        elif code[index] == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    return None


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


def is_ref_qualifier_follower_at(code: str, index: int) -> bool:
    index = skip_ws_forward(code, index)
    if index == len(code):
        return True
    if code.startswith("->", index) or code.startswith("[[", index):
        return True
    if code[index] in REF_QUALIFIER_FOLLOWER_CHARS:
        return True
    if not is_ident(code[index]):
        return False

    keyword, index = read_identifier_forward(code, index)
    if keyword not in REF_QUALIFIER_FOLLOWER_KEYWORDS:
        return False
    if keyword != "noexcept":
        return True

    index = skip_ws_forward(code, index)
    if index < len(code) and code[index] == "(":
        index = skip_balanced_parens_forward(code, index)
        if index is None:
            return True
    return is_ref_qualifier_follower_at(code, index)


def is_ref_qualifier_at(code: str, symbol_index: int) -> bool:
    index = skip_ws_backward(code, symbol_index - 1)
    while index >= 0 and is_ident(code[index]):
        keyword, next_index = read_identifier_backward(code, index)
        if keyword not in CV_QUALIFIERS:
            return False
        index = skip_ws_backward(code, next_index)

    if index < 0 or code[index] != ")":
        return False

    open_paren = find_matching_open_paren(code, index)
    if open_paren is None:
        return False

    before_open = skip_ws_backward(code, open_paren - 1)
    if before_open < 0:
        return False
    if is_ident(code[before_open]):
        keyword, _ = read_identifier_backward(code, before_open)
        if keyword in CONTROL_KEYWORDS_BEFORE_PAREN:
            return False
    elif code[before_open] not in ">])":
        return False

    return is_ref_qualifier_follower_at(code, symbol_index + 2)


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


def rewrite_code(code: str, lookahead: str = "") -> str:
    out: list[str] = []
    analysis_code = code + lookahead
    i = 0
    n = len(code)

    while i < n:
        if analysis_code.startswith("&&", i):
            if is_operator_symbol_at(analysis_code, i):
                out.append("&&")
                i += 2
                continue
            if is_ref_qualifier_at(analysis_code, i):
                out.append("&&")
                i += 2
                continue
            prev_ch = code[i - 1] if i > 0 else ""
            next_ch = analysis_code[i + 2] if i + 2 < len(analysis_code) else ""
            if prev_ch and next_ch and is_ws(prev_ch) and is_ws(next_ch):
                out.append("and")
                i += 2
                continue

        if analysis_code.startswith("||", i):
            if is_operator_symbol_at(analysis_code, i):
                out.append("||")
                i += 2
                continue
            prev_ch = code[i - 1] if i > 0 else ""
            next_ch = analysis_code[i + 2] if i + 2 < len(analysis_code) else ""
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


def lookahead_for(chunks: Sequence[Chunk], start: int) -> str:
    parts: list[str] = []
    for kind, text in chunks[start:]:
        match kind:
            case "opaque":
                parts.append(" ")
            case "code":
                parts.append(text)
                break
    return "".join(parts)


def rewrite_text(src: str) -> str:
    parts: list[str] = []
    chunks = list(split_code_and_opaque(src))
    for index, (kind, text) in enumerate(chunks):
        match kind:
            case "opaque":
                parts.append(text)
            case "code":
                parts.append(rewrite_code(text, lookahead_for(chunks, index + 1)))
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


def test_leaves_rvalue_ref_qualifiers_unchanged() -> None:
    source = "\n".join(
        [
            "auto unwrap() && -> T;",
            "auto unwrap() const && -> T;",
            "auto unwrap() const volatile && -> T;",
            "auto unwrap() && noexcept -> T;",
            "auto unwrap() && noexcept(false) -> T;",
            "auto unwrap() && noexcept(false);",
            "auto unwrap() && requires movable<T>;",
            "auto unwrap() && /* comment */ -> T;",
            "void emit() &&;",
            "void emit() && {}",
            "void emit() && = delete;",
            "void emit() && override;",
            "void emit() && final;",
            "",
        ]
    )

    assert rewrite_text(source) == source


def test_still_rewrites_logical_and_near_parentheses() -> None:
    source = "\n".join(
        [
            "if ((foo) && bar) {}",
            "if (foo() && noexcept(bar)) {}",
            "if (foo() && /* comment */ noexcept(bar)) {}",
            "if (requires { foo(); } && bar) {}",
            "requires(is_relational(Op) && not(foo) && requires { bar; })",
            "",
        ]
    )

    assert rewrite_text(source) == "\n".join(
        [
            "if ((foo) and bar) {}",
            "if (foo() and noexcept(bar)) {}",
            "if (foo() and /* comment */ noexcept(bar)) {}",
            "if (requires { foo(); } and bar) {}",
            "requires(is_relational(Op) and not(foo) and requires { bar; })",
            "",
        ]
    )


def test_stdin_cannot_be_combined_with_in_place(capsys) -> None:
    assert main(["--stdin", "--in-place"]) == 2
    assert "cannot be combined with --in-place" in capsys.readouterr().err


if __name__ == "__main__":
    raise SystemExit(main())
