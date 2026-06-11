//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"

#include "tenzir/source.hpp"
#include "tenzir/test/test.hpp"

#include <sstream>
#include <string>

namespace tenzir {

namespace {

/// The location of `outer` in the main source (line 2, column 1).
constexpr auto outer_call = location{8, 13, 0, 0};

struct fixture {
  SourceMap map;
  /// The id of the body of the `outer` operator.
  SourceId outer_id;
  /// The id of the body of the `inner` operator.
  SourceId inner_id;
};

/// Creates a source map modeling this scenario:
/// - The main source (id `0`) calls `outer` on line 2.
/// - The body of `outer` calls `inner`.
/// - The body of `inner` contains a failing `assert`.
///
/// The sources mix eager and lazy line splitting to cover both paths in the
/// printer.
auto make_fixture() -> fixture {
  auto map = SourceMap{};
  map.add_primary_source(
    Source::new_source("from {}\nouter\n", "<input>", true));
  auto outer = Source::new_source("inner\n", "<outer>", false);
  auto inner = Source::new_source("assert this == 42\n", "<inner>", true);
  auto outer_id = outer->index;
  auto inner_id = inner->index;
  map.add_source(std::move(outer));
  map.add_source(std::move(inner));
  return fixture{std::move(map), outer_id, inner_id};
}

auto print(const SourceMap& map, diagnostic diag) -> std::string {
  auto stream = std::stringstream{};
  auto printer = make_diagnostic_printer(map, color_diagnostics::no, stream);
  printer->emit(std::move(diag));
  return std::move(stream).str();
}

} // namespace

TEST("diagnostic printer - no call site means no call stack") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto diag
    = diagnostic::error("oops").primary(location{0, 4, 0, 0}, "here").done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <input>:1:1
  |
1 | from {}
  | ^^^^ here
  |
)");
}

TEST("diagnostic printer - warning uses warning underline") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto diag = diagnostic::warning("careful")
                .primary(location{0, 4, 0, 0}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(warning: careful
 --> <input>:1:1
  |
1 | from {}
  | ~~~~ here
  |
)");
}

TEST("diagnostic printer - single call site") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call);
  // The diagnostic points at `assert` inside the body of `inner`, which was
  // called from `outer` in the main source.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)");
}

TEST("diagnostic printer - nested call sites print innermost first") {
  auto [map, outer_src, inner_src] = make_fixture();
  // `outer` was called from the main source, top-level.
  auto outer_call_id = map.add_call_site(outer_call);
  // `inner` was called from within the body of `outer`.
  auto inner_call_id
    = map.add_call_site(location{0, 5, outer_src, outer_call_id});
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, inner_call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <outer>:1:1
  |
1 | inner
  | ----- called from here
  |
 --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)");
}

TEST("diagnostic printer - call stack follows the primary annotation") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call);
  // Only the primary annotation's chain is used; the secondary annotation has
  // no call site and must not suppress or duplicate the call stack.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .secondary(location{0, 4, 0, 0}, "related")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <input>:1:1
  |
1 | from {}
  | ---- related
  |
 --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)");
}

TEST("diagnostic printer - call stack follows first primary annotation") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call);
  auto diag = diagnostic::error("assertion failed")
                .secondary(location{0, 4, 0, 0}, "related")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <input>:1:1
  |
1 | from {}
  | ---- related
  |
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)");
}

TEST("diagnostic printer - secondary annotation call site is ignored") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call);
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 4, 0, 0}, "here")
                .secondary(location{0, 6, inner_src, call_id}, "related")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <input>:1:1
  |
1 | from {}
  | ^^^^ here
  |
 --> <inner>:1:1
  |
1 | assert this == 42
  | ------ related
  |
)");
}

TEST("diagnostic printer - out-of-bounds call site is ignored") {
  auto [map, outer_src, inner_src] = make_fixture();
  // No call sites registered, but the location claims one.
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, 42}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - call site with unknown source is skipped") {
  auto [map, outer_src, inner_src] = make_fixture();
  // The call site references a source that is not part of the map.
  auto call_id = map.add_call_site(location{0, 5, 9999999, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - call site beyond source text is skipped") {
  auto [map, outer_src, inner_src] = make_fixture();
  // The call site offset lies beyond the end of the main source text.
  auto call_id = map.add_call_site(location{1000, 1005, 0, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - reflects changes to the source map") {
  auto map = SourceMap{};
  auto stream = std::stringstream{};
  auto printer = make_diagnostic_printer(map, color_diagnostics::no, stream);
  // Without any sources, only the message is printed.
  printer->emit(
    diagnostic::error("oops").primary(location{0, 4, 0, 0}, "here").done());
  CHECK_EQUAL(stream.str(), "error: oops\n");
  // Sources and call sites registered after the printer was created are
  // picked up by subsequent emits of the same printer.
  map.add_primary_source(
    Source::new_source("from {}\nouter\n", "<input>", false));
  auto inner = Source::new_source("assert this == 42\n", "<inner>", false);
  auto inner_src = inner->index;
  map.add_source(std::move(inner));
  auto call_id = map.add_call_site(outer_call);
  stream.str("");
  printer->emit(diagnostic::error("assertion failed")
                  .primary(location{0, 6, inner_src, call_id}, "here")
                  .done());
  // The printer separates consecutive diagnostics with a blank line.
  CHECK_EQUAL(stream.str(), R"(
error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)");
}

TEST("diagnostic printer - cyclic call sites terminate") {
  auto [map, outer_src, inner_src] = make_fixture();
  // A call site that names itself as its own parent must not loop forever.
  // The printer cuts the chain off after 101 entries.
  auto call_id = map.add_call_site(location{8, 13, 0, 1});
  REQUIRE_EQUAL(call_id, CallSiteId{1});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  auto expected = std::string{R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)"};
  for (auto i = 0; i < 101; ++i) {
    expected += R"( --> <input>:2:1
  |
2 | outer
  | ----- called from here
  |
)";
  }
  CHECK_EQUAL(print(map, std::move(diag)), expected);
}

TEST("source map - primary source resolves as source zero") {
  auto source = Source::new_source("from {}\n", "<input>", true);
  auto id = source->index;
  auto map = SourceMap{};
  map.add_primary_source(std::move(source));
  auto primary = map.primary_source();
  REQUIRE(primary);
  CHECK_EQUAL(primary->index, id);
  auto source_zero = map.source(0);
  REQUIRE(source_zero);
  CHECK_EQUAL(source_zero->index, id);
}

TEST("source map - translate normalizes primary source locations") {
  auto source = Source::new_source("from {}\n", "<input>", true);
  auto id = source->index;
  auto map = SourceMap{};
  map.add_primary_source(std::move(source));
  auto source_zero_location = location{1, 4, 0, 0};
  auto primary_location = location{1, 4, id, 0};
  auto expected = location{1, 4, 0, 0};
  CHECK_EQUAL(map.translate(source_zero_location), expected);
  CHECK_EQUAL(map.translate(primary_location), expected);
}

TEST("source map - translate follows call sites to primary source") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call);
  auto inner_location = location{0, 6, inner_src, call_id};
  CHECK_EQUAL(map.translate(inner_location), outer_call);
}

TEST("source map - translate follows nested call sites to primary source") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto outer_call_id = map.add_call_site(outer_call);
  auto inner_call_id
    = map.add_call_site(location{0, 5, outer_src, outer_call_id});
  auto inner_location = location{0, 6, inner_src, inner_call_id};
  CHECK_EQUAL(map.translate(inner_location), outer_call);
}

TEST("source map - translate clears malformed call site") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto inner_location = location{0, 6, inner_src, 42};
  auto expected = location{0, 6, inner_src, 0};
  CHECK_EQUAL(map.translate(inner_location), expected);
}

TEST("source map - translate terminates on cyclic call sites") {
  auto [map, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(location{8, 13, 0, 1});
  REQUIRE_EQUAL(call_id, CallSiteId{1});
  auto inner_location = location{0, 6, inner_src, call_id};
  auto expected = location{8, 13, 0, 0};
  CHECK_EQUAL(map.translate(inner_location), expected);
}

} // namespace tenzir
