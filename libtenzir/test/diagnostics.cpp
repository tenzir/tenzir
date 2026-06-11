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

struct fixture {
  SourceMap map;
  /// The id of the top-level source.
  SourceId input_id;
  /// The id of the body of the `outer` operator.
  SourceId outer_id;
  /// The id of the body of the `inner` operator.
  SourceId inner_id;
};

/// Creates a source map modeling this scenario:
/// - The main source calls `outer` on line 2.
/// - The body of `outer` calls `inner`.
/// - The body of `inner` contains a failing `assert`.
///
/// The sources mix eager and lazy line splitting to cover both paths in the
/// printer.
auto make_fixture() -> fixture {
  auto map = SourceMap{};
  auto input = Source::new_source("from {}\nouter\n", "<input>", true);
  auto outer = Source::new_source("inner\n", "<outer>", false);
  auto inner = Source::new_source("assert this == 42\n", "<inner>", true);
  auto input_id = input->index;
  auto outer_id = outer->index;
  auto inner_id = inner->index;
  map.add_source(std::move(input));
  map.add_source(std::move(outer));
  map.add_source(std::move(inner));
  return fixture{std::move(map), input_id, outer_id, inner_id};
}

/// The location of `outer` in the main source (line 2, column 1).
auto outer_call(SourceId input_id) -> location {
  return location{8, 13, input_id, 0};
}

auto print(const SourceMap& map, diagnostic diag) -> std::string {
  auto stream = std::stringstream{};
  auto printer = make_diagnostic_printer(map, color_diagnostics::no, stream);
  printer->emit(std::move(diag));
  return std::move(stream).str();
}

} // namespace

TEST("diagnostic printer - no call site means no call stack") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto diag = diagnostic::error("oops")
                .primary(location{0, 4, input_src, 0}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <input>:1:1
  |
1 | from {}
  | ^^^^ here
  |
)");
}

TEST("diagnostic printer - warning uses warning underline") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto diag = diagnostic::warning("careful")
                .primary(location{0, 4, input_src, 0}, "here")
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
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  // The diagnostic points at `assert` inside the body of `inner`, which was
  // called from `outer` in the main source.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))),
              R"(error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
 --> <input>:2:1
  |
2 | outer
  | ^^^^^ called from here
  |
)");
}

TEST("diagnostic printer - nested call sites print innermost first") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  // `outer` was called from the main source, top-level.
  auto outer_call_id = map.add_call_site(outer_call(input_src));
  // `inner` was called from within the body of `outer`.
  auto inner_call_id
    = map.add_call_site(location{0, 5, outer_src, outer_call_id});
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, inner_call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))),
              R"(error: assertion failed
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
  | ^^^^^ called from here
  |
)");
}

TEST("diagnostic printer - call stack follows the primary annotation") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  // Only the primary annotation's chain is used; the secondary annotation has
  // no call site and must not suppress or duplicate the call stack.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .secondary(location{0, 4, input_src, 0}, "related")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))),
              R"(error: assertion failed
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
  | ^^^^^ called from here
  |
)");
}

TEST("diagnostic printer - call stack follows first primary annotation") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  auto diag = diagnostic::error("assertion failed")
                .secondary(location{0, 4, input_src, 0}, "related")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))),
              R"(error: assertion failed
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
  | ^^^^^ called from here
  |
)");
}

TEST("diagnostic printer - secondary annotation call site is ignored") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 4, input_src, 0}, "here")
                .secondary(location{0, 6, inner_src, call_id}, "related")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))),
              R"(error: assertion failed
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
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  // No call sites registered, but the location claims one.
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, 42}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - call site with unknown source is skipped") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  // The call site references a source that is not part of the map.
  auto call_id = map.add_call_site(location{0, 5, 9999999, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - call site beyond source text is skipped") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  // The call site offset lies beyond the end of the main source text.
  auto call_id = map.add_call_site(location{1000, 1005, input_src, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, map.enrich(std::move(diag))), R"(error: oops
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
  auto input = Source::new_source("from {}\nouter\n", "<input>", false);
  auto input_src = input->index;
  map.add_source(std::move(input));
  auto inner = Source::new_source("assert this == 42\n", "<inner>", false);
  auto inner_src = inner->index;
  map.add_source(std::move(inner));
  auto call_id = map.add_call_site(outer_call(input_src));
  stream.str("");
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  printer->emit(map.enrich(std::move(diag)));
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
  | ^^^^^ called from here
  |
)");
}

TEST("diagnostic printer - only renders provided annotations") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
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

TEST("source map - enrich appends call trace") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  auto original_location = location{0, 6, inner_src, call_id};
  auto diag = diagnostic::error("oops")
                .primary(original_location, "here")
                .secondary(location{0, 4, input_src, 0}, "related")
                .done();
  auto enriched = map.enrich(std::move(diag));
  REQUIRE_EQUAL(enriched.annotations.size(), size_t{3});
  CHECK(enriched.annotations[0].primary);
  CHECK_EQUAL(enriched.annotations[0].source, original_location);
  CHECK(not enriched.annotations[1].primary);
  CHECK_EQUAL(enriched.annotations[1].text, "related");
  CHECK(enriched.annotations[2].primary);
  CHECK_EQUAL(enriched.annotations[2].text, "called from here");
  CHECK_EQUAL(enriched.annotations[2].source, outer_call(input_src));
}

TEST("source map - enrich appends nested trace inward to outward") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto outer_call_id = map.add_call_site(outer_call(input_src));
  auto inner_call_location = location{0, 5, outer_src, outer_call_id};
  auto inner_call_id = map.add_call_site(inner_call_location);
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, inner_call_id}, "here")
                .done();
  auto enriched = map.enrich(std::move(diag));
  REQUIRE_EQUAL(enriched.annotations.size(), size_t{3});
  CHECK(not enriched.annotations[1].primary);
  CHECK_EQUAL(enriched.annotations[1].source, inner_call_location);
  CHECK(enriched.annotations[2].primary);
  CHECK_EQUAL(enriched.annotations[2].source, outer_call(input_src));
}

TEST("source map - enrich ignores secondary annotation call sites") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  auto diag = diagnostic::error("oops")
                .primary(location{0, 4, input_src, 0}, "here")
                .secondary(location{0, 6, inner_src, call_id}, "related")
                .done();
  auto enriched = map.enrich(std::move(diag));
  REQUIRE_EQUAL(enriched.annotations.size(), size_t{2});
}

TEST("source map - enrich handles malformed call sites") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, 42}, "here")
                .done();
  auto enriched = map.enrich(std::move(diag));
  REQUIRE_EQUAL(enriched.annotations.size(), size_t{1});
}

TEST("source map - enrich terminates on cyclic call sites") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  // A call site that names itself as its own parent must not loop forever.
  auto cyclic_location = location{8, 13, input_src, 1};
  auto call_id = map.add_call_site(cyclic_location);
  REQUIRE_EQUAL(call_id, CallSiteId{1});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .done();
  auto enriched = map.enrich(std::move(diag));
  REQUIRE_EQUAL(enriched.annotations.size(), size_t{2});
  CHECK(not enriched.annotations[1].primary);
  CHECK_EQUAL(enriched.annotations[1].source, cyclic_location);
}

TEST("diagnostic - reset primary locations except top-level") {
  auto [map, input_src, outer_src, inner_src] = make_fixture();
  auto call_id = map.add_call_site(outer_call(input_src));
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, inner_src, call_id}, "here")
                .primary(location{0, 4, input_src, 0}, "top-level primary")
                .secondary(location{0, 5, outer_src, call_id}, "secondary")
                .done();
  diag.reset_primary_locations_except_top_callsite();
  auto top_level_primary = location{0, 4, input_src, 0};
  auto secondary = location{0, 5, outer_src, call_id};
  CHECK_EQUAL(diag.annotations[0].source, location::unknown);
  CHECK_EQUAL(diag.annotations[1].source, top_level_primary);
  CHECK_EQUAL(diag.annotations[2].source, secondary);
}

} // namespace tenzir
