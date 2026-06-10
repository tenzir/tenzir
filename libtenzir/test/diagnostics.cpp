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

/// The main source, registered with id `0`:
/// ```
/// from {}
/// outer
/// ```
constexpr auto main_text = std::string_view{"from {}\nouter\n"};

/// The location of `outer` in the main source (line 2, column 1).
constexpr auto outer_call = location{8, 13, 0, 0};

/// The body of the `outer` operator, registered with id `1`:
/// ```
/// inner
/// ```
constexpr auto outer_body_text = std::string_view{"inner\n"};

/// The body of the `inner` operator, registered with id `2`:
/// ```
/// assert this == 42
/// ```
constexpr auto inner_body_text = std::string_view{"assert this == 42\n"};

auto make_source_map() -> SourceMap {
  auto map = SourceMap{};
  map.add_source(SourceMap::Source{
    .index = 0,
    .text = std::string{main_text},
    .origin = "<input>",
  });
  map.add_source(SourceMap::Source{
    .index = 1,
    .text = std::string{outer_body_text},
    .origin = "<outer>",
  });
  map.add_source(SourceMap::Source{
    .index = 2,
    .text = std::string{inner_body_text},
    .origin = "<inner>",
  });
  return map;
}

auto print(const SourceMap& map, diagnostic diag) -> std::string {
  auto stream = std::stringstream{};
  auto printer = make_diagnostic_printer(map, color_diagnostics::no, stream);
  printer->emit(std::move(diag));
  return std::move(stream).str();
}

} // namespace

TEST("diagnostic printer - no call site means no call stack") {
  auto map = make_source_map();
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

TEST("diagnostic printer - single call site") {
  auto map = make_source_map();
  auto call_id = map.add_call_site(outer_call);
  // The diagnostic points at `assert` inside the body of `inner`, which was
  // called from `outer` in the main source.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, 2, call_id}, "here")
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
  auto map = make_source_map();
  // `outer` was called from the main source, top-level.
  auto outer_id = map.add_call_site(outer_call);
  // `inner` was called from within the body of `outer`.
  auto inner_id = map.add_call_site(location{0, 5, 1, outer_id});
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, 2, inner_id}, "here")
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
  auto map = make_source_map();
  auto call_id = map.add_call_site(outer_call);
  // Only the first annotation's chain is used; the secondary annotation has
  // no call site and must not suppress or duplicate the call stack.
  auto diag = diagnostic::error("assertion failed")
                .primary(location{0, 6, 2, call_id}, "here")
                .secondary(location{0, 4, 0, 0}, "related")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: assertion failed
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  ⋮
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

TEST("diagnostic printer - out-of-bounds call site is ignored") {
  auto map = make_source_map();
  // No call sites registered, but the location claims one.
  auto diag
    = diagnostic::error("oops").primary(location{0, 6, 2, 42}, "here").done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - call site with unknown source is skipped") {
  auto map = make_source_map();
  // The call site references a source that is not part of the map.
  auto call_id = map.add_call_site(location{0, 5, 99, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, 2, call_id}, "here")
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
  auto map = make_source_map();
  // The call site offset lies beyond the end of the main source text.
  auto call_id = map.add_call_site(location{1000, 1005, 0, 0});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, 2, call_id}, "here")
                .done();
  CHECK_EQUAL(print(map, std::move(diag)), R"(error: oops
 --> <inner>:1:1
  |
1 | assert this == 42
  | ^^^^^^ here
  |
)");
}

TEST("diagnostic printer - cyclic call sites terminate") {
  auto map = make_source_map();
  // A call site that names itself as its own parent must not loop forever.
  // The printer cuts the chain off after 101 entries.
  auto call_id = map.add_call_site(location{8, 13, 0, 1});
  REQUIRE_EQUAL(call_id, CallSiteId{1});
  auto diag = diagnostic::error("oops")
                .primary(location{0, 6, 2, call_id}, "here")
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

} // namespace tenzir
