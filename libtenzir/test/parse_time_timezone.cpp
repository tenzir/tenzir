//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/view3.hpp"

#include <cstdlib>
#include <ctime>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

struct Row {
  std::string_view input;
  Option<std::string_view> zone;
};

// Builds a slice with the string columns `s` and `tz`.
auto make_slice(std::vector<Row> const& rows) -> table_slice {
  auto builder = series_builder{};
  for (auto const& row : rows) {
    auto r = builder.record();
    r.field("s").data(row.input);
    if (row.zone) {
      r.field("tz").data(*row.zone);
    } else {
      r.field("tz").null();
    }
  }
  return builder.finish_assert_one_slice();
}

struct Result {
  std::vector<data> values;
  std::vector<std::string> warnings;
};

// Evaluates `src` with the legacy evaluator that operators without a Nova
// implementation still use.
auto eval_expr(std::string_view src, table_slice const& slice) -> Result {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(src, location::unknown, ctx);
  REQUIRE(expr);
  auto resolved = resolve_entities(*expr, ExternPrelude::empty(), ctx);
  REQUIRE(resolved);
  auto evaluated = eval(*expr, slice, base_ctx{dh, test::builtin_registry()});
  auto result = Result{};
  for (auto value : evaluated.values()) {
    result.values.push_back(materialize(value));
  }
  for (auto& diag : std::move(dh).collect()) {
    result.warnings.push_back(std::move(diag.message));
  }
  return result;
}

auto at(std::string_view iso) -> data {
  auto result = data{};
  REQUIRE(parsers::data(iso, result));
  return result;
}

} // namespace

TEST("legacy parse_time interprets wall-clock time per event zone") {
  auto slice = make_slice({
    {"Nov  6 09:13:01", "Europe/Zurich"},
    {"Jul  4 09:13:01", "Europe/Zurich"},
    {"Nov  6 09:13:01", "America/Los_Angeles"},
    {"Nov  6 09:13:01", "UTC"},
    {"Nov  6 09:13:01", None{}},
    {"Nov  6 09:13:01", "Mars/Olympus_Mons"},
  });
  auto result = eval_expr("s.parse_time(\"%b %e %H:%M:%S\", "
                          "reference=2025-12-01T00:00:00Z, timezone=tz)",
                          slice);
  REQUIRE_EQUAL(result.values.size(), size_t{6});
  CHECK_EQUAL(result.values[0], at("2025-11-06T08:13:01Z"));
  CHECK_EQUAL(result.values[1], at("2025-07-04T07:13:01Z"));
  CHECK_EQUAL(result.values[2], at("2025-11-06T17:13:01Z"));
  CHECK_EQUAL(result.values[3], at("2025-11-06T09:13:01Z"));
  CHECK_EQUAL(result.values[4], at("2025-11-06T09:13:01Z"));
  CHECK_EQUAL(result.values[5], data{});
  REQUIRE_EQUAL(result.warnings.size(), size_t{1});
  CHECK_EQUAL(result.warnings[0],
              "`parse_time` got unknown time zone `Mars/Olympus_Mons`");
}

TEST("legacy parse_time resolves DST gaps and overlaps") {
  auto slice = make_slice({
    {"2025-03-30 02:30:00", "Europe/Zurich"},
    {"2025-10-26 02:30:00", "Europe/Zurich"},
  });
  auto result
    = eval_expr("s.parse_time(\"%Y-%m-%d %H:%M:%S\", timezone=tz)", slice);
  REQUIRE_EQUAL(result.values.size(), size_t{2});
  CHECK_EQUAL(result.values[0], at("2025-03-30T01:30:00Z"));
  CHECK_EQUAL(result.values[1], at("2025-10-26T00:30:00Z"));
  CHECK(result.warnings.empty());
}

TEST("legacy parse_time prefers an explicit UTC offset") {
  auto slice = make_slice({{"2025-07-04 12:00:00 +0400", "Europe/Zurich"}});
  auto result
    = eval_expr("s.parse_time(\"%Y-%m-%d %H:%M:%S %z\", timezone=tz)", slice);
  REQUIRE_EQUAL(result.values.size(), size_t{1});
  CHECK_EQUAL(result.values[0], at("2025-07-04T08:00:00Z"));
  REQUIRE_EQUAL(result.warnings.size(), size_t{1});
  CHECK_EQUAL(result.warnings[0], "`parse_time` ignores `timezone` because the "
                                  "format includes a time zone");
}

TEST("legacy parse_time does not treat %Z as a resolved zone") {
  // `strptime` consumes the name without interpreting it, so `timezone`
  // still determines the instant. Some C libraries, such as macOS's, also
  // shift the parsed fields into the host zone for `GMT`, so pin that to UTC.
  auto const* previous = std::getenv("TZ");
  auto restore = Option<std::string>{};
  if (previous) {
    restore = std::string{previous};
  }
  ::setenv("TZ", "UTC", 1);
  ::tzset();
  auto slice = make_slice({{"2025-07-04 12:00:00 GMT", "Europe/Zurich"}});
  auto result
    = eval_expr("s.parse_time(\"%Y-%m-%d %H:%M:%S %Z\", timezone=tz)", slice);
  if (restore) {
    ::setenv("TZ", restore->c_str(), 1);
  } else {
    ::unsetenv("TZ");
  }
  ::tzset();
  REQUIRE_EQUAL(result.values.size(), size_t{1});
  CHECK_EQUAL(result.values[0], at("2025-07-04T10:00:00Z"));
  CHECK(result.warnings.empty());
}
