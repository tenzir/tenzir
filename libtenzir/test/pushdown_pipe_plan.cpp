//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/pipe_plan.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;
using namespace std::literals;

namespace {

using Path = std::vector<std::string>;

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto at(std::string_view source) -> tenzir::time {
  auto expr = parse(source);
  auto const* constant = try_as<ast::constant>(expr);
  REQUIRE(constant);
  return as<tenzir::time>(constant->value);
}

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

auto select(std::vector<std::pair<Path, Path>> fields) -> ir::QueryStage {
  auto result = ir::QueryStage::Select{};
  for (auto& [output, input] : fields) {
    result.fields.push_back({.output = output, .input = input});
  }
  return {std::move(result)};
}

auto drop(std::vector<Path> fields) -> ir::QueryStage {
  return {ir::QueryStage::Drop{std::move(fields)}};
}

auto summarize(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto key(Path field) -> ir::AggregateColumn {
  return {.output = field, .function = None{}, .input = field};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

/// A dialect that spells every stage plainly. With `nested`, a nested field
/// binds to a column with a dotted name, which a command cannot name.
class TestDialect final : public PipeQueryDialect {
public:
  explicit TestDialect(bool nested = false) : nested_{nested} {
  }

  auto render_null() const -> Option<std::string> override {
    return "null";
  }

  auto render_bool(bool value) const -> std::string override {
    return value ? "true" : "false";
  }

  auto render_int(int64_t value) const -> Option<std::string> override {
    return fmt::to_string(value);
  }

  auto render_uint(uint64_t value) const -> Option<std::string> override {
    return fmt::to_string(value);
  }

  auto render_double(double value) const -> std::string override {
    return fmt::to_string(value);
  }

  auto quote_string(std::string_view text) const
    -> Option<std::string> override {
    return fmt::format("\"{}\"", text);
  }

  auto render_time(tenzir::time value) const -> Option<std::string> override {
    return fmt::to_string(value.time_since_epoch().count());
  }

  auto binary_operator(ast::binary_op op) const
    -> Option<std::string_view> override {
    using enum ast::binary_op;
    switch (op) {
      case eq:
        return "==";
      case neq:
        return "!=";
      case lt:
        return "<";
      case leq:
        return "<=";
      case gt:
        return ">";
      case geq:
        return ">=";
      case and_:
        return "and";
      case or_:
        return "or";
      default:
        return None{};
    }
  }

  auto render_not(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("!{}", operand);
  }

  auto render_negate(std::string_view operand) const
    -> Option<std::string> override {
    return fmt::format("-{}", operand);
  }

  auto render_null_check(std::string_view operand, bool negated) const
    -> Option<std::string> override {
    return fmt::format("{}({})", negated ? "present" : "missing", operand);
  }

  auto render_in(std::string_view operand,
                 std::span<std::string const> elements) const
    -> Option<std::string> override {
    return fmt::format("{} in ({})", operand, fmt::join(elements, ", "));
  }

  auto boolean_values() const -> bool override {
    return false;
  }

  auto bind_path(std::span<std::string const> path) const
    -> Option<std::string> override {
    if (not nested_) {
      return PipeQueryDialect::bind_path(path);
    }
    return fmt::to_string(fmt::join(path, "."));
  }

  auto bind_column(std::string_view column) const
    -> Option<std::string> override {
    return std::string{column};
  }

  auto name_column(std::string_view column) const
    -> Option<std::string> override {
    if (column.contains('.')) {
      return None{};
    }
    return std::string{column};
  }

  auto generated_name(std::string_view kind, size_t index) const
    -> std::string override {
    return fmt::format("_{}{}", kind, index);
  }

  auto filter_stage(std::string_view predicate) const
    -> Option<std::string> override {
    return fmt::format(" | where {}", predicate);
  }

  auto select_stage(std::span<PipeAlias const> aliases,
                    std::span<std::string const> columns) const
    -> Option<std::string> override {
    return fmt::format(" | {}select {}", assign(aliases),
                       fmt::join(columns, ", "));
  }

  auto drop_stage(std::span<std::string const> columns) const
    -> Option<std::string> override {
    return fmt::format(" | drop {}", fmt::join(columns, ", "));
  }

  auto render_aggregate(std::string_view function,
                        Option<std::string_view> column) const
    -> Option<std::string> override {
    if (function != "count" and function != "min" and function != "max") {
      return None{};
    }
    return fmt::format("{}({})", function, column ? *column : "");
  }

  auto aggregate_stage(std::span<PipeAlias const> aliases,
                       std::span<PipeFunction const> functions,
                       std::span<std::string const> keys) const
    -> Option<std::string> override {
    auto calls = std::vector<std::string>{};
    for (auto const& function : functions) {
      calls.push_back(fmt::format("{}={}", function.name, function.call));
    }
    return fmt::format(" | {}stats {} by {}", assign(aliases),
                       fmt::join(calls, ", "), fmt::join(keys, ", "));
  }

  auto
  sort_stage(std::span<PipeSortKey const> keys, Option<uint64_t> bound) const
    -> Option<std::string> override {
    TENZIR_UNUSED(keys, bound);
    return None{};
  }

  auto limit_stage(uint64_t count) const -> Option<std::string> override {
    return fmt::format(" | head {}", count);
  }

private:
  static auto assign(std::span<PipeAlias const> aliases) -> std::string {
    auto result = std::string{};
    for (auto const& alias : aliases) {
      result += fmt::format("let {}={} | ", alias.name, alias.input);
    }
    return result;
  }

  bool nested_;
};

/// The options of a base query whose events hold the target's own time in
/// `ts`.
auto timed(bool native = true, bool narrow = true) -> PipeOptions {
  return {
    .extend_base = false,
    .time_column = std::string{"ts"},
    .native_time = native,
    .narrow = narrow,
  };
}

auto plan(std::vector<ir::QueryStage> stages, PipeOptions options = timed(),
          bool nested = false) -> Option<PipeQuery> {
  return render_pipe(stages, TestDialect{nested}, options);
}

auto stages(std::vector<ir::QueryStage> stages, PipeOptions options = timed(),
            bool nested = false) -> std::string {
  auto query = plan(std::move(stages), std::move(options), nested);
  return query ? query->stages : std::string{};
}

auto times(std::vector<ir::QueryStage> stages) -> std::vector<std::string> {
  auto query = plan(std::move(stages));
  REQUIRE(query);
  return query->times;
}

auto window(std::vector<ir::QueryStage> stages, PipeOptions options = timed())
  -> TimeWindow {
  auto query = plan(std::move(stages), std::move(options));
  REQUIRE(query);
  return query->window;
}

} // namespace

TEST("pipe plans bind nested fields through the dialect") {
  // By default, a nested field binds to no column.
  CHECK_EQUAL(stages({filter("a.b == 1")}, PipeOptions{}), "");
  CHECK_EQUAL(stages({filter("a.b == 1")}, PipeOptions{}, true),
              " | where (a.b == 1)");
  // A command cannot name a dotted column, so a selection aliases it, and
  // stages after it see the alias.
  auto query
    = plan({select({{{"x"}, {"a", "b"}}, {{"y"}, {"c"}}}), filter("x == 1")},
           PipeOptions{}, true);
  REQUIRE(query);
  CHECK_EQUAL(query->stages,
              " | let _column0=a.b | select _column0, c | where (_column0 == "
              "1)");
  REQUIRE(query->layout.fields);
  CHECK_EQUAL(query->layout.fields->size(), size_t{2});
  CHECK_EQUAL(query->layout.fields->front().second, "_column0");
  // Aggregations bind their inputs against the fields before them.
  CHECK_EQUAL(stages({summarize({key({"a", "b"}),
                                 {.output = {"n"},
                                  .function = "count",
                                  .input = Path{"c", "d"}}})},
                     PipeOptions{}, true),
              " | let _column0=a.b | let _column1=c.d | stats "
              "_column2=count(_column1) by _column0");
}

TEST("pipe plans track which columns hold the target's own time") {
  CHECK_EQUAL(times({filter("x > 1")}), std::vector{"ts"s});
  // Through chains of aliases and duplicates.
  CHECK_EQUAL(times({select({{{"t"}, {"ts"}}, {{"u"}, {"ts"}}}),
                     select({{{"v"}, {"u"}}})}),
              std::vector{"ts"s});
  // Dropping one alias keeps the column of the other.
  CHECK_EQUAL(
    times({select({{{"t"}, {"ts"}}, {{"u"}, {"ts"}}}), drop({{"t"}})}),
    std::vector{"ts"s});
  CHECK_EQUAL(times({select({{{"t"}, {"ts"}}, {{"x"}, {"x"}}}), drop({{"t"}})}),
              std::vector<std::string>{});
  // A field reassigned to `ts` holds another column.
  CHECK_EQUAL(times({select({{{"ts"}, {"x"}}})}), std::vector<std::string>{});
  CHECK_EQUAL(times({drop({{"ts"}})}), std::vector<std::string>{});
  // Group keys keep the time, while counts and extremes do not.
  CHECK_EQUAL(
    times({summarize(
      {key({"ts"}), {.output = {"n"}, .function = "count", .input = None{}}})}),
    std::vector{"ts"s});
  CHECK_EQUAL(times({summarize(
                {{.output = {"m"}, .function = "min", .input = Path{"ts"}}})}),
              std::vector<std::string>{});
  // A base query that may redefine its time holds none of the target's own.
  auto query = plan({filter("x > 1")}, timed(false));
  REQUIRE(query);
  CHECK(query->times.empty());
}

TEST("pipe plans compare time literals only with the target's own time") {
  CHECK_EQUAL(stages({filter("ts > 1970-01-01T00:00:01Z")}),
              " | where (ts > 1000000000)");
  CHECK_EQUAL(stages({filter("x > 1970-01-01T00:00:01Z")}), "");
  CHECK_EQUAL(stages({filter("ts - 1970-01-01T00:00:01Z > 1")}), "");
  // A base query that may redefine its time compares none.
  CHECK_EQUAL(stages({filter("ts > 1970-01-01T00:00:01Z")}, timed(false)), "");
  CHECK_EQUAL(stages({filter("x > 1")}, timed(false)), " | where (x > 1)");
  // An identity keeps the time, but an alias is another field.
  CHECK(not stages({select({{{"ts"}, {"ts"}}}), filter("ts > "
                                                       "1970-01-01T00:00:01Z")})
              .empty());
  CHECK_EQUAL(
    stages({select({{{"t"}, {"ts"}}}), filter("t > 1970-01-01T00:00:01Z")}),
    "");
  CHECK_EQUAL(
    stages({select({{{"ts"}, {"x"}}}), filter("ts > 1970-01-01T00:00:01Z")}),
    "");
  // Without a time column, the dialect decides.
  CHECK_EQUAL(stages({filter("x > 1970-01-01T00:00:01Z")}, PipeOptions{}),
              " | where (x > 1000000000)");
}

TEST("pipe plans narrow the window of the leading filters") {
  auto const t1 = at("2025-01-01T00:01:00Z");
  auto const t2 = at("2025-01-01T00:02:00Z");
  auto const t3 = at("2025-01-01T00:03:00Z");
  auto both = window({filter("ts >= 2025-01-01T00:01:00Z and x > 1"),
                      filter("2025-01-01T00:03:00Z > ts")});
  CHECK(both.begin == Option<tenzir::time>{t1});
  CHECK(both.end == Option<tenzir::time>{t3});
  // A disjunction encloses its operands, if they all bound a side.
  auto enclosed = window({filter("(ts >= 2025-01-01T00:02:00Z and ts < "
                                 "2025-01-01T00:03:00Z) or ts == "
                                 "2025-01-01T00:01:00Z")});
  CHECK(enclosed.begin == Option<tenzir::time>{t1});
  CHECK(enclosed.end == Option<tenzir::time>{t3});
  auto open = window({filter("ts >= 2025-01-01T00:02:00Z or x > 1")});
  CHECK(not open.begin);
  CHECK(not open.end);
  auto half = window(
    {filter("ts >= 2025-01-01T00:02:00Z or ts > 2025-01-01T00:01:00Z")});
  CHECK(half.begin == Option<tenzir::time>{t1});
  CHECK(not half.end);
  // Only the filters before a limit, and only with `narrow`.
  auto limited = window({limit(3), filter("ts >= 2025-01-01T00:02:00Z")});
  CHECK(not limited.begin);
  auto unnarrowed
    = window({filter("ts >= 2025-01-01T00:02:00Z")}, timed(true, false));
  CHECK(not unnarrowed.begin);
  auto selected = window(
    {select({{{"ts"}, {"ts"}}}), filter("ts == 2025-01-01T00:02:00Z")});
  CHECK(selected.begin == Option<tenzir::time>{t2});
  CHECK(selected.end == Option<tenzir::time>{t2});
}
