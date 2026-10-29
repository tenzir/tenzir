//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/pipe.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;

namespace {

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

auto filters(std::vector<std::string_view> sources) -> ir::OptimizeFilter {
  auto result = ir::OptimizeFilter{};
  for (auto source : sources) {
    result.push_back(parse(source));
  }
  return result;
}

auto paths(std::vector<std::string_view> sources) -> ir::OptimizeProjection {
  auto result = ir::OptimizeProjection{};
  for (auto source : sources) {
    auto path = ast::field_path::try_from(parse(source));
    REQUIRE(path);
    result.push_back(std::move(*path));
  }
  return result;
}

/// Spells an equality of a column with a literal as the column's name, and
/// vetoes every other atom.
class TestRenderer final : public PrefilterRenderer {
protected:
  auto render_atom(Expr const& expr) const -> Option<std::string> override {
    auto const* binary = try_as<Binary>(expr);
    if (not binary or binary->op != BinaryOp::eq) {
      return None{};
    }
    auto const* column = try_as<Column>(*binary->left);
    if (not column or not is<Literal>(*binary->right)) {
      return None{};
    }
    return fmt::format("{}", fmt::join(column->path, "."));
  }

  auto render_junction(Junction junction,
                       std::span<std::string const> operands) const
    -> std::string override {
    auto keyword = junction == Junction::conjunction ? " & " : " / ";
    return fmt::format("({})", fmt::join(operands, keyword));
  }
};

/// A dialect that rejects queries ending in `!`, cannot spell the field `bad`,
/// and spells limits up to 100.
class TestDialect final : public PipeDialect {
public:
  explicit TestDialect(size_t budget = 1000) : budget_{budget} {
  }

  auto accepts_trailing_stages(std::string_view query) const -> bool override {
    return not query.ends_with('!');
  }

  auto renderer() const -> Renderer const& override {
    return renderer_;
  }

  auto filter_stage(std::span<std::string const> predicates) const
    -> std::string override {
    return fmt::format(" | where {}", fmt::join(predicates, " & "));
  }

  auto project_stage(std::span<std::string const> names) const
    -> Option<std::string> override {
    if (std::ranges::contains(names, "bad")) {
      return None{};
    }
    return fmt::format(" | keep {}", fmt::join(names, ","));
  }

  auto limit_stage(uint64_t n) const -> Option<std::string> override {
    if (n > 100) {
      return None{};
    }
    return fmt::format(" | first {}", n);
  }

  auto max_appendix() const -> size_t override {
    return budget_;
  }

private:
  TestRenderer renderer_;
  size_t budget_;
};

auto plan(std::string_view query, std::vector<std::string_view> filter,
          Option<ir::OptimizeProjection> projection, Option<uint64_t> limit,
          Option<uint64_t> cap = None{},
          TestDialect const& dialect = TestDialect{}) -> PipePlan {
  return plan_trailing_stages(dialect, query, filters(std::move(filter)),
                              projection, limit, cap, test::builtin_registry());
}

} // namespace

TEST("trailing stages filter, project, and limit in order") {
  auto filtered = plan("q", {"a == 1", "b.c == 2"}, paths({"d"}), 5);
  // The projection retains what the local filter reads. The limit stays local,
  // since every predicate does.
  CHECK_EQUAL(filtered.query, "q | where a & b.c | keep d,a,b");
  CHECK(filtered.filtered);
  CHECK(filtered.projected);
  CHECK(not filtered.limited);
  auto limited = plan("q", {}, paths({"d.e", "d"}), 5);
  CHECK_EQUAL(limited.query, "q | keep d | first 5");
  CHECK(not limited.filtered);
  CHECK(limited.projected);
  CHECK(limited.limited);
}

TEST("trailing stages skip what the dialect cannot spell") {
  CHECK_EQUAL(plan("q", {}, None{}, 101).query, "q");
  CHECK_EQUAL(plan("q", {}, None{}, 0).query, "q");
  CHECK_EQUAL(plan("q", {}, paths({"bad"}), None{}).query, "q");
  CHECK_EQUAL(plan("q", {}, paths({"this"}), None{}).query, "q");
  // A predicate without a prefilter stays local and suppresses the limit.
  auto local = plan("q", {"a > 1"}, None{}, 5);
  CHECK_EQUAL(local.query, "q");
  CHECK(not local.filtered);
  CHECK(not local.limited);
  // A rejected query stays unchanged.
  CHECK_EQUAL(plan("q!", {"a == 1"}, paths({"a"}), None{}).query, "q!");
}

TEST("trailing stages cap the query before filtering") {
  CHECK_EQUAL(plan("q", {"a == 1"}, None{}, None{}, 10).query,
              "q | first 10 | where a");
  // Without a filter stage, the cap is the service's business.
  CHECK_EQUAL(plan("q", {}, None{}, 5, 10).query, "q | first 5");
  // A cap that the dialect cannot spell keeps the filter local entirely.
  auto uncapped = plan("q", {"a == 1"}, paths({"d"}), None{}, 101);
  CHECK_EQUAL(uncapped.query, "q | keep d,a");
  CHECK(not uncapped.filtered);
}

TEST("trailing stages keep the filter and projection within the budget") {
  // `| where b` fits, `| where long_name` does not. A predicate that does not
  // fit leaves room for later ones.
  auto dialect = TestDialect{std::string_view{" | where b"}.size()};
  auto result
    = plan("q", {"long_name == 1", "b == 2"}, None{}, None{}, None{}, dialect);
  CHECK_EQUAL(result.query, "q | where b");
  // The projection must fit into what the filter left over.
  auto projected = plan("q", {"b == 2"}, paths({"c"}), None{}, None{}, dialect);
  CHECK_EQUAL(projected.query, "q | where b");
  CHECK(not projected.projected);
  // Limit stages have a bounded size and do not count against the budget.
  auto limited = plan("q", {}, None{}, 5, None{}, TestDialect{0});
  CHECK_EQUAL(limited.query, "q | first 5");
}

TEST("predicates fit a budget in order") {
  auto predicates = std::vector<std::string>{"aaaa", "bbbbbbbb", "cc", "d"};
  auto size = [](std::span<std::string const> selection) {
    auto result = size_t{0};
    for (auto const& x : selection) {
      result += x.size();
    }
    return result;
  };
  CHECK_EQUAL(select_within_budget(predicates, 7, size),
              (std::vector<std::string>{"aaaa", "cc", "d"}));
  CHECK(select_within_budget(predicates, 0, size).empty());
}

TEST("limits are pushable only without a filter") {
  auto empty = ir::OptimizeFilter{};
  CHECK_EQUAL(pushable_limit(empty, uint64_t{5}), Option<uint64_t>{5});
  CHECK(not pushable_limit(empty, None{}));
  CHECK(not pushable_limit(empty, uint64_t{0}));
  CHECK(not pushable_limit(empty, uint64_t{6}, 5));
  CHECK(not pushable_limit(filters({"a == 1"}), uint64_t{5}));
}
