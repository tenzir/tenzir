#define SUITE pipeline_new

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/collect.hpp"
#include "vast/detail/generator.hpp"
#include "vast/expression.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/test.hpp"

#include <vast/test/fixtures/actor_system_and_events.hpp>

#include <caf/test/dsl.hpp>

#include <utility>

namespace vast {

namespace {

struct physical_operator {
  virtual detail::generator<table_slice>
  push(detail::generator<table_slice> pull) = 0;

  virtual ~physical_operator() noexcept = default;

  // type input_schema();
  // pivot
  [[nodiscard]] virtual type output_schema() const = 0;
};

struct physical_plan {
  std::vector<std::unique_ptr<physical_operator>> operators;

  detail::generator<table_slice> push(detail::generator<table_slice> pull) {
    for (const auto& op : operators) {
      pull = op->push(std::move(pull));
      // for (const auto& slice : pull) {
      //   // op->input_schema() passt ? slice.layout()
      //   pull = op->push(std::move(pull));
      //   // map<type, std::vector<physical_operators>> --> successors
      // }
    }
    return pull;
  }
};

struct logical_operator {
public:
  [[nodiscard]] virtual caf::expected<
    std::vector<std::unique_ptr<physical_operator>>>
  instantiate(const type& schema) const = 0;

  virtual ~logical_operator() noexcept = default;
};

struct logical_plan {
  std::vector<std::unique_ptr<logical_operator>> operators_ = {};

  caf::expected<physical_plan> instantiate(type schema) {
    auto physical_operators = std::vector<std::unique_ptr<physical_operator>>{};
    for (const auto& op : operators_) {
      auto x = op->instantiate(schema);
      if (!x)
        return x.error(); // TODO: richtig geile Fehlermeldung
      if (x->empty())     // empty physical means no-op
        continue;
      schema = x->back()->output_schema();
      physical_operators.insert(physical_operators.end(),
                                std::make_move_iterator(x->begin()),
                                std::make_move_iterator(x->end()));
    }
    return physical_plan{std::move(physical_operators)};
  }
};

struct physical_where : physical_operator {
  explicit physical_where(expression tailored_expr, type input_schema)
    : tailored_expr_{std::move(tailored_expr)},
      input_schema_{std::move(input_schema)} {
  }

  detail::generator<table_slice>
  push(detail::generator<table_slice> pull) override {
    for (const auto& slice : pull) {
      auto result = filter(slice, tailored_expr_);
      if (result)
        co_yield *result;
    }
  }

  [[nodiscard]] type output_schema() const override {
    return input_schema_;
  }

  expression tailored_expr_ = {};
  type input_schema_{};
};

struct logical_where : logical_operator {
  explicit logical_where(expression expr) : expression_(std::move(expr)) {
  }

  caf::expected<std::vector<std::unique_ptr<physical_operator>>>
  instantiate(const type& schema) const override {
    auto tailored = tailor(expression_, schema);
    if (!tailored)
      return tailored.error();
    auto vs = std::vector<std::unique_ptr<physical_operator>>{};
    vs.emplace_back(
      std::make_unique<physical_where>(std::move(*tailored), schema));
    return vs;
  }

  expression expression_;
};

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
  }
};

detail::generator<table_slice> slices(std::vector<table_slice> table_slices) {
  for (auto& slice : table_slices)
    co_yield std::move(slice);
}

} // namespace

FIXTURE_SCOPE(pipeline_new, fixture)

TEST(physical_where) {
  auto expr = unbox(to<expression>("event_type == \"n1\""));
  auto tailored = unbox(tailor(expr, suricata_dns_log[0].layout()));
  auto physical_w = physical_where{tailored, suricata_dns_log[0].layout()};
  auto results = detail::collect(physical_w.push(slices(suricata_dns_log)));
  CHECK(results.empty());
}

TEST(logical_where) {
  auto expr = unbox(to<expression>("event_type == \"n1\""));
  auto logical_w = logical_where{expr};
  auto physical_ws = unbox(logical_w.instantiate(suricata_dns_log[0].layout()));
  REQUIRE_EQUAL(physical_ws.size(), 1u);
  auto results
    = detail::collect(physical_ws[0]->push(slices(suricata_dns_log)));
  CHECK(results.empty());
}

TEST(physical_plan) {
  auto expr = unbox(to<expression>("event_type == \"n1\""));
  auto tailored = unbox(tailor(expr, suricata_dns_log[0].layout()));
  auto vs = std::vector<std::unique_ptr<physical_operator>>{};
  vs.emplace_back(
    std::make_unique<physical_where>(tailored, suricata_dns_log[0].layout()));
  vs.emplace_back(
    std::make_unique<physical_where>(tailored, suricata_dns_log[0].layout()));
  auto plan = physical_plan{std::move(vs)};
  auto results = detail::collect(plan.push(slices(suricata_dns_log)));
  CHECK(results.empty());
}

TEST(logical_plan) {
  auto expr = unbox(to<expression>("event_type == \"n1\""));
  auto logical_w = logical_where{expr};
  auto physical_ws = unbox(logical_w.instantiate(suricata_dns_log[0].layout()));
  auto vs = std::vector<std::unique_ptr<logical_operator>>{};
  vs.emplace_back(std::make_unique<logical_where>(expr));
  vs.emplace_back(std::make_unique<logical_where>(expr));
  auto logical_plan_ = logical_plan{std::move(vs)};
  auto physical_plan
    = unbox(logical_plan_.instantiate(suricata_dns_log[0].layout()));
  auto results = detail::collect(physical_plan.push(slices(suricata_dns_log)));
  //       ----- X--\
  // -----/----- Y--=
  CHECK(results.empty());
}

FIXTURE_SCOPE_END()

} // namespace vast
