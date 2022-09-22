//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE pipeline_executor

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/generator.hpp"
#include "vast/expression.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/test/dsl.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <variant>

namespace vast {

namespace {

// -- helpers -----------------------------------------------------------------

template <class List>
struct tl_unwrap;

template <class... Types>
struct tl_unwrap<caf::detail::type_list<Types...>> {
  using type = caf::detail::type_list<typename Types::type...>;
};

template <class List>
using tl_unwrap_t = typename tl_unwrap<List>::type;

// -- physical operators ------------------------------------------------------

template <class Input, class Output>
struct physical_operator final {
  using type
    = std::function<auto(detail::generator<Input>)->detail::generator<Output>>;
};

template <class Input>
struct physical_operator<Input, void> final {
  using type = std::function<auto(detail::generator<Input>)->void>;
};

template <class Output>
struct physical_operator<void, Output> final {
  using type = std::function<auto(void)->detail::generator<Output>>;
};

// -- logical operators -------------------------------------------------------

template <class Input = void, class Output = void>
struct logical_operator;

template <>
struct logical_operator<void, void> {
  virtual ~logical_operator() noexcept = default;

  using allowed_type_list = caf::detail::type_list<void, table_slice>;

  inline static constexpr std::array<
    std::string_view, caf::detail::tl_size<allowed_type_list>::value>
    allowed_type_names = {
      "Void",
      "Arrow",
    };

  // Invert list by forbidding void -> void
  using allowed_physical_operator_list
    = caf::detail::type_list<physical_operator<void, table_slice>,
                             physical_operator<table_slice, table_slice>,
                             physical_operator<table_slice, void>>;

  using allowed_physical_operator_variant
    = caf::detail::tl_apply_t<tl_unwrap_t<allowed_physical_operator_list>,
                              std::variant>;

  [[nodiscard]] virtual std::string_view name() const noexcept = 0;

  [[nodiscard]] virtual int input_type_index() const noexcept = 0;
  [[nodiscard]] virtual int output_type_index() const noexcept = 0;

  [[nodiscard]] std::string_view input_type_name() const noexcept {
    return allowed_type_names[input_type_index()];
  }

  [[nodiscard]] std::string_view output_type_name() const noexcept {
    return allowed_type_names[output_type_index()];
  }

  [[nodiscard]] virtual caf::expected<allowed_physical_operator_variant>
  make_erased(type input_schema) = 0;
};

template <class Input, class Output>
struct logical_operator : logical_operator<> {
  static_assert(caf::detail::tl_contains<allowed_type_list, Input>::value,
                "Input is not a supported input type");
  static_assert(caf::detail::tl_contains<allowed_type_list, Output>::value,
                "Output is not a supported output type");
  static_assert(
    caf::detail::tl_contains<allowed_physical_operator_list,
                             physical_operator<Input, Output>>::value,
    "physical_operator<Input, Output> is not a supported phyiscal operator");

  using result_type = typename physical_operator<Input, Output>::type;

  [[nodiscard]] virtual caf::expected<result_type> make(type input_schema) = 0;

  [[nodiscard]] int input_type_index() const noexcept final {
    return caf::detail::tl_index_of<allowed_type_list, Input>::value;
  }

  [[nodiscard]] int output_type_index() const noexcept final {
    return caf::detail::tl_index_of<allowed_type_list, Output>::value;
  }

  [[nodiscard]] caf::expected<allowed_physical_operator_variant>
  make_erased(type input_schema) final {
    if constexpr (std::is_same_v<Input, table_slice>) {
      if (!input_schema)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("pipeline operator '{}' has input "
                                           "type '{}', but got no input schema",
                                           name(), input_type_name()));
    } else {
      if (input_schema)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("pipeline operator '{}' has input "
                                           "type '{}', but unexpectedly got "
                                           "input schema '{}'",
                                           name(), input_type_name(),
                                           input_schema));
    }
    auto result = make(std::move(input_schema));
    if (!result)
      return std::move(result.error());
    return std::move(*result);
  };
};

// -- plan --------------------------------------------------------------------

struct plan {
  static caf::expected<plan>
  make(std::vector<std::unique_ptr<logical_operator<>>> operators) {
    if (operators.size() < 2)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("pipeline must have at least two "
                                         "operators, but got {}",
                                         operators.size()));
    constexpr auto void_index
      = caf::detail::tl_index_of<logical_operator<>::allowed_type_list,
                                 void>::value;
    constexpr auto void_name
      = logical_operator<>::allowed_type_names[void_index];
    auto expected_input_type_index = void_index;
    auto expected_input_type_name = void_name;
    for (const auto& operator_ : operators) {
      if (operator_->input_type_index() != expected_input_type_index)
        return caf::make_error(
          ec::invalid_configuration,
          fmt::format("pipeline must have matching operator types: operator "
                      "'{}' expected '{}' and received '{}'",
                      operator_->name(), operator_->input_type_name(),
                      expected_input_type_name));
      expected_input_type_index = operator_->output_type_index();
      expected_input_type_name = operator_->output_type_name();
    }
    if (expected_input_type_index != void_index)
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("pipeline must have the output type '{}' but got '{}'",
                    void_name, operators.back()->output_type_name()));
    auto result = plan{};
    result.operators_ = std::move(operators);
    return result;
  }

  detail::generator<caf::expected<void>> run() {
    auto physical_operators = std::vector<detail::stable_map<
      type, logical_operator<>::allowed_physical_operator_variant>>{};
    physical_operators.resize(operators_.size());
    // FIXME: implement execution
    co_return;
  }

private:
  std::vector<std::unique_ptr<logical_operator<>>> operators_;
};

// -- where operator ----------------------------------------------------------

struct where_operator final : logical_operator<table_slice, table_slice> {
  explicit where_operator(expression expr) noexcept : expr_{std::move(expr)} {
    // nop
  }

  [[nodiscard]] std::string_view name() const noexcept override {
    return "where";
  }

  [[nodiscard]] caf::expected<result_type> make(type input_schema) override {
    auto tailored_expr = tailor(expr_, input_schema);
    if (!tailored_expr)
      return std::move(tailored_expr.error());
    return
      [tailored_expr = std::move(*tailored_expr)](
        detail::generator<table_slice> pull) -> detail::generator<table_slice> {
        for (const auto& slice : pull) {
          if (auto filtered_slice = filter(slice, tailored_expr))
            co_yield std::move(*filtered_slice);
        }
      };
  }

private:
  expression expr_ = {};
};

// -- source operator ---------------------------------------------------------

struct source_operator final : logical_operator<void, table_slice> {
  explicit source_operator(std::vector<table_slice> slices) noexcept
    : slices_{std::move(slices)} {
    // nop
  }

  [[nodiscard]] std::string_view name() const noexcept override {
    return "source";
  }

  [[nodiscard]] caf::expected<result_type>
  make([[maybe_unused]] type input_schema) override {
    return
      [slices = std::exchange(
         slices_, {})]() mutable noexcept -> detail::generator<table_slice> {
        for (auto& slice : slices)
          co_yield std::move(slice);
      };
  }

private:
  std::vector<table_slice> slices_ = {};
};

// -- sink operator ---------------------------------------------------------

struct sink_operator final : logical_operator<table_slice, void> {
  using sink_function = std::function<void(table_slice)>;

  explicit sink_operator(sink_function sink) noexcept : sink_{std::move(sink)} {
    // nop
  }

  [[nodiscard]] std::string_view name() const noexcept override {
    return "sink";
  }

  [[nodiscard]] caf::expected<result_type>
  make([[maybe_unused]] type input_schema) override {
    return
      [sink = sink_](detail::generator<table_slice> pull) noexcept -> void {
        for (auto slice : pull)
          sink(std::move(slice));
      };
  }

private:
  sink_function sink_ = {};
};

struct fixture : fixtures::events {
  fixture() = default;
};

} // namespace

FIXTURE_SCOPE(pipeline_executor_fixture, fixture)

TEST(where_operator) {
  auto result = std::vector<table_slice>{};
  auto operators = std::vector<std::unique_ptr<logical_operator<>>>{};
  operators.push_back(std::make_unique<source_operator>(zeek_conn_log_full));
  operators.push_back(std::make_unique<where_operator>(
    unbox(to<expression>("orig_bytes > 100"))));
  operators.push_back(std::make_unique<sink_operator>([&](table_slice slice) {
    result.push_back(std::move(slice));
  }));
  auto put = unbox(plan::make(std::move(operators)));
  auto num_iterations = 0;
  for (const auto& ok : put.run()) {
    if (!ok)
      FAIL("plan execution failed: " << ok.error());
    ++num_iterations;
  }
  CHECK_EQUAL(num_iterations, 40);
  CHECK_EQUAL(rows(result), 120u);
}

FIXTURE_SCOPE_END()

} // namespace vast
