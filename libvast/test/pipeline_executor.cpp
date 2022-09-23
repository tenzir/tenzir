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
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/test/dsl.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>
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
struct physical_operator_fun final
  : std::function<
      auto(detail::generator<Input> /*pull*/)->detail::generator<Output>> {
  using super = std::function<
    auto(detail::generator<Input> /*pull*/)->detail::generator<Output>>;
  using super::super;
};

template <class Input>
struct physical_operator_fun<Input, void> final
  : std::function<auto(detail::generator<Input> /*pull*/)
                    ->detail::generator<caf::expected<void>>> {
  using super = std::function<auto(detail::generator<Input> /*pull*/)
                                ->detail::generator<caf::expected<void>>>;
  using super::super;
};

template <class Output>
struct physical_operator_fun<void, Output> final
  : std::function<auto()->detail::generator<Output>> {
  using super = std::function<auto()->detail::generator<Output>>;
  using super::super;
};

template <class Input, class Output>
struct physical_operator final {
  using type = physical_operator_fun<Input, Output>;
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

  using allowed_type_variant = caf::detail::tl_apply_t<
    caf::detail::tl_filter_type_t<allowed_type_list, void>, std::variant>;

  // FIXME: Invert list by forbidding void -> void
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
  using input_type = Input;
  using output_type = Output;

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

// -- actor mapping -----------------------------------------------------------

caf::behavior logical_operator_source(caf::event_based_actor* self,
                                      std::unique_ptr<logical_operator<>> op) {
  constexpr auto void_index
    = caf::detail::tl_index_of<logical_operator<>::allowed_type_list,
                               void>::value;
  if (op->input_type_index() != void_index) {
    self->quit(caf::make_error(ec::logic_error, "FIXME"));
    return {};
  }
  return {
    [self, op = std::move(op)](atom::run) mutable {
      struct stream_state_t {
        bool running = false;
        logical_operator<>::allowed_physical_operator_variant fun = {};
      };
      auto state = stream_state_t{};
      auto fun = op->make_erased({});
      VAST_ASSERT_CHEAP(fun); // FIXME
      state.fun = std::move(*fun);
      return caf::attach_stream_source(
        self,
        // Initializer.
        [x = std::move(state)](stream_state_t& state) mutable {
          state = std::move(x);
          state.running = true;
        },
        // Generator.
        [](stream_state_t& state,
           caf::downstream<logical_operator<>::allowed_type_variant>& out,
           size_t num) {
          auto f = [&]<class Input, class Output>(
                     const physical_operator_fun<Input, Output>& fun) {
            if constexpr (!std::is_void_v<Input>) {
              die("unreachable");
            } else {
              for (auto output : fun()) {
                MESSAGE("source produced");
                if (!output) {
                  MESSAGE("source interrupts because it idled");
                  break;
                }
                MESSAGE("source pushes");
                out.push(logical_operator<>::allowed_type_variant{
                  std::move(output),
                });
                if (--num == 0) {
                  MESSAGE("source interrupts because it exceeded num");
                  break;
                }
              }
              MESSAGE("source ends loop");
            }
          };
          std::visit(f, state.fun);
        },
        // Predicate.
        [](const stream_state_t& state) {
          return state.running;
        });
    },
  };
}

caf::behavior logical_operator_stage(caf::event_based_actor* self,
                                     std::unique_ptr<logical_operator<>> op) {
  constexpr auto void_index
    = caf::detail::tl_index_of<logical_operator<>::allowed_type_list,
                               void>::value;
  if (op->input_type_index() == void_index
      || op->output_type_index() == void_index) {
    self->quit(caf::make_error(ec::logic_error, "FIXME"));
    return {};
  }
  return {
    [self, op = std::move(op)](
      caf::stream<logical_operator<>::allowed_type_variant> in) mutable {
      struct stream_state_t {
        std::unique_ptr<logical_operator<>> op = {};
        std::unordered_map<
          type,
          std::pair<std::queue<logical_operator<>::allowed_type_variant>,
                    detail::generator<logical_operator<>::allowed_type_variant>>>
          inputs_and_outputs = {};
      };
      return caf::attach_stream_stage(
        self, in,
        // Initializer.
        [op = std::move(op)](stream_state_t& state) mutable {
          state.op = std::move(op);
        },
        // Processor.
        [](stream_state_t& state,
           caf::downstream<logical_operator<>::allowed_type_variant>& out,
           logical_operator<>::allowed_type_variant input_value) {
          // 1. Determine the schema.
          auto input_schema = type{};
          if (const auto* slice = std::get_if<table_slice>(&input_value)) {
            VAST_ASSERT_CHEAP(*slice);
            input_schema = slice->layout();
          }
          MESSAGE("stage received schema: " << input_schema.name());
          // 2. Create or cache our input and output state.
          auto input_and_output_it
            = state.inputs_and_outputs.find(input_schema);
          if (input_and_output_it == state.inputs_and_outputs.end()) {
            input_and_output_it
              = state.inputs_and_outputs.emplace_hint(input_and_output_it);
            auto fun = state.op->make_erased(input_schema);
            VAST_ASSERT_CHEAP(fun); // FIXME: handle error
            auto f = [&]<class Input, class Output>(
                       const physical_operator_fun<Input, Output>& fun) {
              if constexpr (std::is_void_v<Input> || std::is_void_v<Output>) {
                die("unreachable");
              } else {
                auto* input_ptr = &input_and_output_it->second.first;
                input_and_output_it->second.second
                  = [input_ptr, fun = std::move(fun)]()
                  -> detail::generator<logical_operator<>::allowed_type_variant> {
                  // We're lifting the strongly typed phyiscal operator instance
                  // into a type-erased generator here.
                  auto lifted_input
                    = [input_ptr]() -> detail::generator<Input> {
                    while (true) {
                      if (input_ptr->empty()) {
                        co_yield Input{};
                        continue;
                      }
                      auto* next = std::get_if<Input>(&input_ptr->front());
                      VAST_ASSERT_CHEAP(next);
                      co_yield std::move(*next);
                      input_ptr->pop();
                    }
                  }();
                  for (auto output_value : fun(std::move(lifted_input))) {
                    if (!output_value)
                      break;
                    co_yield logical_operator<>::allowed_type_variant{
                      std::move(output_value),
                    };
                  }
                }();
              }
            };
            std::visit(f, *fun);
          }
          auto& [input, output] = input_and_output_it->second;
          // 3. Push value into input queue.
          input.push(std::move(input_value));
          for (auto output_value : output)
            out.push(std::move(output_value));
        },
        // Finalizer.
        [](stream_state_t& state, const caf::error& err) {
          // FIXME: handle error
        });
    },
  };
}

caf::behavior logical_operator_sink(caf::event_based_actor* self,
                                    std::unique_ptr<logical_operator<>> op) {
  constexpr auto void_index
    = caf::detail::tl_index_of<logical_operator<>::allowed_type_list,
                               void>::value;
  if (op->output_type_index() != void_index) {
    self->quit(caf::make_error(ec::logic_error, "FIXME"));
    return {};
  }
  return {
    [self, op = std::move(op)](
      caf::stream<logical_operator<>::allowed_type_variant> in) mutable {
      struct stream_state_t {
        std::unique_ptr<logical_operator<>> op = {};
        std::unordered_map<
          type, std::pair<std::queue<logical_operator<>::allowed_type_variant>,
                          detail::generator<caf::expected<void>>>>
          inputs_and_outputs = {};
      };
      return caf::attach_stream_sink(
        self, in,
        // Initializer.
        [op = std::move(op)](stream_state_t& state) mutable {
          state.op = std::move(op);
        },
        // Processor.
        [](stream_state_t& state,
           logical_operator<>::allowed_type_variant input_value) {
          // 1. Determine the schema.
          auto input_schema = type{};
          if (const auto* slice = std::get_if<table_slice>(&input_value)) {
            if (!*slice)
              return;
            input_schema = slice->layout();
          }
          // 2. Create or cache our input and output state.
          auto input_and_output_it
            = state.inputs_and_outputs.find(input_schema);
          if (input_and_output_it == state.inputs_and_outputs.end()) {
            input_and_output_it
              = state.inputs_and_outputs.emplace_hint(input_and_output_it);
            auto fun = state.op->make_erased(input_schema);
            VAST_ASSERT(fun); // FIXME: handle error
            auto f = [&]<class Input, class Output>(
                       const physical_operator_fun<Input, Output>& fun) {
              if constexpr (!std::is_void_v<Output>) {
                die("unreachable");
              } else {
                auto* input_ptr = &input_and_output_it->second.first;
                input_and_output_it->second.second
                  = [input_ptr, fun = std::move(fun)]()
                  -> detail::generator<caf::expected<void>> {
                  // We're lifting the strongly typed phyiscal operator instance
                  // into a type-erased generator here.
                  auto lifted_input
                    = [input_ptr]() -> detail::generator<Input> {
                    while (true) {
                      if (input_ptr->empty()) {
                        co_yield Input{};
                        continue;
                      }
                      auto* next = std::get_if<Input>(&input_ptr->front());
                      VAST_ASSERT_CHEAP(next);
                      co_yield std::move(*next);
                      input_ptr->pop();
                    }
                  }();
                  return fun(std::move(lifted_input));
                }();
              }
            };
            std::visit(f, *fun);
          }
          auto& [input, output] = input_and_output_it->second;
          // 3. Push value into input queue.
          input.push(std::move(input_value));
          for (auto output_value : output)
            VAST_ASSERT_CHEAP(output_value); // FIXME
        },
        // Finalizer.
        [](stream_state_t& state, const caf::error& err) {
          // FIXME: handle error
        });
    },
  };
}

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

  template <class Actor>
  auto run_stream(const Actor& self) && {
    auto it = operators_.rbegin();
    auto pipeline = self->spawn(logical_operator_sink, std::move(*it++));
    for (; it < std::prev(operators_.rend()); ++it)
      pipeline = pipeline * self->spawn(logical_operator_stage, std::move(*it));
    pipeline
      = pipeline * self->spawn(logical_operator_source, std::move(*it++));
    VAST_ASSERT_CHEAP(it == operators_.rend());
    return self->request(pipeline, caf::infinite, atom::run_v);
  }

  auto run_local() && {
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
    return [sink = sink_](detail::generator<table_slice> pull) noexcept
           -> detail::generator<caf::expected<void>> {
      for (auto slice : pull) {
        sink(std::move(slice));
        co_yield {};
      }
    };
  }

private:
  sink_function sink_ = {};
};

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    // nop
  }
};

} // namespace

} // namespace vast

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::logical_operator<>::allowed_type_variant)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(
  std::vector<vast::logical_operator<>::allowed_type_variant>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(
  caf::stream<vast::logical_operator<>::allowed_type_variant>)

namespace vast {

FIXTURE_SCOPE(pipeline_executor_fixture, fixture)

TEST(where_operator) {
  auto self = caf::scoped_actor{sys};
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
  auto handle = std::move(put).run_stream(self);
  run();
  handle.receive(
    [](const caf::message& msg) {
      MESSAGE(msg);
    },
    [](const caf::error& err) {
      FAIL(err);
    });
  std::this_thread::sleep_for(std::chrono::seconds{10});

  /* for (const auto& ok : put.run()) { */
  /*   if (!ok) */
  /*     FAIL("plan execution failed: " << ok.error()); */
  /*   ++num_iterations; */
  /* } */
  CHECK_EQUAL(num_iterations, 40);
  CHECK_EQUAL(rows(result), 120u);
}

FIXTURE_SCOPE_END()

} // namespace vast
