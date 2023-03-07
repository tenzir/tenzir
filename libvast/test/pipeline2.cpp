//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline2.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/overload.hpp"
#include "vast/expression.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/detail/scope_guard.hpp>
#include <caf/test/dsl.hpp>

#include <queue>
#include <unordered_map>

namespace vast {
namespace {

struct command final : public logical_operator<void, void> {
  caf::expected<physical_operator<void, void>>
  instantiate(const type& input_schema) noexcept override {
    REQUIRE(!input_schema);
    return []() -> generator<std::monostate> {
      MESSAGE("hello, world!");
      co_return;
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "command";
  }
};

struct source final : public logical_operator<void, events> {
  explicit source(std::vector<table_slice> events)
    : events_(std::move(events)) {
  }

  caf::expected<physical_operator<void, events>>
  instantiate(const type& input_schema) noexcept override {
    REQUIRE(!input_schema);
    return [*this]() -> generator<table_slice> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("source destroy");
      }};
      for (auto& table_slice : events_) {
        MESSAGE("source yield");
        co_yield table_slice;
      }
      MESSAGE("source return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "source";
  }

  std::vector<table_slice> events_;
};

struct sink final : public logical_operator<events, void> {
  explicit sink(std::function<void(table_slice)> callback)
    : callback_(std::move(callback)) {
  }

  caf::expected<physical_operator<events, void>>
  instantiate(const type& input_schema) noexcept override {
    return [=](generator<table_slice> input) -> generator<std::monostate> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("sink destroy");
      }};
      for (auto&& slice : input) {
        if (slice.rows() != 0) {
          REQUIRE_EQUAL(slice.schema(), input_schema);
          MESSAGE("sink callback");
          callback_(slice);
        }
        MESSAGE("sink yield");
        co_yield {};
      }
      MESSAGE("sink return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return "sink";
  }

  std::function<void(table_slice)> callback_;
};

struct where final : public logical_operator<events, events> {
  explicit where(expression expr) : expr_(std::move(expr)) {
  }

  caf::expected<physical_operator<events, events>>
  instantiate(const type& input_schema) noexcept override {
    auto expr = tailor(expr_, input_schema);
    if (!expr) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to instantiate where "
                                         "operator: {}",
                                         expr.error()));
    }
    return [expr = std::move(*expr)](
             generator<table_slice> input) -> generator<table_slice> {
      auto guard = caf::detail::scope_guard{[] {
        MESSAGE("where destroy");
      }};
      for (auto&& slice : input) {
        // TODO: adjust filter
        if (auto result = filter(slice, expr)) {
          MESSAGE("where yield result");
          co_yield *result;
        } else {
          MESSAGE("where yield no result");
          co_yield {};
        }
      }
      MESSAGE("where return");
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("where {}", expr_);
  }

  expression expr_;
};

// All generators must ...
// - co_return if the input is exhausted
// - co_yield empty if the input yields empty (because we have to stall?)

template <class Batch>
auto make_batch_buffer(std::shared_ptr<bool> stop) {
  auto queue = std::make_shared<std::queue<Batch>>();
  auto gen = [](std::shared_ptr<std::queue<Batch>> queue,
                std::shared_ptr<bool> stop) -> generator<Batch> {
    // This generator never co_returns...
    while (!*stop) {
      if (queue->empty()) {
        // MESSAGE("yield empty");
        co_yield Batch{};
        continue;
      }
      auto element = std::move(queue->front());
      VAST_ASSERT(batch_traits<Batch>::size(element) != 0);
      queue->pop();
      // MESSAGE("yield element");
      co_yield std::move(element);
    }
  }(queue, stop);
  return std::tuple{std::move(queue), std::move(gen)};
}

auto make_run(std::vector<logical_operator_ptr> ops) {
  // We can handle the first operator in a special way, as its input element
  // type is always void.
  auto current_op = ops.begin();
  auto run
    = [](logical_operator_ptr op) mutable noexcept -> generator<runtime_batch> {
    auto gen = unbox(op->runtime_instantiate({}));
    auto f = [op = std::move(op)]<element_type Input, element_type Output>(
               physical_operator<Input, Output> gen) mutable noexcept
      -> generator<runtime_batch> {
      if constexpr (not std::is_void_v<Input>) {
        die("unreachable");
      } else {
        // gen lives throughout the iteration
        for (auto&& output : gen()) {
          // MESSAGE("yield output");
          co_yield std::move(output);
        }
      }
    };
    return std::visit(std::move(f), std::move(gen));
  }(std::move(*current_op++));
  // Now we repeat the process for the following operators, knowing that their
  // input element type is never void.
  for (; current_op != ops.end(); ++current_op) {
    run =
      [](generator<runtime_batch> prev_run,
         logical_operator_ptr op) mutable noexcept -> generator<runtime_batch> {
      struct gen_state {
        generator<runtime_batch> gen;
        generator<runtime_batch>::iterator current;
        std::function<void(runtime_batch)> push;
      };
      // For every input element, we take the following steps:
      auto stop = std::make_shared<bool>(false);
      auto gens = std::unordered_map<type, gen_state>{};
      auto f = [&gens, &stop, op = op.get()]<class Batch>(
                 Batch input) mutable -> generator<runtime_batch> {
        if (batch_traits<Batch>::size(input) == 0) {
          co_return;
        }
        // 1. Find the input schema.
        auto input_schema = batch_traits<Batch>::schema(input);
        // 2. Try to find an already existing generator, or create a new one
        // if it doesn't exist yet for the given input schema.
        auto gen_it = gens.find(input_schema);
        if (gen_it == gens.end()) {
          MESSAGE("created batch buffer for '" << op->to_string()
                                               << "': " << input_schema.name());
          auto [buffer_queue, buffer] = make_batch_buffer<Batch>(stop);
          auto f
            = [buffer
               = std::move(buffer)]<element_type Input, element_type Output>(
                physical_operator<Input, Output> gen) mutable noexcept
            -> generator<runtime_batch> {
            if constexpr (std::is_void_v<Input>
                          || !std::is_same_v<element_type_to_batch_t<Input>,
                                             Batch>) {
              die("unreachable"); // TODO
            } else {
              // gen lives throughout the iteration
              for (auto&& output : gen(std::move(buffer))) {
                // MESSAGE("yield 123");
                co_yield std::move(output);
              }
            }
          };
          gen_it = gens.emplace_hint(gen_it, input_schema, gen_state{});
          auto& state = gen_it->second;
          state.gen = std::visit(std::move(f),
                                 unbox(op->runtime_instantiate(input_schema)));
          state.push = [queue = std::move(buffer_queue)](runtime_batch batch) {
            queue->push(std::get<Batch>(std::move(batch)));
          };
          // 3. Push the input element into the buffer.
          state.push(std::move(input));
          state.current = state.gen.begin();
        } else {
          // 3. Push the input element into the buffer.
          gen_it->second.push(std::move(input));
        }
        // 4. Pull from the buffer
        auto& state = gen_it->second;
        while (true) {
          // TODO: never happens
          if (state.current == state.gen.end()) {
            // MESSAGE("at end of generator");
            co_return;
          }
          auto output = std::move(*state.current);
          ++state.current;
          // MESSAGE("yielded in generator");
          co_yield std::move(output);
        }
      };
      for (auto&& input : prev_run) {
        for (auto&& output : std::visit(f, std::move(input))) {
          auto const empty = output.size() == 0;
          // MESSAGE("will yield in outer generator");
          co_yield std::move(output);
          if (empty) {
            // MESSAGE("empty break");
            break;
          }
        }
      }
      *stop = true;
      for (auto& gen : gens) {
        for (; gen.second.current != gen.second.gen.end();
             ++gen.second.current) {
          auto output = std::move(*gen.second.current);
          if (output.size() != 0) {
            co_yield std::move(output);
          }
        }
      }
    }(std::move(run), std::move(*current_op));
  }
  return run;
}

auto execute(pipeline2 pipeline) noexcept -> caf::expected<void> {
  auto ops = std::move(pipeline).unwrap();
  if (ops.empty())
    return {}; // no-op
  if (ops.front()->input_element_type().id != element_type_id<void>) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("unable to execute pipeline: expected "
                                       "input type {}, got {}",
                                       element_type_traits<void>::name,
                                       ops.front()->input_element_type().name));
  }
  if (ops.back()->output_element_type().id != element_type_id<void>) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("unable to execute pipeline: expected "
                                       "output type {}, got {}",
                                       element_type_traits<void>::name,
                                       ops.back()->output_element_type().name));
  }
  for (auto&& elem : make_run(std::move(ops))) {
    // TODO: We should check whether there's been an error every time we arrive
    // here.
    // FIXME: this can just be an assertion; nothing to do here
    MESSAGE("got output with size " << elem.size());
    REQUIRE(std::holds_alternative<std::monostate>(elem));
  }
  return {};
}

template <class... Ts>
auto make_pipeline(Ts&&... ts) -> pipeline2 {
  auto ops = std::vector<logical_operator_ptr>{};
  (ops.push_back(std::make_unique<Ts>(ts)), ...);
  return unbox(pipeline2::make(std::move(ops)));
}

struct fixture : fixtures::events {};

} // namespace

TEST(command) {
  auto put = make_pipeline(command{});
  REQUIRE_NOERROR(execute(std::move(put)));
}

FIXTURE_SCOPE(pipeline2_fixture, fixture);

TEST(source | where #type == "zeek.conn" | sink) {
  auto put = make_pipeline(
    source{{head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1),
            head(zeek_conn_log.at(0), 1), head(zeek_conn_log.at(0), 1)}},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    where{unbox(to<expression>(R"(#type == "zeek.conn")"))},
    sink{[](const table_slice&) {
      MESSAGE("---- sink ----");
      return;
    }});
  REQUIRE_NOERROR(execute(std::move(put)));
}

FIXTURE_SCOPE_END()

} // namespace vast
