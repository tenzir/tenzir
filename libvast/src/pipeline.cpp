//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include <queue>
#include <unordered_map>

namespace vast {

namespace {

class execute_ctrl final : public operator_control_plane {
public:
  explicit execute_ctrl() {
  }

  [[nodiscard]] auto error() const noexcept -> const caf::error& {
    return error_;
  }

private:
  [[nodiscard]] auto self() noexcept -> caf::event_based_actor* override {
    die("not implemented");
  }

  auto abort(caf::error error) noexcept -> void override {
    error_ = std::move(error);
  }

  auto warn(caf::error) noexcept -> void override {
    die("not implemented");
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto demand(type = {}) const noexcept -> size_t override {
    die("not implemented");
  }

  /// Access available schemas.
  auto schemas() const noexcept -> const std::vector<type>& override {
    die("not implemented");
  }

  /// Access available concepts.
  auto concepts() const noexcept -> const concepts_map& override {
    die("not implemented");
  }

  caf::error error_ = {};
};

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

auto make_run(std::span<const logical_operator_ptr> ops,
              operator_control_plane* ctrl) {
  // We can handle the first operator in a special way, as its input element
  // type is always void.
  auto current_op = ops.begin();
  auto run = [](const runtime_logical_operator* op,
                operator_control_plane* ctrl) mutable noexcept
    -> generator<runtime_batch> {
    auto f = []<element_type Input, element_type Output>(
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
    auto gen = op->runtime_instantiate({}, ctrl);
    if (not gen) {
      ctrl->abort(gen.error());
      return {};
    }
    return std::visit(std::move(f), std::move(*gen));
  }((current_op++)->get(), ctrl);
  // Now we repeat the process for the following operators, knowing that their
  // input element type is never void.
  for (; current_op != ops.end(); ++current_op) {
    run = [](generator<runtime_batch> prev_run,
             const runtime_logical_operator* op,
             operator_control_plane* ctrl) mutable noexcept
      -> generator<runtime_batch> {
      struct gen_state {
        generator<runtime_batch> gen;
        generator<runtime_batch>::iterator current;
        std::function<void(runtime_batch)> push;
      };
      // For every input element, we take the following steps:
      auto stop = std::make_shared<bool>(false);
      auto gens = std::unordered_map<type, gen_state>{};
      auto f = [&gens, &stop, op, ctrl]<class Batch>(
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

          auto gen = op->runtime_instantiate(input_schema, ctrl);
          if (not gen) {
            ctrl->abort(std::move(gen.error()));
            co_return;
          }

          gen_it = gens.emplace_hint(gen_it, input_schema, gen_state{});
          auto& state = gen_it->second;
          state.gen = std::visit(std::move(f), std::move(*gen));
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
    }(std::move(run), current_op->get(), ctrl);
  }
  return run;
}

} // namespace

auto pipeline::execute() const noexcept -> caf::expected<void> {
  if (ops_.empty())
    return {}; // no-op
  if (ops_.front()->input_element_type().id != element_type_id<void>) {
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("unable to execute pipeline: expected "
                  "input type {}, got {}",
                  element_type_traits<void>::name,
                  ops_.front()->input_element_type().name));
  }
  if (ops_.back()->output_element_type().id != element_type_id<void>) {
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("unable to execute pipeline: expected "
                  "output type {}, got {}",
                  element_type_traits<void>::name,
                  ops_.back()->output_element_type().name));
  }
  auto ctrl = execute_ctrl{};
  for (auto&& elem : make_run(ops_, &ctrl)) {
    if (ctrl.error()) {
      VAST_INFO("got error: {}", ctrl.error());
      return ctrl.error();
    }
    VAST_ASSERT(std::holds_alternative<std::monostate>(elem));
  }
  if (ctrl.error()) {
    VAST_INFO("got error: {}", ctrl.error());
    return ctrl.error();
  }
  return {};
}
} // namespace vast
