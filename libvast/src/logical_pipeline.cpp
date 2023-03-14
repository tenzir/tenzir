//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_pipeline.hpp"

#include "vast/concept/parseable/vast/pipeline.hpp"
#include "vast/element_type.hpp"
#include "vast/logical_operator.hpp"
#include "vast/operator_control_plane.hpp"
#include "vast/physical_operator.hpp"
#include "vast/plugin.hpp"

#include <queue>
#include <unordered_map>

namespace vast {

namespace {

class execute_ctrl final : public operator_control_plane {
public:
  explicit execute_ctrl() noexcept = default;

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

/// Creates a generator from a source operator.
/// @pre `op->input_element_type()` must be `void`
auto producer_from_logical_source(runtime_logical_operator* op,
                                  operator_control_plane* ctrl)
  -> generator<runtime_batch> {
  auto gen = op->make_runtime_physical_operator({}, ctrl);
  if (not gen) {
    ctrl->abort(gen.error());
    return {};
  }
  auto f = []<element_type Input, element_type Output>(
             physical_operator<Input, Output> gen) -> generator<runtime_batch> {
    if constexpr (not std::is_void_v<Input>) {
      die("tried to instantiate pipeline that does not begin with void");
    } else {
      // This coroutine captures keeps the physical operator alive.
      for (auto&& output : gen()) {
        VAST_INFO("yield output");
        // This performs a conversion to `runtime_batch`.
        co_yield std::move(output);
      }
    }
  };
  return std::visit(f, std::move(*gen));
}

struct instance_state {
  generator<runtime_batch> gen;
  generator<runtime_batch>::iterator it;
  std::queue<runtime_batch> queue;
  runtime_physical_operator physical_op;
};

template <class Batch>
auto generator_for_queue(std::queue<runtime_batch>& queue, bool& stop)
  -> generator<Batch> {
  while (!stop) {
    if (queue.empty()) {
      // MESSAGE("yield empty");
      co_yield Batch{};
      continue;
    }
    auto batch_ptr = std::get_if<Batch>(&queue.front());
    VAST_ASSERT(batch_ptr);
    auto batch = std::move(*batch_ptr);
    queue.pop();
    VAST_ASSERT(batch_traits<Batch>::size(batch) != 0);
    // MESSAGE("yield element");
    co_yield std::move(batch);
  }
}

template <class Batch>
  requires(!std::same_as<Batch, runtime_batch>)
auto erase_producer(generator<Batch> producer) -> generator<runtime_batch> {
  for (auto&& output : producer) {
    co_yield std::move(output);
  }
}

template <class InputBatch>
auto make_producer_from_queue(std::queue<runtime_batch>& queue, bool& stop,
                              runtime_physical_operator& physical_op)
  -> generator<runtime_batch> {
  auto input_generator = generator_for_queue<InputBatch>(queue, stop);
  auto make_generator
    = [&]<element_type Input, element_type Output>(
        physical_operator<Input, Output>& physical_op) noexcept
    -> generator<runtime_batch> {
    if constexpr (std::is_void_v<Input>) {
      die("input type of operator must not be void here");
    } else if constexpr (!std::is_same_v<element_type_to_batch_t<Input>,
                                         InputBatch>) {
      die("input generator did not yield operator input type");
    } else {
      return erase_producer(physical_op(std::move(input_generator)));
    }
  };
  return std::visit(make_generator, physical_op);
}

auto exhaust_on_stall(instance_state& instance) -> generator<runtime_batch> {
  // This effectively returns a generator that pulls from the instantiation,
  // which pulls from the queue. A stall is requested when the queue is empty.
  // In this case, the generator returned here becomes exhausted, which will
  // request the next batch from `previous`.
  while (true) {
    // The inner generators can only end after `stop` is set, but the operator
    // instantiation can end before that.
    if (instance.it == instance.gen.end()) {
      break;
    }
    auto output = std::move(*instance.it);
    auto empty = size(output) == 0;
    ++instance.it;
    VAST_INFO("yielded in generator");
    co_yield std::move(output);
    if (empty) {
      break;
    }
  }
}

/// Appends an operator to an existing generator.
/// @pre `previous` may only yield batches for `op->input_element_type()`
/// @pre `op->input_element_type()` must not be `void`
auto append_logical_operator(generator<runtime_batch> previous,
                             runtime_logical_operator* op,
                             operator_control_plane* ctrl)
  -> generator<runtime_batch> {
  // For every unique schema retrieved from `previous`, we want to instantiate
  // `op` once. Thus, we return a generator that, when advanced, advances
  // `previous` and looks at the schema of the input batch. If we have not seen
  // that schema yet, we instantiate `op` and a schema-specific queue. The
  // instantiated operator is provided a generator that tries to pull new
  // elements from the queue, and the input batch we are dispatching is pushed
  // to this queue. Because this provides additional input to to the
  // corresponding instance, we iterate over it in order to stream its
  // results if possible. This does not necessarily already yield results, due
  // to operators such as `summarize`. Hence, when the input generator is
  // exhausted, we signal the generators that pull from the queue that they
  // should stop as well. Finally, we iterate once more over all operator
  // instances. They will notice that their input is exhausted, which means
  // that they will become exhausted themselves eventually.

  // We can capture references to `instance_state` due to pointer stability.
  auto instances = std::unordered_map<type, instance_state>{};
  auto stop = false;

  /// Dispatches an input batch, creating physical operators on demand.
  auto dispatch = [&]<class InputBatch>(InputBatch input)
    -> caf::expected<std::reference_wrapper<instance_state>> {
    // Empty batches should be handled before calling this.
    VAST_ASSERT(batch_traits<InputBatch>::size(input) != 0);
    // For every input batch, we take the following steps:
    // 1. Find the input schema.
    auto input_schema = batch_traits<InputBatch>::schema(input);
    // 2. Try to find an already existing generator, or create a new one
    // if it doesn't exist yet for the given input schema.
    auto lookup = instances.find(input_schema);
    if (lookup == instances.end()) {
      auto physical_op = op->make_runtime_physical_operator(input_schema, ctrl);
      if (not physical_op) {
        return std::move(physical_op.error());
      }
      lookup = instances.emplace_hint(lookup, input_schema, instance_state{});
      auto& instance = lookup->second;
      instance.physical_op = std::move(*physical_op);
      instance.gen = make_producer_from_queue<InputBatch>(instance.queue, stop,
                                                          instance.physical_op);
      // 3. Push the input element into the buffer.
      instance.queue.push(std::move(input));
      instance.it = instance.gen.begin();
    } else {
      // 3. Push the input element into the buffer.
      auto& instance = lookup->second;
      if (instance.it != instance.gen.end()) {
        instance.queue.push(std::move(input));
      }
    }
    return std::ref(lookup->second);
  };

  for (auto&& input : previous) {
    if (size(input) == 0) {
      VAST_INFO("inner stalled");
      co_yield {};
      continue;
    }
    auto instance = std::visit(dispatch, std::move(input));
    if (!instance) {
      ctrl->abort(std::move(instance.error()));
    }
    // Iterate until the queue is empty.
    for (auto&& output : exhaust_on_stall(*instance)) {
      VAST_INFO("will yield in outer generator");
      co_yield std::move(output);
    }
    if (op->done()) {
      // If all instances require no more input, then we can abort the
      // iteration over `previous` and process the instances until they
      // become exhausted.
      break;
    }
  }
  stop = true;
  for (auto& [_, instance] : instances) {
    // The input to `gen` will now become exhausted when the queue is empty.
    // Thus, the instantiation must also become exhausted eventually. This also
    // implies that we can just ignore empty batches.
    for (; instance.it != instance.gen.end(); ++instance.it) {
      auto output = std::move(*instance.it);
      if (size(output) != 0) {
        co_yield std::move(output);
      }
    }
  }
}

/// Creates a generator from a pipeline that is closed from the left.
/// @pre `!ops.empty()`
/// @pre `ops[0]->input_element_type()` must be `void`
/// @pre The pipeline defined by `ops` must not be ill-typed.
auto make_local_producer(std::span<logical_operator_ptr> ops,
                         operator_control_plane* ctrl)
  -> generator<runtime_batch> {
  auto it = ops.begin();
  VAST_ASSERT(it != ops.end());
  auto producer = producer_from_logical_source((it++)->get(), ctrl);
  for (; it != ops.end(); ++it) {
    producer = append_logical_operator(std::move(producer), it->get(), ctrl);
  }
  return producer;
}

} // namespace

auto logical_pipeline::parse(std::string_view repr)
  -> caf::expected<logical_pipeline> {
  auto ops = std::vector<logical_operator_ptr>{};
  // plugin name parser
  using parsers::alnum, parsers::chr, parsers::space, parsers::optional_ws;
  const auto plugin_name_char_parser = alnum | chr{'-'};
  const auto plugin_name_parser = optional_ws >> +plugin_name_char_parser;
  while (!repr.empty()) {
    // 1. parse a single word as operator plugin name
    const auto* f = repr.begin();
    const auto* const l = repr.end();
    auto plugin_name = std::string{};
    if (!plugin_name_parser(f, l, plugin_name)) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator name is invalid",
                                         repr));
    }
    // 2. find plugin using operator name
    const auto* plugin = plugins::find<logical_operator_plugin>(plugin_name);
    if (!plugin) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator '{}' does not exist",
                                         repr, plugin_name));
    }
    // 3. ask the plugin to parse itself from the remainder
    auto [remaining_repr, op]
      = plugin->make_logical_operator(std::string_view{f, l});
    if (!op)
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to parse pipeline '{}': {}",
                                         repr, op.error()));
    ops.push_back(std::move(*op));
    repr = remaining_repr;
  }
  return make(std::move(ops));
}

auto logical_pipeline::make(std::vector<logical_operator_ptr> ops)
  -> caf::expected<logical_pipeline> {
  const auto invalid = std::find(ops.begin(), ops.end(), nullptr);
  if (invalid != ops.end())
    return caf::make_error(ec::invalid_argument,
                           fmt::format("invalid operator at index {}",
                                       std::distance(ops.begin(), invalid)));
  const auto mismatch
    = std::adjacent_find(ops.begin(), ops.end(), [](auto& a, auto& b) {
        return a->output_element_type() != b->input_element_type()
               && a->output_element_type().id != element_type_id<void>;
      });
  if (mismatch != ops.end()) {
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("element type mismatch: cannot connect {} -> {}",
                  (*mismatch)->output_element_type().name,
                  (*(mismatch + 1))->input_element_type().name));
  }
  auto flattened = std::vector<logical_operator_ptr>{};
  flattened.reserve(ops.size());
  for (auto& op : ops) {
    if (auto* p = dynamic_cast<logical_pipeline*>(op.get())) {
      flattened.insert(flattened.end(),
                       std::make_move_iterator(p->ops_.begin()),
                       std::make_move_iterator(p->ops_.end()));
    } else {
      flattened.push_back(std::move(op));
    }
  }
  return logical_pipeline{std::move(flattened)};
}

auto logical_pipeline::input_element_type() const noexcept
  -> runtime_element_type {
  if (ops_.empty())
    return element_type_traits<void>{};
  return ops_.front()->input_element_type();
}

auto logical_pipeline::output_element_type() const noexcept
  -> runtime_element_type {
  if (ops_.empty())
    return element_type_traits<void>{};
  return ops_.back()->output_element_type();
}

auto logical_pipeline::make_runtime_physical_operator(
  [[maybe_unused]] const type& input_schema,
  [[maybe_unused]] operator_control_plane* ctrl) noexcept
  -> caf::expected<runtime_physical_operator> {
  return caf::make_error(ec::logic_error, "instantiated pipeline");
}

auto logical_pipeline::to_string() const noexcept -> std::string {
  return fmt::to_string(fmt::join(ops_, " | "));
}

auto logical_pipeline::closed() const noexcept -> bool {
  return input_element_type().id == element_type_id<void>
         && output_element_type().id == element_type_id<void>;
}

auto logical_pipeline::unwrap() && -> std::vector<logical_operator_ptr> {
  return std::exchange(ops_, {});
}

auto make_local_executor(logical_pipeline pipeline) noexcept
  -> generator<caf::expected<void>> {
  if (!pipeline.closed()) {
    co_yield caf::make_error(ec::invalid_argument,
                             fmt::format("pipeline is not closed: {} -> {}",
                                         pipeline.input_element_type().name,
                                         pipeline.output_element_type().name));
    co_return;
  }
  auto ops = std::move(pipeline).unwrap();
  if (ops.empty())
    co_return; // no-op
  auto ctrl = execute_ctrl{};
  auto producer = make_local_producer(ops, &ctrl);
  for (auto&& output : producer) {
    if (ctrl.error()) {
      VAST_INFO("got error: {}", ctrl.error());
      co_yield ctrl.error();
      co_return;
    }
    VAST_ASSERT(std::holds_alternative<std::monostate>(output));
    co_yield {};
  }
  if (ctrl.error()) {
    VAST_INFO("got error: {}", ctrl.error());
    co_yield ctrl.error();
    co_return;
  }
}

} // namespace vast
