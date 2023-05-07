//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/execution_node.hpp"

#include "vast/framed.hpp"
#include "vast/modules.hpp"
#include "vast/operator_control_plane.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <iterator>

namespace vast {

namespace {

class actor_control_plane : public operator_control_plane {
public:
  explicit actor_control_plane(
    system::execution_node_actor::stateful_impl<execution_node_state>& self)
    : self_{self} {
  }

  auto self() noexcept -> system::execution_node_actor::base& override {
    return self_;
  }

  auto node() noexcept -> system::node_actor override {
    return self_.state.node;
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_DEBUG("{} called actor_control_plane::abort({})", self_, error);
    VAST_ASSERT(error != caf::none);
    self_.quit(std::move(error));
  }

  auto warn(caf::error err) noexcept -> void override {
    VAST_WARN("[WARN] {}: {}", self_.state.op->to_string(), err);
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    return vast::modules::schemas();
  }

  auto concepts() const noexcept -> const concepts_map& override {
    return vast::modules::concepts();
  }

private:
  system::execution_node_actor::stateful_impl<execution_node_state>& self_;
};

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return !chunk || chunk->size() == 0;
}

template <class Output>
  requires(!std::same_as<Output, std::monostate>)
class source_driver final
  : public caf::stream_source_driver<
      caf::broadcast_downstream_manager<framed<Output>>> {
public:
  source_driver(generator<Output> gen, const execution_node_state& state)
    : gen_(std::move(gen)), state_{state} {
  }

  void pull(caf::downstream<framed<Output>>& out, size_t num) override {
    // TODO: use num and timeout logic?
    // auto num_events = num * 10000 / 100;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      // The source will signal that it is exhausted in `done()`, which always
      // gets called afer `pull()`, so we don't need to check it here.
      return;
    }
    for (size_t i = 0; i < num; ++i) {
      ++it;
      if (it == gen_.end()) {
        out.push(std::nullopt);
        return;
      }
      auto next = std::move(*it);
      if (empty(next))
        return;
      out.push(std::move(next));
    }
  }

  auto done() const noexcept -> bool override {
    auto is_done = gen_.unsafe_current() == gen_.end();
    if (is_done) {
      VAST_DEBUG("source is done");
    }
    return is_done;
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing source: {}", error);
    state_.self->quit();
  }

private:
  generator<Output> gen_;
  const execution_node_state& state_;
};

template <class Input>
auto generator_for_queue(std::shared_ptr<std::deque<Input>> queue,
                         std::shared_ptr<bool> stop) -> generator<Input> {
  VAST_ASSERT(queue);
  while (true) {
    if (queue->empty()) {
      if (*stop) {
        co_return;
      }
      co_yield Input{};
      continue;
    }
    auto batch = std::move(queue->front());
    queue->pop_front();
    // Empty batches should not be put in any queue, but handled directly.
    VAST_ASSERT(!empty(batch));
    co_yield std::move(batch);
  }
}

// We need a custom driver to get access to `out` when finalizing.
template <class Input, class Output>
  requires(!std::same_as<Input, std::monostate>
           && !std::same_as<Output, std::monostate>)
class stage_driver final
  : public caf::stream_stage_driver<
      framed<Input>, caf::broadcast_downstream_manager<framed<Output>>> {
  using super = caf::stream_stage_driver<
    framed<Input>, caf::broadcast_downstream_manager<framed<Output>>>;

public:
  stage_driver(caf::broadcast_downstream_manager<framed<Output>>& out,
               std::shared_ptr<std::deque<Input>> queue,
               std::shared_ptr<bool> stop, generator<Output> gen,
               const execution_node_state& state)
    : super{out},
      queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)},
      state_{state} {
  }

  void process(caf::downstream<framed<Output>>& out,
               std::vector<framed<Input>>& in) override {
    VAST_DEBUG("stage driver received input ({})", state_.op->to_string());
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      out.push(std::nullopt);
      return;
    }
    VAST_ASSERT(queue_->empty());
    VAST_ASSERT(std::none_of(in.begin(), in.end(), [](auto& x) {
      return not x.is_sentinel() && empty(x.value());
    }));
    for (auto&& elem : in) {
      if (elem.is_sentinel()) {
        VAST_ASSERT(&elem == &in.back());
        *stop_ = true;
        break;
      }
      queue_->push_back(std::move(elem.value()));
    }
    while (true) {
      ++it;
      if (it == gen_.end()) {
        out.push(std::nullopt);
        return;
      }
      auto batch = std::move(*it);
      if (empty(batch) && queue_->empty() && !*stop_) {
        return;
      }
      if (!empty(batch)) {
        out.push(std::move(batch));
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing stage driver for ({}), error = {}",
               state_.op->to_string(), error);
    state_.self->quit(error);
  }

private:
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<Output> gen_;
  const execution_node_state& state_;
};

// We need this to get access to `out` when finalizing.
template <class Input>
  requires(!std::same_as<Input, std::monostate>)
class sink_driver final : public caf::stream_sink_driver<framed<Input>> {
public:
  sink_driver(std::shared_ptr<std::deque<Input>> queue,
              std::shared_ptr<bool> stop, generator<std::monostate> gen,
              const execution_node_state& state)
    : queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)},
      state_{state} {
  }

  void process(std::vector<framed<Input>>& in) override {
    VAST_DEBUG("sink driver received input");
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      state_.self->quit();
      return;
    }
    VAST_ASSERT(queue_->empty());
    for (auto&& elem : in) {
      if (elem.is_sentinel()) {
        VAST_ASSERT(&elem == &in.back());
        *stop_ = true;
        break;
      }
      queue_->push_back(std::move(elem.value()));
    }
    while (!queue_->empty() || *stop_) {
      ++it;
      if (it == gen_.end()) {
        state_.self->quit();
        return;
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing sink driver: {}", error);
    state_.self->quit();
  }

private:
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<std::monostate> gen_;
  const execution_node_state& state_;
};

} // namespace

auto execution_node_state::start(std::vector<caf::actor> next)
  -> caf::result<void> {
  if (!op) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} was already started", *self));
  }
  auto result = op->instantiate(std::monostate{}, *ctrl);
  if (!result) {
    self->quit(std::move(result.error()));
    return {};
  }
  return std::visit(
    detail::overload{
      [&](generator<std::monostate>&) -> caf::result<void> {
        // This case corresponds to a `void -> void` operator.
        if (!next.empty()) {
          return caf::make_error(ec::logic_error,
                                 fmt::format("pipeline was already closed by "
                                             "'{}', but has more "
                                             "operators ({}) afterwards",
                                             op->to_string(), next.size()));
        }
        // TODO: void -> void not implemented
        return caf::make_error(ec::unimplemented,
                               "support for void -> void operators is not "
                               "implemented yet");
      },
      [&]<class T>(generator<T>& gen) -> caf::result<void> {
        if (next.empty()) {
          return caf::make_error(
            ec::logic_error, "pipeline is still open after last operator '{}'",
            op->to_string());
        }
        auto source = caf::detail::make_stream_source<source_driver<T>>(
          self, std::move(gen), *this);
        auto dest = std::move(next.front());
        next.erase(next.begin());
        source->add_outbound_path(dest, std::make_tuple(std::move(next)));
        return {};
      }},
    *result);
}

template <class Input>
auto execution_node_state::start(caf::stream<framed<Input>> in,
                                 std::vector<caf::actor> next)
  -> caf::result<caf::inbound_stream_slot<framed<Input>>> {
  if (!op) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} was already started", *self));
  }
  auto queue = std::make_shared<std::deque<Input>>();
  auto stop = std::make_shared<bool>();
  auto result = op->instantiate(generator_for_queue(queue, stop), *ctrl);
  if (!result) {
    self->quit(std::move(result.error()));
    return {};
  }
  return std::visit(
    [&]<class Output>(generator<Output> gen)
      -> caf::expected<caf::inbound_stream_slot<framed<Input>>> {
      if constexpr (std::is_same_v<Output, std::monostate>) {
        if (!next.empty()) {
          return caf::make_error(ec::logic_error,
                                 fmt::format("pipeline was already closed by "
                                             "'{}', but has more "
                                             "operators ({}) afterwards",
                                             op->to_string(), next.size()));
        }
        auto sink = caf::detail::make_stream_sink<sink_driver<Input>>(
          self, std::move(queue), std::move(stop), std::move(gen), *this);
        return sink->add_inbound_path(in);
      } else {
        if (next.empty()) {
          return caf::make_error(
            ec::logic_error, "pipeline is still open after last operator '{}'",
            op->to_string());
        }
        auto stage
          = caf::detail::make_stream_stage<stage_driver<Input, Output>>(
            self, std::move(queue), std::move(stop), std::move(gen), *this);
        auto slot = stage->add_inbound_path(in);
        auto dest = std::move(next.front());
        next.erase(next.begin());
        stage->add_outbound_path(dest, std::make_tuple(std::move(next)));
        return slot;
      }
    },
    std::move(*result));
}

auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op, system::node_actor node)
  -> system::execution_node_actor::behavior_type {
  self->state.self = self;
  self->state.op = std::move(op);
  self->state.ctrl = std::make_unique<actor_control_plane>(*self);
  self->state.node = std::move(node);
  return {
    [self](atom::run, std::vector<caf::actor> next) -> caf::result<void> {
      VAST_DEBUG("source execution node received atom::run");
      return self->state.start(std::move(next));
    },
    [self](caf::stream<framed<table_slice>> in, std::vector<caf::actor> next) {
      return self->state.start(in, std::move(next));
    },
    [self](caf::stream<framed<chunk_ptr>> in, std::vector<caf::actor> next) {
      return self->state.start(in, std::move(next));
    },
  };
}

} // namespace vast
