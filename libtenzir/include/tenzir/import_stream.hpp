//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/node_control.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <caf/attach_stream_source.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <deque>

namespace tenzir {

namespace detail {

struct import_source_state {
  bool stop;
  std::deque<table_slice> queue;
};

class import_source_driver final
  : public caf::stream_source_driver<
      caf::broadcast_downstream_manager<table_slice>> {
public:
  explicit import_source_driver(std::shared_ptr<import_source_state> state,
                                std::function<void(caf::error)> on_done)
    : state_{std::move(state)}, on_done_{std::move(on_done)} {
  }

  void pull(caf::downstream<table_slice>& out, size_t num) override {
    auto processed = size_t{0};
    while (processed < std::min(num, state_->queue.size())) {
      auto& slice = state_->queue[processed];
      if (slice.rows() > 0) {
        slice.import_time(time::clock::now());
        out.push(std::move(slice));
      }
      processed += 1;
    }
    state_->queue.erase(state_->queue.begin(),
                        state_->queue.begin()
                          + detail::narrow<ptrdiff_t>(processed));
  }

  auto done() const noexcept -> bool override {
    return state_->queue.empty() && state_->stop;
  }

  void finalize(const caf::error& error) override {
    if (error && error != caf::exit_reason::unreachable) {
      on_done_(error);
    } else {
      on_done_({});
    }
  }

private:
  std::shared_ptr<import_source_state> state_;
  std::function<void(caf::error)> on_done_;
};

} // namespace detail

class import_stream {
public:
  static auto make(caf::scheduled_actor& self, const node_actor& node)
    -> caf::expected<import_stream> {
    return make(&self, node);
  }

  static auto make(caf::scheduled_actor* self, const node_actor& node)
    -> caf::expected<import_stream> {
    auto blocking_self = caf::scoped_actor{self->system()};
    auto components = get_node_components<importer_actor>(blocking_self, node);
    if (not components) {
      return components.error();
    }
    auto [importer] = std::move(*components);
    return import_stream{self, importer};
  }

  import_stream(caf::scheduled_actor* self,
                stream_sink_actor<table_slice> sink) {
    self_state_ = std::make_shared<self_state>();
    source_state_ = std::make_shared<detail::import_source_state>();
    source_ = caf::detail::make_stream_source<detail::import_source_driver>(
      self, source_state_, [self_state = self_state_](caf::error err) {
        if (self_state->callback) {
          self_state->callback(std::move(err));
          self_state->observed = true;
          return;
        }
        if (self_state.unique()) {
          TENZIR_ERROR("import stream failed without check: {}", err);
          return;
        }
        self_state->result = std::move(err);
      });
    source_->add_outbound_path(sink);
    for (const auto& plugin : plugins::get<analyzer_plugin>()) {
      // We can safely assert that the analyzer was already initialized. The
      // pipeline API guarantees that remote operators run after the node was
      // successfully initialized, which implies that analyzers have been
      // initialized as well.
      auto analyzer = plugin->analyzer();
      TENZIR_ASSERT(analyzer);
      source_->add_outbound_path(analyzer);
    }
  }

  import_stream(const import_stream&) = delete;
  import_stream(import_stream&&) = default;
  auto operator=(const import_stream&) -> import_stream& = delete;
  auto operator=(import_stream&&) -> import_stream& = default;

  ~import_stream() {
    if (not self_state_ or self_state_->observed) {
      return;
    }
    finish();
    if (not self_state_->result.has_value()) {
      if (not self_state_->callback) {
        TENZIR_WARN("import stream destroyed before result is known");
      }
      return;
    }
    auto& error = *self_state_->result;
    if (error) {
      TENZIR_ERROR("import stream destroyed with unobserved error: {}", error);
    }
  }

  void enqueue(table_slice slice) {
    TENZIR_ASSERT_CHEAP(not source_state_->stop);
    source_state_->queue.push_back(std::move(slice));
  }

  auto enqueued() const -> size_t {
    return self_state_->result.has_value() ? 0 : source_state_->queue.size();
  }

  void finish(std::function<void(caf::error)> callback = {}) {
    if (not source_state_->stop) {
      source_state_->stop = true;
      // TODO: Is this okay?
      source_->generate_messages();
      source_->out().fan_out_flush();
      source_->out().force_emit_batches();
      source_->stop();
    }
    if (callback) {
      if (self_state_->result.has_value()) {
        callback(*self_state_->result);
        self_state_->observed = true;
      } else {
        self_state_->callback = std::move(callback);
      }
    }
  }

  auto has_ended() const -> bool {
    return self_state_->result.has_value();
  }

  auto error() const -> caf::error {
    if (self_state_->result.has_value()) {
      self_state_->observed = true;
      return *self_state_->result;
    }
    return {};
  }

private:
  struct self_state {
    bool observed = false;
    std::optional<caf::error> result;
    std::function<void(caf::error)> callback;
  };

  detail::import_source_driver::source_ptr_type source_;
  std::shared_ptr<detail::import_source_state> source_state_;
  std::shared_ptr<self_state> self_state_;
};

} // namespace tenzir
