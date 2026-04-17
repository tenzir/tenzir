//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/node.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/shared_diagnostic_handler.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/util/byte_size.h>
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <algorithm>
#include <numeric>
#include <utility>

namespace tenzir::plugins::cache {

// NOTE: The sort order of the state is relied upon for cache eviction.
TENZIR_ENUM(cache_state, failed, closed, open);

namespace {

struct cache_update {
  uint64_t approx_bytes = {};
  cache_state state = {};
};

struct cache_actor_traits {
  using signatures = caf::type_list<
    // Announce the sender as writer and monitor it for liveness.
    auto(atom::announce)->caf::result<void>,
    // Announce with explicit monitor flag. Pass false when the sender is a
    // short-lived companion actor (new executor) to avoid premature
    // mark_done when the companion is destroyed.
    auto(atom::announce, bool monitor)->caf::result<void>,
    // Check if the cache already has a writer.
    auto(atom::write, atom::ok)->caf::result<bool>,
    // Write events into the cache.
    auto(atom::write, table_slice events)->caf::result<bool>,
    // Signal that writing is complete (replaces monitor-based detection).
    auto(atom::write, atom::done)->caf::result<void>,
    // Read events from the cache.
    auto(atom::read)->caf::result<table_slice>,
    // Positional read (does not require persistent reader identity).
    auto(atom::read, uint64_t offset)->caf::result<table_slice>>;
};

using cache_actor = caf::typed_actor<cache_actor_traits>;

class cache {
public:
  [[maybe_unused]] static constexpr auto name = "cache";

  cache(cache_actor::pointer self, shared_diagnostic_handler diagnostics,
        located<uint64_t> max_events, uint64_t max_bytes, duration read_timeout,
        duration write_timeout,
        caf::async::producer_resource<cache_update> update_producer)
    : self_{self},
      diagnostics_{std::move(diagnostics)},
      max_events_{max_events},
      max_bytes_{max_bytes},
      update_multicaster_{self_},
      read_timeout_{read_timeout},
      write_timeout_{write_timeout} {
    update_multicaster_
      .as_observable()
      // We report the first value immediately...
      .take(1)
      .merge(
        // ... followed by one every second...
        update_multicaster_.as_observable().sample(std::chrono::seconds{1}),
        // ... and then end with the last value.
        update_multicaster_.as_observable().take_last(1))
      .subscribe(std::move(update_producer));
  }

  ~cache() noexcept {
    TENZIR_DEBUG("cache actor destroyed, done_={}", done_);
    for (auto& [_, reader] : readers_) {
      reader.rp.deliver({});
    }
  }

  auto make_behavior() -> cache_actor::behavior_type {
    return {
      [this](atom::announce) -> caf::result<void> {
        return announce(true);
      },
      [this](atom::announce, bool monitor) -> caf::result<void> {
        return announce(monitor);
      },
      [this](atom::write, atom::ok) -> caf::result<bool> {
        return write_ok();
      },
      [this](atom::write, table_slice events) -> caf::result<bool> {
        return write(std::move(events));
      },
      [this](atom::write, atom::done) -> caf::result<void> {
        return write_done();
      },
      [this](atom::read) -> caf::result<table_slice> {
        return read();
      },
      [this](atom::read, uint64_t offset) -> caf::result<table_slice> {
        return read_at(offset);
      },
    };
  }

private:
  auto reset_read_timeout() -> void {
    TENZIR_ASSERT(read_timeout_ > duration::zero());
    on_read_timeout_.dispose();
    on_read_timeout_ = self_->run_delayed_weak(read_timeout_, [this] {
      TENZIR_DEBUG("cache: read_timeout expired, quitting");
      self_->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto set_write_timeout() -> void {
    if (write_timeout_ == duration::zero()) {
      return;
    }
    TENZIR_ASSERT(write_timeout_ > duration::zero());
    on_write_timeout_.dispose();
    on_write_timeout_ = self_->run_delayed_weak(write_timeout_, [this] {
      TENZIR_DEBUG("cache: write_timeout expired, quitting");
      self_->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto mark_done(cache_state state) -> void {
    TENZIR_DEBUG("cache: mark_done({})", state);
    TENZIR_ASSERT(not done_);
    done_ = true;
    update_multicaster_.push(cache_update{
      .approx_bytes = byte_size_,
      .state = state,
    });
    update_multicaster_.close();
    reset_read_timeout();
    // We ignore error messages because they do not matter to the readers.
    for (auto& [_, reader] : readers_) {
      if (reader.offset == cached_events_.size() and reader.rp.pending()) {
        reader.rp.deliver(table_slice{});
      }
    }
    // Deliver to pending positional reads at end-of-data.
    for (auto& [offset, rps] : pending_positional_reads_) {
      if (offset == cached_events_.size()) {
        for (auto& rp : rps) {
          if (rp.pending()) {
            rp.deliver(table_slice{});
          }
        }
      }
    }
    pending_positional_reads_.clear();
  }

  auto announce(bool monitor) -> caf::result<void> {
    TENZIR_DEBUG("cache: announce(monitor={})", monitor);
    if (not writer_) {
      const auto sender = self_->current_sender();
      writer_ = sender->address();
      // Async executor requests use short-lived companion actors, so sender
      // identity is only stable when we also monitor the writer actor.
      enforce_writer_identity_ = monitor;
      if (monitor) {
        self_->monitor(sender, [this](const caf::error& err) {
          if (done_) {
            return;
          }
          mark_done(err.valid() ? cache_state::failed : cache_state::closed);
        });
      }
      set_write_timeout();
    }
    return {};
  }

  auto write_ok() -> caf::result<bool> {
    return writer_;
  }

  auto write(table_slice events) -> caf::result<bool> {
    if (not writer_) {
      return diagnostic::error("cache has no writer").to_error();
    }
    if (enforce_writer_identity_) {
      const auto sender = self_->current_sender();
      if (not sender) {
        return diagnostic::error("cache write request has no sender").to_error();
      }
      if (sender->address() != writer_) {
        return diagnostic::error("cache write request from non-writer actor")
          .to_error();
      }
    }
    TENZIR_ASSERT(events.rows() > 0);
    auto exceeded_capacity = false;
    if (cache_size_ + events.rows() > max_events_.inner) {
      events = head(std::move(events), max_events_.inner - cache_size_);
      diagnostic::warning("cache exceeded capacity of {} events",
                          max_events_.inner)
        .primary(max_events_.source)
        .emit(diagnostics_);
      exceeded_capacity = true;
      if (events.rows() == 0) {
        return false;
      }
    }
    const auto approx_bytes = events.approx_bytes();
    // If a single cache exceeds the total capacity, we stop short of adding the
    // batch of events that'd make it go over the limit. This is better than
    // being evicted immediately.
    if (byte_size_ + approx_bytes > max_bytes_ and not exceeded_capacity) {
      diagnostic::warning("cache exceeded total capacity of {} MiB",
                          max_bytes_ / (1 << 20))
        .hint("consider increasing `tenzir.cache.capacity` option")
        .emit(diagnostics_);
      return false;
    }
    cache_size_ += events.rows();
    byte_size_ += approx_bytes;
    update_multicaster_.push(cache_update{
      .approx_bytes = byte_size_,
      .state = cache_state::open,
    });
    cached_events_.push_back(std::move(events));
    for (auto& [_, reader] : readers_) {
      if (not reader.rp.pending()) {
        TENZIR_ASSERT(reader.offset < cached_events_.size());
        continue;
      }
      reader.rp.deliver(cached_events_[reader.offset++]);
      TENZIR_ASSERT(reader.offset == cached_events_.size());
    }
    // Fulfill pending positional reads whose offset is now available.
    auto fulfilled = std::vector<uint64_t>{};
    for (auto& [offset, rps] : pending_positional_reads_) {
      if (offset < cached_events_.size()) {
        for (auto& rp : rps) {
          if (rp.pending()) {
            rp.deliver(cached_events_[offset]);
          }
        }
        fulfilled.push_back(offset);
      }
    }
    for (auto offset : fulfilled) {
      pending_positional_reads_.erase(offset);
    }
    return not exceeded_capacity;
  }

  auto write_done() -> caf::result<void> {
    if (not writer_) {
      return diagnostic::error("cache has no writer").to_error();
    }
    if (enforce_writer_identity_) {
      const auto sender = self_->current_sender();
      if (not sender) {
        return diagnostic::error("cache write request has no sender").to_error();
      }
      if (sender->address() != writer_) {
        return diagnostic::error("cache write request from non-writer actor")
          .to_error();
      }
    }
    if (not done_) {
      mark_done(cache_state::closed);
    }
    return {};
  }

  auto read_at(uint64_t offset) -> caf::result<table_slice> {
    if (done_) {
      reset_read_timeout();
    }
    if (offset < cached_events_.size()) {
      return cached_events_[offset];
    }
    if (offset == cached_events_.size() and done_) {
      return table_slice{};
    }
    // Data not available yet; store a pending promise.
    auto rp = self_->make_response_promise<table_slice>();
    pending_positional_reads_[offset].push_back(rp);
    return rp;
  }

  auto read() -> caf::result<table_slice> {
    if (done_) {
      reset_read_timeout();
    }
    const auto sender = self_->current_sender();
    TENZIR_ASSERT(sender);
    auto& reader = readers_[sender->address()];
    TENZIR_ASSERT(not reader.rp.pending());
    TENZIR_ASSERT(reader.offset <= cached_events_.size());
    if (reader.offset == 0) {
      self_->monitor(sender, [this, sender](const caf::error&) {
        const auto erased = readers_.erase(sender);
        TENZIR_ASSERT(erased == 1);
      });
    }
    if (reader.offset == cached_events_.size()) {
      if (done_) {
        return table_slice{};
      }
      reader.rp = self_->make_response_promise<table_slice>();
      return reader.rp;
    }
    return cached_events_[reader.offset++];
  }

  struct reader {
    size_t offset = {};
    caf::typed_response_promise<table_slice> rp;
  };

  cache_actor::pointer self_ = {};

  shared_diagnostic_handler diagnostics_;

  located<uint64_t> max_events_;
  uint64_t cache_size_ = {};
  std::vector<table_slice> cached_events_;

  uint64_t byte_size_ = {};
  const uint64_t max_bytes_;
  caf::flow::multicaster<cache_update> update_multicaster_;

  caf::actor_addr writer_;
  bool enforce_writer_identity_ = true;
  bool done_ = {};
  detail::flat_map<caf::actor_addr, reader> readers_;
  detail::flat_map<uint64_t,
                   std::vector<caf::typed_response_promise<table_slice>>>
    pending_positional_reads_;

  duration read_timeout_ = {};
  duration write_timeout_ = {};
  caf::disposable on_read_timeout_;
  caf::disposable on_write_timeout_;
};

struct cache_manager_actor_traits {
  using signatures = caf::type_list<
    // Get the cache.
    auto(atom::get, std::string id, bool exclusive)->caf::result<caf::actor>,
    // Create the cache if it does not already exist.
    auto(atom::create, std::string id, bool exclusive,
         shared_diagnostic_handler diagnostics, uint64_t capacity,
         location capacity_loc, duration read_timeout, duration write_timeout)
      ->caf::result<caf::actor>>::append_from<component_plugin_actor::signatures>;
};

using cache_manager_actor = caf::typed_actor<cache_manager_actor_traits>;

class cache_manager {
public:
  [[maybe_unused]] static constexpr auto name = "cache-manager";

  explicit cache_manager(cache_manager_actor::pointer self, uint64_t max_events)
    : self_{self}, max_bytes_{max_events} {
  }

  auto make_behavior() -> cache_manager_actor::behavior_type {
    // Every 30 seconds, we check the total size of all caches, and evict the
    // oldest if we've gone over the limit.
    detail::weak_run_delayed_loop(self_, std::chrono::seconds{30}, [this] {
      check_total_size();
    });
    return {
      [this](atom::get, std::string id,
             bool exclusive) -> caf::result<caf::actor> {
        return get(std::move(id), exclusive);
      },
      [this](atom::create, std::string id, bool exclusive,
             shared_diagnostic_handler diagnostics, uint64_t capacity,
             location capacity_loc, duration read_timeout,
             duration write_timeout) -> caf::result<caf::actor> {
        return create(std::move(id), exclusive, std::move(diagnostics),
                      {capacity, capacity_loc}, read_timeout, write_timeout);
      },
      [](atom::status, status_verbosity, duration) -> caf::result<record> {
        return {};
      },
    };
  }

private:
  auto check_exclusive(const cache_actor& cache, bool exclusive) const
    -> caf::result<caf::actor> {
    TENZIR_ASSERT(cache);
    auto handle = caf::actor_cast<caf::actor>(cache);
    if (not exclusive) {
      return handle;
    }
    auto rp = self_->make_response_promise<caf::actor>();
    self_->mail(atom::write_v, atom::ok_v)
      .request(cache, caf::infinite)
      .then(
        [rp, handle = std::move(handle)](bool has_writer) mutable {
          rp.deliver(has_writer ? caf::actor{} : std::move(handle));
        },
        [rp](const caf::error& err) mutable {
          rp.deliver(diagnostic::error(err)
                       .note("failed to check for cache write exclusivity")
                       .to_error());
        });
    return rp;
  }

  auto get(std::string id, bool exclusive) -> caf::result<caf::actor> {
    const auto it = caches_.find(id);
    if (it == caches_.end()) {
      return diagnostic::error("cache `{}` does not exist", id).to_error();
    }
    return check_exclusive(it->second.handle, exclusive);
  }

  auto
  create(std::string id, bool exclusive, shared_diagnostic_handler diagnostics,
         located<uint64_t> max_events, duration read_timeout,
         duration write_timeout) -> caf::result<caf::actor> {
    const auto it = caches_.find(id);
    if (it == caches_.end()) {
      auto [byte_size_consumer, byte_size_producer]
        = caf::async::make_spsc_buffer_resource<cache_update>();
      self_->make_observable()
        .from_resource(std::move(byte_size_consumer))
        .for_each([this, id](cache_update update) {
          const auto it = caches_.find(id);
          TENZIR_ASSERT(it != caches_.end());
          it->second.update = update;
        });
      auto handle
        = self_->spawn(caf::actor_from_state<cache>, std::move(diagnostics),
                       max_events, max_bytes_, read_timeout, write_timeout,
                       std::move(byte_size_producer));
      auto monitor
        = self_->monitor(handle, [this, id](const caf::error&) mutable {
            const auto it = caches_.find(id);
            TENZIR_ASSERT(it != caches_.end());
            caches_.erase(it);
          });
      return caf::actor_cast<caf::actor>(
        caches_
          .try_emplace(it, std::move(id), std::move(handle), std::move(monitor))
          ->second.handle);
    }
    return check_exclusive(it->second.handle, exclusive);
  }

  auto check_total_size() -> void {
    // Check the total size of the buffers, and if we're over the limit, expire
    // the oldest to make room for new ones.
    auto total
      = std::transform_reduce(caches_.begin(), caches_.end(), uint64_t{},
                              std::plus{}, [](const auto& cache) {
                                return cache.second.update.approx_bytes;
                              });
    while (total > max_bytes_) {
      const auto oldest = std::ranges::min_element(
        caches_, std::ranges::less{}, [](const auto& cache) {
          return cache.second.created_at;
        });
      TENZIR_ASSERT(oldest != caches_.end());
      TENZIR_DEBUG("{} rotates cache `{}` after exceeding capacity of {} to "
                   "reduce total cache size from {} to {}",
                   *self_, oldest->first, oldest->second.handle, max_bytes_,
                   total, total - oldest->second.update.approx_bytes);
      total -= oldest->second.update.approx_bytes;
      oldest->second.monitor.dispose();
      self_->send_exit(oldest->second.handle,
                       diagnostic::error("cache rotated").to_error());
      caches_.erase(oldest);
    }
  }

  struct managed_cache {
    managed_cache(const managed_cache&) = delete;
    managed_cache(managed_cache&&) = delete;
    auto operator=(const managed_cache&) -> managed_cache& = delete;
    auto operator=(managed_cache&&) -> managed_cache& = delete;
    managed_cache(cache_actor handle, caf::disposable monitor)
      : handle{std::move(handle)}, monitor{std::move(monitor)} {
    }

    ~managed_cache() {
      caf::anon_send_exit(handle, caf::exit_reason::user_shutdown);
    }

    cache_actor handle;
    caf::disposable monitor;
    cache_update update = {};
    std::chrono::steady_clock::time_point created_at
      = std::chrono::steady_clock::now();

    auto operator<(const managed_cache& other) const -> bool {
      return std::tie(update.state, created_at, handle)
             < std::tie(other.update.state, other.created_at, other.handle);
    }
  };

  cache_manager_actor::pointer self_ = {};
  const uint64_t max_bytes_ = {};
  std::unordered_map<std::string, managed_cache> caches_;
};

class write_cache_operator final : public operator_base {
public:
  write_cache_operator() = default;

  explicit write_cache_operator(located<std::string> id,
                                located<uint64_t> max_events,
                                duration read_timeout, duration write_timeout)
    : id_{std::move(id)},
      sink_{true},
      max_events_{max_events},
      read_timeout_{read_timeout},
      write_timeout_{write_timeout} {
  }

  explicit write_cache_operator(located<std::string> id) : id_{std::move(id)} {
  }

  template <class Output>
  auto run(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<Output> {
    const auto cache_manager
      = ctrl.self().system().registry().get<cache_manager_actor>(
        "tenzir.cache-manager");
    TENZIR_ASSERT(cache_manager);
    auto cache = cache_actor{};
    if (sink_) {
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::create_v, id_.inner,
              /* exclusive */ true, ctrl.shared_diagnostics(),
              max_events_.inner, max_events_.source, read_timeout_,
              write_timeout_)
        .request(cache_manager, caf::infinite)
        .then(
          [&](caf::actor& handle) {
            if (not handle) {
              diagnostic::error("cache already exists")
                .primary(id_.source)
                .emit(ctrl.diagnostics());
              return;
            }
            cache = caf::actor_cast<cache_actor>(std::move(handle));
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to retrieve cache")
              .primary(id_.source)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    } else {
      // We intentionally use a blocking actor here as we must be able to return
      // if we do not have exclusive write access to the cache before yielding
      // to avoid upstream operators starting up in the first place.
      auto blocking_self = caf::scoped_actor{ctrl.self().system()};
      blocking_self
        ->mail(atom::get_v, id_.inner,
               /* exclusive */ true)
        .request(cache_manager, caf::infinite)
        .receive(
          [&](caf::actor& handle) {
            cache = caf::actor_cast<cache_actor>(std::move(handle));
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to retrieve cache")
              .primary(id_.source)
              .emit(ctrl.diagnostics());
          });
      if (not cache) {
        co_return;
      }
      co_yield {};
    }
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::announce_v)
      .request(cache, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](caf::error& err) {
          diagnostic::error(err)
            .note("failed to announce write-cache operator to cache")
            .primary(id_.source)
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    // Now, all we need to do is send our inputs to the cache batch by batch.
    for (auto&& events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      ctrl.set_waiting(true);
      auto accepted = false;
      ctrl.self()
        .mail(atom::write_v, std::move(events))
        .request(cache, caf::infinite)
        .then(
          [&](bool result) {
            accepted = result;
            ctrl.set_waiting(false);
          },
          [&](caf::error& err) {
            diagnostic::error(err)
              .note("failed to write to cache")
              .primary(id_.source)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      if (not accepted) {
        co_return;
      }
    }
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    auto* typed_input = std::get_if<generator<table_slice>>(&input);
    TENZIR_ASSERT(typed_input);
    if (sink_) {
      return run<std::monostate>(std::move(*typed_input), ctrl);
    }
    return run<table_slice>(std::move(*typed_input), ctrl);
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<write_cache_operator>(*this);
  };

  auto name() const -> std::string override {
    return "write_cache";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    TENZIR_UNUSED(order);
    return do_not_optimize(*this);
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<table_slice>()) {
      if (sink_) {
        return tag_v<void>;
      }
      return tag_v<table_slice>;
    }
    return diagnostic::error("`cache` does not accept {} as input",
                             operator_type_name(input))
      .to_error();
  }

  friend auto inspect(auto& f, write_cache_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_), f.field("sink", x.sink_),
                              f.field("max_events", x.max_events_),
                              f.field("read_timeout", x.read_timeout_),
                              f.field("write_timeout", x.write_timeout_));
  }

private:
  located<std::string> id_;
  bool sink_ = {};
  located<uint64_t> max_events_;
  duration read_timeout_ = {};
  duration write_timeout_ = {};
};

class read_cache_operator final : public crtp_operator<read_cache_operator> {
public:
  read_cache_operator() = default;

  explicit read_cache_operator(located<std::string> id)
    : id_{std::move(id)}, source_{true} {
  }

  explicit read_cache_operator(located<std::string> id,
                               located<uint64_t> max_events,
                               duration read_timeout, duration write_timeout)
    : id_{std::move(id)},
      max_events_{max_events},
      read_timeout_{read_timeout},
      write_timeout_{write_timeout} {
  }

  auto run(std::optional<generator<table_slice>> input,
           operator_control_plane& ctrl) const -> generator<table_slice> {
    TENZIR_ASSERT(source_ != input.has_value());
    const auto cache_manager
      = ctrl.self().system().registry().get<cache_manager_actor>(
        "tenzir.cache-manager");
    TENZIR_ASSERT(cache_manager);
    auto cache = cache_actor{};
    ctrl.set_waiting(true);
    (source_ ? ctrl.self()
                 .mail(atom::get_v, id_.inner, /* exclusive */ false)
                 .request(cache_manager, caf::infinite)
             : ctrl.self()
                 .mail(atom::create_v, id_.inner, /* exclusive */ false,
                       ctrl.shared_diagnostics(), max_events_.inner,
                       max_events_.source, read_timeout_, write_timeout_)
                 .request(cache_manager, caf::infinite))
      .then(
        [&](caf::actor& handle) {
          cache = caf::actor_cast<cache_actor>(std::move(handle));
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to retrieve cache")
            .primary(id_.source)
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    TENZIR_ASSERT(cache);
    // Now, we can get batch by batch from the cache.
    while (true) {
      if (input) {
        auto it = input->unsafe_current();
        if (it != input->end()) {
          TENZIR_ASSERT((*it).rows() == 0);
          ++it;
        }
      }
      auto events = table_slice{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::read_v)
        .request(cache, caf::infinite)
        .then(
          [&](table_slice& response) {
            ctrl.set_waiting(false);
            events = std::move(response);
          },
          [&](caf::error& err) {
            diagnostic::error(err)
              .note("failed to read from cache")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
      if (events.rows() == 0) {
        co_return;
      }
      co_yield std::move(events);
    }
    TENZIR_UNREACHABLE();
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    return run({}, ctrl);
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    input.begin();
    return run(std::move(input), ctrl);
  }

  auto name() const -> std::string override {
    return "read_cache";
  }

  auto idle_after() const -> duration override {
    // We only send stub events between the two operators to break the back
    // pressure and instead use a side channel for transporting events, hence
    // the need to schedule the reading side independently of receiving input if
    // we're not a source.
    return source_ ? duration::zero() : duration::max();
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    TENZIR_UNUSED(order);
    return do_not_optimize(*this);
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (source_) {
      if (input.is<void>()) {
        return tag_v<table_slice>;
      }
    } else if (input.is<table_slice>()) {
      return tag_v<table_slice>;
    }
    return diagnostic::error("`cache` does not accept {} as input",
                             operator_type_name(input))
      .to_error();
  }

  friend auto inspect(auto& f, read_cache_operator& x) -> bool {
    return f.object(x).fields(f.field("id", x.id_),
                              f.field("source", x.source_),
                              f.field("max_events", x.max_events_),
                              f.field("read_timeout", x.read_timeout_),
                              f.field("write_timeout", x.write_timeout_));
  }

private:
  located<std::string> id_;
  bool source_ = {};
  located<uint64_t> max_events_;
  duration read_timeout_ = {};
  duration write_timeout_ = {};
};

// -- new async executor operators ---------------------------------------------

struct CacheArgs {
  std::string id;
  std::string mode = "readwrite";
  std::optional<located<uint64_t>> capacity;
  std::optional<located<duration>> read_timeout;
  std::optional<located<duration>> write_timeout;
};

class WriteCacheSink final : public Operator<table_slice, void> {
public:
  explicit WriteCacheSink(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_DEBUG("WriteCacheSink: entering start(), id='{}', "
                 "has_read_timeout={}, has_write_timeout={}, has_capacity={}",
                 args_.id, args_.read_timeout.has_value(),
                 args_.write_timeout.has_value(), args_.capacity.has_value());
    co_await OperatorBase::start(ctx);
    auto cache_manager = ctx.actor_system().registry().get<cache_manager_actor>(
      "tenzir.cache-manager");
    TENZIR_DEBUG("WriteCacheSink: cache_manager={}", bool{cache_manager});
    TENZIR_ASSERT(cache_manager);
    auto capacity = args_.capacity ? args_.capacity->inner
                                   : std::numeric_limits<uint64_t>::max();
    auto capacity_loc
      = args_.capacity ? args_.capacity->source : location::unknown;
    TENZIR_ASSERT(args_.read_timeout);
    auto read_timeout = args_.read_timeout->inner;
    auto write_timeout
      = args_.write_timeout ? args_.write_timeout->inner : duration::zero();
    TENZIR_DEBUG("WriteCacheSink: creating cache '{}'", args_.id);
    auto result
      = co_await async_mail(atom::create_v, args_.id, /*exclusive=*/true,
                            shared_diagnostic_handler{}, capacity, capacity_loc,
                            read_timeout, write_timeout)
          .request(cache_manager);
    if (not result) {
      done_ = true;
      diagnostic::error(result.error()).note("failed to create cache").emit(ctx);
      co_return;
    }
    if (not *result) {
      done_ = true;
      diagnostic::error("cache `{}` already has a writer", args_.id).emit(ctx);
      co_return;
    }
    cache_ = caf::actor_cast<cache_actor>(*result);
    TENZIR_DEBUG("WriteCacheSink: start() complete");
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    if (not announced_) {
      // Pass monitor=false because the sender is a short-lived companion actor
      // that would trigger premature mark_done when destroyed.
      auto announce_result
        = co_await async_mail(atom::announce_v, false).request(cache_);
      if (not announce_result) {
        diagnostic::error(announce_result.error())
          .note("failed to announce write-cache operator to cache")
          .emit(ctx);
        co_return;
      }
      announced_ = true;
    }
    TENZIR_DEBUG("WriteCacheSink: writing {} events", input.rows());
    auto result
      = co_await async_mail(atom::write_v, std::move(input)).request(cache_);
    if (not result) {
      diagnostic::error(result.error())
        .note("failed to write to cache")
        .emit(ctx);
      co_return;
    }
    if (not *result) {
      done_ = true;
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    // `start()` can fail before `cache_` is assigned; in that case we already
    // emitted diagnostics and must not send requests to an invalid actor.
    if (not cache_) {
      co_return FinalizeBehavior::done;
    }
    if (not announced_) {
      auto announce_result
        = co_await async_mail(atom::announce_v, false).request(cache_);
      if (not announce_result) {
        diagnostic::error(announce_result.error())
          .note("failed to announce write-cache operator to cache")
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      announced_ = true;
    }
    auto result
      = co_await async_mail(atom::write_v, atom::done_v).request(cache_);
    if (not result) {
      diagnostic::error(result.error())
        .note("failed to finalize cache write")
        .emit(ctx);
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    if (done_) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

private:
  CacheArgs args_;
  cache_actor cache_;
  bool done_ = false;
  bool announced_ = false;
};

class ReadCacheSource final : public Operator<void, table_slice> {
public:
  explicit ReadCacheSource(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    auto cache_manager = ctx.actor_system().registry().get<cache_manager_actor>(
      "tenzir.cache-manager");
    TENZIR_ASSERT(cache_manager);
    auto result
      = co_await async_mail(atom::get_v, args_.id, /*exclusive=*/false)
          .request(cache_manager);
    if (not result) {
      // TODO: Errors emitted from start should trigger the Runner to abort.
      done_ = true;
      diagnostic::error(result.error())
        .note("failed to retrieve cache")
        .emit(ctx);
      co_return;
    }
    cache_ = caf::actor_cast<cache_actor>(*result);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    auto result
      = co_await async_mail(atom::read_v, read_offset_).request(cache_);
    co_return std::move(result);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto& expected = result.as<caf::expected<table_slice>>();
    if (not expected) {
      diagnostic::error(expected.error())
        .note("failed to read from cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (expected->rows() == 0) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(*expected));
    ++read_offset_;
  }

  auto state() -> OperatorState override {
    if (done_) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

private:
  CacheArgs args_;
  cache_actor cache_;
  mutable uint64_t read_offset_ = 0;
  mutable bool done_ = false;
};

class CacheReadwrite final : public Operator<table_slice, table_slice> {
public:
  explicit CacheReadwrite(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    auto cache_manager = ctx.actor_system().registry().get<cache_manager_actor>(
      "tenzir.cache-manager");
    TENZIR_ASSERT(cache_manager);
    auto capacity = args_.capacity ? args_.capacity->inner
                                   : std::numeric_limits<uint64_t>::max();
    auto capacity_loc
      = args_.capacity ? args_.capacity->source : location::unknown;
    TENZIR_ASSERT(args_.read_timeout);
    auto read_timeout = args_.read_timeout->inner;
    auto write_timeout
      = args_.write_timeout ? args_.write_timeout->inner : duration::zero();
    auto result
      = co_await async_mail(atom::create_v, args_.id, /*exclusive=*/true,
                            shared_diagnostic_handler{}, capacity, capacity_loc,
                            read_timeout, write_timeout)
          .request(cache_manager);
    if (not result) {
      diagnostic::error(result.error()).note("failed to create cache").emit(ctx);
      co_return;
    }
    if (not *result) {
      diagnostic::error("cache `{}` already has a writer", args_.id).emit(ctx);
      co_return;
    }
    cache_ = caf::actor_cast<cache_actor>(*result);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (input.rows() == 0) {
      co_return;
    }
    if (not announced_) {
      // Pass monitor=false because the sender is a short-lived companion actor
      // that would trigger premature mark_done when destroyed.
      auto announce_result
        = co_await async_mail(atom::announce_v, false).request(cache_);
      if (not announce_result) {
        diagnostic::error(announce_result.error())
          .note("failed to announce cache operator")
          .emit(ctx);
        co_return;
      }
      announced_ = true;
    }
    auto result
      = co_await async_mail(atom::write_v, std::move(input)).request(cache_);
    if (not result) {
      diagnostic::error(result.error())
        .note("failed to write to cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (not *result) {
      done_ = true;
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    auto result
      = co_await async_mail(atom::read_v, read_offset_).request(cache_);
    co_return std::move(result);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (finalized_) {
      co_return;
    }
    auto& expected = result.as<caf::expected<table_slice>>();
    if (not expected) {
      diagnostic::error(expected.error())
        .note("failed to read from cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (expected->rows() == 0) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(*expected));
    ++read_offset_;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    finalized_ = true;
    if (not announced_) {
      auto announce_result
        = co_await async_mail(atom::announce_v, false).request(cache_);
      if (not announce_result) {
        diagnostic::error(announce_result.error())
          .note("failed to announce cache operator")
          .emit(ctx);
        done_ = true;
        co_return FinalizeBehavior::done;
      }
      announced_ = true;
    }
    auto write_result
      = co_await async_mail(atom::write_v, atom::done_v).request(cache_);
    if (not write_result) {
      diagnostic::error(write_result.error())
        .note("failed to finalize cache write")
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    // Drain remaining data from the cache.
    while (true) {
      auto result
        = co_await async_mail(atom::read_v, read_offset_).request(cache_);
      if (not result) {
        diagnostic::error(result.error())
          .note("failed to drain cache")
          .emit(ctx);
        break;
      }
      if (result->rows() == 0) {
        break;
      }
      co_await push(std::move(*result));
      ++read_offset_;
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    if (done_) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

private:
  CacheArgs args_;
  cache_actor cache_;
  mutable uint64_t read_offset_ = 0;
  mutable bool done_ = false;
  bool finalized_ = false;
  bool announced_ = false;
};

// -- IR operator and compiler plugin ------------------------------------------

class CacheIr final : public ir::Operator {
public:
  CacheIr() = default;

  CacheIr(location op_loc, ast::expression id,
          std::optional<ast::expression> mode,
          std::optional<ast::expression> capacity,
          std::optional<ast::expression> read_timeout,
          std::optional<ast::expression> write_timeout,
          duration default_read_timeout)
    : op_loc_{op_loc},
      id_{std::move(id)},
      mode_{std::move(mode)},
      capacity_{std::move(capacity)},
      read_timeout_{std::move(read_timeout)},
      write_timeout_{std::move(write_timeout)},
      default_read_timeout_{default_read_timeout} {
  }

  auto name() const -> std::string override {
    return "cache_ir";
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (not mode_value_) {
      // Mode not yet resolved; defer type inference.
      return std::optional<element_type_tag>{};
    }
    if (*mode_value_ == "write") {
      if (input.is_not<table_slice>()) {
        diagnostic::error("`cache --mode write` expects events as input")
          .primary(op_loc_)
          .emit(dh);
        return failure::promise();
      }
      return tag_v<void>;
    }
    if (*mode_value_ == "read") {
      if (input.is_not<void>()) {
        diagnostic::error("`cache --mode read` must be used as a source")
          .primary(op_loc_)
          .emit(dh);
        return failure::promise();
      }
      return tag_v<table_slice>;
    }
    if (*mode_value_ == "readwrite") {
      if (input.is_not<table_slice>()) {
        diagnostic::error("`cache` expects events as input")
          .primary(op_loc_)
          .emit(dh);
        return failure::promise();
      }
      return tag_v<table_slice>;
    }
    diagnostic::error("unknown cache mode `{}`", *mode_value_)
      .primary(op_loc_)
      .emit(dh);
    return failure::promise();
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(id_.substitute(ctx));
    if (mode_) {
      TRY(mode_->substitute(ctx));
    }
    if (capacity_) {
      TRY(capacity_->substitute(ctx));
    }
    if (read_timeout_) {
      TRY(read_timeout_->substitute(ctx));
    }
    if (write_timeout_) {
      TRY(write_timeout_->substitute(ctx));
    }
    // Resolve mode.
    if (mode_) {
      if (instantiate or mode_->is_deterministic(ctx)) {
        TRY(auto value, const_eval(*mode_, ctx));
        auto* str = try_as<std::string>(value);
        if (not str) {
          diagnostic::error("expected `string` for mode argument")
            .primary(*mode_)
            .emit(ctx);
          return failure::promise();
        }
        if (*str != "read" and *str != "write" and *str != "readwrite") {
          diagnostic::error("unknown mode `{}`", *str)
            .note("available modes: read, write, readwrite")
            .primary(*mode_)
            .emit(ctx);
          return failure::promise();
        }
        mode_value_ = std::move(*str);
      }
    } else {
      mode_value_ = "readwrite";
    }
    if (not instantiate) {
      return {};
    }
    // Resolve remaining arguments to concrete values.
    {
      TRY(auto value, const_eval(id_, ctx));
      auto* str = try_as<std::string>(value);
      if (not str) {
        diagnostic::error("expected `string` for cache id")
          .primary(id_)
          .emit(ctx);
        return failure::promise();
      }
      id_value_ = std::move(*str);
    }
    if (capacity_) {
      TRY(auto value, const_eval(*capacity_, ctx));
      if (auto* val = try_as<uint64_t>(value)) {
        capacity_value_ = located<uint64_t>{*val, capacity_->get_location()};
      } else if (auto* val = try_as<int64_t>(value)) {
        if (*val < 0) {
          diagnostic::error("capacity must not be negative")
            .primary(*capacity_)
            .emit(ctx);
          return failure::promise();
        }
        capacity_value_ = located<uint64_t>{static_cast<uint64_t>(*val),
                                            capacity_->get_location()};
      } else {
        diagnostic::error("expected `uint64` for capacity")
          .primary(*capacity_)
          .emit(ctx);
        return failure::promise();
      }
    }
    if (read_timeout_) {
      TRY(auto value, const_eval(*read_timeout_, ctx));
      auto* val = try_as<duration>(value);
      if (not val) {
        diagnostic::error("expected `duration` for read_timeout")
          .primary(*read_timeout_)
          .emit(ctx);
        return failure::promise();
      }
      if (*val <= duration::zero()) {
        diagnostic::error("read_timeout must be greater than zero")
          .primary(*read_timeout_)
          .emit(ctx);
        return failure::promise();
      }
      read_timeout_value_
        = located<duration>{*val, read_timeout_->get_location()};
    }
    if (write_timeout_) {
      TRY(auto value, const_eval(*write_timeout_, ctx));
      auto* val = try_as<duration>(value);
      if (not val) {
        diagnostic::error("expected `duration` for write_timeout")
          .primary(*write_timeout_)
          .emit(ctx);
        return failure::promise();
      }
      if (*val <= duration::zero()) {
        diagnostic::error("write_timeout must be greater than zero")
          .primary(*write_timeout_)
          .emit(ctx);
        return failure::promise();
      }
      write_timeout_value_
        = located<duration>{*val, write_timeout_->get_location()};
    }
    return {};
  }

  auto references(let_id id) const -> bool override {
    return ast::references(id_, id) or (mode_ and ast::references(*mode_, id))
           or (capacity_ and ast::references(*capacity_, id))
           or (read_timeout_ and ast::references(*read_timeout_, id))
           or (write_timeout_ and ast::references(*write_timeout_, id));
  }

  auto spawn(element_type_tag input) and -> AnyOperator override {
    TENZIR_ASSERT(mode_value_);
    TENZIR_ASSERT(id_value_);
    auto args = CacheArgs{};
    args.id = std::move(*id_value_);
    args.mode = *mode_value_;
    args.capacity = capacity_value_;
    args.read_timeout
      = read_timeout_value_
          ? read_timeout_value_
          : located<duration>{default_read_timeout_, location::unknown};
    args.write_timeout = write_timeout_value_;
    if (*mode_value_ == "write") {
      TENZIR_ASSERT(input.is<table_slice>());
      return WriteCacheSink{std::move(args)};
    }
    if (*mode_value_ == "read") {
      TENZIR_ASSERT(input.is<void>());
      return ReadCacheSource{std::move(args)};
    }
    TENZIR_ASSERT(*mode_value_ == "readwrite");
    TENZIR_ASSERT(input.is<table_slice>());
    return CacheReadwrite{std::move(args)};
  }

  auto main_location() const -> location override {
    return op_loc_;
  }

  friend auto inspect(auto& f, CacheIr& x) -> bool {
    return f.object(x).fields(
      f.field("op_loc", x.op_loc_), f.field("id", x.id_),
      f.field("mode", x.mode_), f.field("capacity", x.capacity_),
      f.field("read_timeout", x.read_timeout_),
      f.field("write_timeout", x.write_timeout_),
      f.field("default_read_timeout", x.default_read_timeout_),
      f.field("mode_value", x.mode_value_), f.field("id_value", x.id_value_),
      f.field("capacity_value", x.capacity_value_),
      f.field("read_timeout_value", x.read_timeout_value_),
      f.field("write_timeout_value", x.write_timeout_value_));
  }

private:
  location op_loc_;
  ast::expression id_;
  std::optional<ast::expression> mode_;
  std::optional<ast::expression> capacity_;
  std::optional<ast::expression> read_timeout_;
  std::optional<ast::expression> write_timeout_;
  duration default_read_timeout_ = {};
  // Resolved values (set during substitute with instantiate=true).
  std::optional<std::string> mode_value_;
  std::optional<std::string> id_value_;
  std::optional<located<uint64_t>> capacity_value_;
  std::optional<located<duration>> read_timeout_value_;
  std::optional<located<duration>> write_timeout_value_;
};

using cache_ir_plugin = inspection_plugin<ir::Operator, CacheIr>;

class cache_plugin final : public virtual operator_factory_plugin,
                           public virtual operator_parser_plugin,
                           public virtual component_plugin,
                           public virtual operator_compiler_plugin {
public:
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    using namespace si_literals;
    using namespace std::literals;
    TRY(cache_lifetime_,
        try_get_or(global_config, "tenzir.cache.lifetime", duration{10min}));
    if (cache_lifetime_ <= duration::zero()) {
      return diagnostic::error("cache lifetime must be greater than zero")
        .to_error();
    }
    TRY(cache_capacity_,
        try_get_or(global_config, "tenzir.cache.capacity", 1_Gi));
    if (cache_capacity_ < 64_Mi) {
      return diagnostic::error("cache capacity must be at least 64 MiB")
        .to_error();
    }
    return {};
  }

  auto name() const -> std::string override {
    return "cache";
  };

  auto component_name() const -> std::string override {
    return "cache-manager";
  }

  auto make_component(node_actor::stateful_pointer<node_state> self) const
    -> component_plugin_actor override {
    return self->spawn<caf::linked>(caf::actor_from_state<cache_manager>,
                                    cache_capacity_);
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"cache", "https://docs.tenzir.com/"
                                           "operators/cache"};
    auto id = located<std::string>{};
    auto mode = std::optional<located<std::string>>{};
    auto capacity = std::optional<located<uint64_t>>{};
    auto read_timeout = std::optional<located<duration>>{};
    auto write_timeout = std::optional<located<duration>>{};
    parser.add(id, "<id>");
    parser.add("--mode", mode, "<read|write|readwrite>");
    parser.add("--capacity", capacity, "<capacity>");
    parser.add("--read-timeout", read_timeout, "<duration>");
    parser.add("--write-timeout", write_timeout, "<duration>");
    parser.parse(p);
    if (mode
        and (mode->inner != "read" and mode->inner != "write"
             and mode->inner != "readwrite")) {
      diagnostic::error("unknown mode `{}`", mode->inner)
        .note("available modes: read, write, readwrite")
        .primary(mode->source)
        .throw_();
    }
    if (not capacity) {
      capacity.emplace(std::numeric_limits<uint64_t>::max(), location::unknown);
    }
    if (not read_timeout) {
      read_timeout.emplace(cache_lifetime_, location::unknown);
    } else if (read_timeout->inner <= duration::zero()) {
      diagnostic::error("read timeout must be a positive duration")
        .primary(read_timeout->source)
        .throw_();
    }
    if (not write_timeout) {
      write_timeout.emplace(duration::zero(), location::unknown);
    } else if (write_timeout->inner <= duration::zero()) {
      diagnostic::error("write timeout must be a positive duration")
        .primary(write_timeout->source)
        .throw_();
    }
    if (not mode or mode->inner == "readwrite") {
      auto result = std::make_unique<pipeline>();
      result->append(std::make_unique<write_cache_operator>(id));
      result->append(std::make_unique<read_cache_operator>(
        std::move(id), *capacity, read_timeout->inner, write_timeout->inner));
      return result;
    }
    if (mode->inner == "write") {
      return std::make_unique<write_cache_operator>(
        std::move(id), *capacity, read_timeout->inner, write_timeout->inner);
    }
    TENZIR_ASSERT(mode->inner == "read");
    return std::make_unique<read_cache_operator>(std::move(id));
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto id = located<std::string>{};
    auto mode = std::optional<located<std::string>>{};
    auto capacity = std::optional<located<uint64_t>>{};
    auto read_timeout = std::optional<located<duration>>{};
    auto write_timeout = std::optional<located<duration>>{};
    argument_parser2::operator_("cache")
      .positional("id", id)
      .named("mode", mode)
      .named("capacity", capacity)
      .named("read_timeout", read_timeout)
      .named("write_timeout", write_timeout)
      .parse(inv, ctx)
      .ignore();
    auto failed = false;
    if (mode
        and (mode->inner != "read" and mode->inner != "write"
             and mode->inner != "readwrite")) {
      diagnostic::error("unknown mode `{}`", mode->inner)
        .note("available modes: read, write, readwrite")
        .primary(mode->source)
        .emit(ctx);
      failed = true;
    }
    if (not capacity) {
      capacity.emplace(std::numeric_limits<uint64_t>::max(),
                       inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `capacity` in `read` mode")
        .primary(capacity->source)
        .emit(ctx);
    }
    if (not read_timeout) {
      read_timeout.emplace(cache_lifetime_, inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `read_timeout` in `read` mode")
        .primary(read_timeout->source)
        .emit(ctx);
    } else if (read_timeout->inner <= duration::zero()) {
      diagnostic::error("read timeout must be a positive duration")
        .primary(read_timeout->source)
        .emit(ctx);
      failed = true;
    }
    if (not write_timeout) {
      write_timeout.emplace(duration::zero(), inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `write_timeout` in `read` mode")
        .primary(write_timeout->source)
        .emit(ctx);
    } else if (write_timeout->inner <= duration::zero()) {
      diagnostic::error("create timeout must be a positive duration")
        .primary(write_timeout->source)
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    if (not mode or mode->inner == "readwrite") {
      auto result = std::make_unique<pipeline>();
      result->append(std::make_unique<write_cache_operator>(id));
      result->append(std::make_unique<read_cache_operator>(
        std::move(id), *capacity, read_timeout->inner, write_timeout->inner));
      return result;
    }
    if (mode->inner == "write") {
      return std::make_unique<write_cache_operator>(
        std::move(id), *capacity, read_timeout->inner, write_timeout->inner);
    }
    TENZIR_ASSERT(mode->inner == "read");
    return std::make_unique<read_cache_operator>(std::move(id));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto id = ast::expression{};
    auto mode = std::optional<ast::expression>{};
    auto capacity = std::optional<ast::expression>{};
    auto read_timeout = std::optional<ast::expression>{};
    auto write_timeout = std::optional<ast::expression>{};
    auto provider = session_provider::make(ctx);
    auto loc = inv.op.get_location();
    TRY(argument_parser2::operator_("cache")
          .positional("id", id, "string")
          .named("mode", mode, "string")
          .named("capacity", capacity, "uint64")
          .named("read_timeout", read_timeout, "duration")
          .named("write_timeout", write_timeout, "duration")
          .parse(operator_factory_invocation{std::move(inv.op),
                                             std::move(inv.args)},
                 provider.as_session()));
    TRY(id.bind(ctx));
    if (mode) {
      TRY(mode->bind(ctx));
    }
    if (capacity) {
      TRY(capacity->bind(ctx));
    }
    if (read_timeout) {
      TRY(read_timeout->bind(ctx));
    }
    if (write_timeout) {
      TRY(write_timeout->bind(ctx));
    }
    return CacheIr{loc,
                   std::move(id),
                   std::move(mode),
                   std::move(capacity),
                   std::move(read_timeout),
                   std::move(write_timeout),
                   cache_lifetime_};
  }

private:
  duration cache_lifetime_ = {};
  uint64_t cache_capacity_ = {};
};

using write_cache_plugin = operator_inspection_plugin<write_cache_operator>;
using read_cache_plugin = operator_inspection_plugin<read_cache_operator>;

} // namespace

} // namespace tenzir::plugins::cache

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::cache_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::write_cache_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::read_cache_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::cache_ir_plugin)
