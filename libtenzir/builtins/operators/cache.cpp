//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try_get.hpp>
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
    // Make the cache actor monitor the sender.
    auto(atom::announce)->caf::result<void>,
    // Check if the cache already has a writer.
    auto(atom::write, atom::ok)->caf::result<bool>,
    // Write events into the cache.
    auto(atom::write, table_slice events)->caf::result<bool>,
    // Read events from the cache.
    auto(atom::read)->caf::result<table_slice>>;
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
    for (auto& [_, reader] : readers_) {
      reader.rp.deliver({});
    }
  }

  auto make_behavior() -> cache_actor::behavior_type {
    return {
      [this](atom::announce) -> caf::result<void> {
        return announce();
      },
      [this](atom::write, atom::ok) -> caf::result<bool> {
        return write_ok();
      },
      [this](atom::write, table_slice events) -> caf::result<bool> {
        return write(std::move(events));
      },
      [this](atom::read) -> caf::result<table_slice> {
        return read();
      },
    };
  }

private:
  auto reset_read_timeout() -> void {
    TENZIR_ASSERT(read_timeout_ > duration::zero());
    on_read_timeout_.dispose();
    on_read_timeout_ = self_->run_delayed_weak(read_timeout_, [this] {
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
      self_->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto announce() -> caf::result<void> {
    if (not writer_) {
      const auto sender = self_->current_sender();
      writer_ = sender->address();
      self_->monitor(sender, [this](const caf::error& err) {
        TENZIR_ASSERT(not done_);
        done_ = true;
        update_multicaster_.push(cache_update{
          .approx_bytes = byte_size_,
          .state = err ? cache_state::failed : cache_state::closed,
        });
        update_multicaster_.close();
        reset_read_timeout();
        // We ignore error messages because they do not matter to the readers.
        for (auto& [_, reader] : readers_) {
          if (reader.offset == cached_events_.size() and reader.rp.pending()) {
            reader.rp.deliver(table_slice{});
          }
        }
      });
      set_write_timeout();
    }
    return {};
  }

  auto write_ok() -> caf::result<bool> {
    return writer_;
  }

  auto write(table_slice events) -> caf::result<bool> {
    TENZIR_ASSERT(writer_);
    TENZIR_ASSERT(events.rows() > 0);
    const auto sender = self_->current_sender();
    TENZIR_ASSERT(sender);
    if (writer_ != sender->address()) {
      return false;
    }
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
    return not exceeded_capacity;
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
  bool done_ = {};
  detail::flat_map<caf::actor_addr, reader> readers_;

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

class cache_plugin final : public virtual operator_factory_plugin,
                           public virtual operator_parser_plugin,
                           public virtual component_plugin {
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

  auto make(invocation inv, session ctx) const
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
