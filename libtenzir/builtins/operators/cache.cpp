//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::cache {

namespace {

struct cache_traits {
  using signatures = caf::type_list<
    // Check if the cache already has a writer.
    auto(atom::write, atom::ok)->caf::result<bool>,
    // Write events into the cache.
    auto(atom::write, table_slice events)->caf::result<bool>,
    // Read events from the cache.
    auto(atom::read)->caf::result<table_slice>>;
};

using cache_actor = caf::typed_actor<cache_traits>;

struct cache_state {
  static constexpr auto name = "cache";

  cache_actor::pointer self = {};

  shared_diagnostic_handler diagnostics = {};

  located<uint64_t> capacity = {};
  uint64_t cache_size = {};
  std::vector<table_slice> cache = {};

  caf::actor_addr writer = {};
  bool done = {};

  duration read_timeout = {};
  duration write_timeout = {};
  caf::disposable on_read_timeout = {};
  caf::disposable on_write_timeout = {};

  struct reader {
    size_t offset = {};
    caf::typed_response_promise<table_slice> rp = {};
  };
  detail::flat_map<caf::actor_addr, reader> readers = {};

  cache_state(cache_actor::pointer self, shared_diagnostic_handler diagnostics,
              located<uint64_t> capacity, duration read_timeout,
              duration write_timeout)
    : self{self},
      diagnostics{std::move(diagnostics)},
      capacity{capacity},
      read_timeout{read_timeout},
      write_timeout{write_timeout} {
  }

  auto make_behavior() -> cache_actor::behavior_type {
    return {
      [this](atom::write, atom::ok) -> caf::result<bool> {
        return write_ok();
      },
      [this](atom::write, table_slice& events) -> caf::result<bool> {
        return write(std::move(events));
      },
      [this](atom::read) -> caf::result<table_slice> {
        return read();
      },
    };
  }

  auto reset_read_timeout() -> void {
    TENZIR_ASSERT(read_timeout > duration::zero());
    on_read_timeout.dispose();
    on_read_timeout = detail::weak_run_delayed(self, read_timeout, [this] {
      self->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto set_write_timeout() -> void {
    if (write_timeout == duration::zero()) {
      return;
    }
    TENZIR_ASSERT(write_timeout > duration::zero());
    on_write_timeout.dispose();
    on_write_timeout = detail::weak_run_delayed(self, write_timeout, [this] {
      self->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto write_ok() -> caf::result<bool> {
    return writer;
  }

  auto write(table_slice events) -> caf::result<bool> {
    TENZIR_ASSERT(events.rows() > 0);
    const auto sender = self->current_sender();
    TENZIR_ASSERT(sender);
    if (not writer) {
      writer = sender->address();
      self->monitor(sender, [this](const caf::error&) {
        TENZIR_ASSERT(not done);
        done = true;
        reset_read_timeout();
        // We ignore error messages because they do not matter to the readers.
        for (auto& [_, reader] : readers) {
          if (reader.offset == cache.size() and reader.rp.pending()) {
            reader.rp.deliver(table_slice{});
          }
        }
      });
      set_write_timeout();
    } else if (writer != sender->address()) {
      return false;
    }
    auto exceeded_capacity = false;
    if (cache_size + events.rows() > capacity.inner) {
      events = head(std::move(events), capacity.inner - cache_size);
      diagnostic::warning("cache exceeded capacity")
        .primary(capacity.source)
        .emit(diagnostics);
      exceeded_capacity = true;
      if (events.rows() == 0) {
        return false;
      }
    }
    cache_size += events.rows();
    cache.push_back(std::move(events));
    for (auto& [_, reader] : readers) {
      if (not reader.rp.pending()) {
        TENZIR_ASSERT(reader.offset < cache.size());
        continue;
      }
      reader.rp.deliver(cache[reader.offset++]);
      TENZIR_ASSERT(reader.offset == cache.size());
    }
    return not exceeded_capacity;
  }

  auto read() -> caf::result<table_slice> {
    if (done) {
      reset_read_timeout();
    }
    const auto sender = self->current_sender();
    TENZIR_ASSERT(sender);
    auto& reader = readers[sender->address()];
    TENZIR_ASSERT(not reader.rp.pending());
    TENZIR_ASSERT(reader.offset <= cache.size());
    if (reader.offset == 0) {
      self->monitor(sender, [this, sender](const caf::error&) {
        const auto erased = readers.erase(sender);
        TENZIR_ASSERT(erased == 1);
      });
    }
    if (reader.offset == cache.size()) {
      if (done) {
        return table_slice{};
      }
      reader.rp = self->make_response_promise<table_slice>();
      return reader.rp;
    }
    return cache[reader.offset++];
  }
};

struct cache_manager_traits {
  using signatures = caf::type_list<
    // Get the cache.
    auto(atom::get, std::string id, bool exclusive)->caf::result<caf::actor>,
    // Create the cache if it does not already exist.
    auto(atom::create, std::string id, bool exclusive,
         shared_diagnostic_handler diagnostics, uint64_t capacity,
         location capacity_loc, duration read_timeout, duration write_timeout)
      ->caf::result<caf::actor>>;
};

using cache_manager_actor
  = caf::typed_actor<cache_manager_traits>::extend_with<component_plugin_actor>;

struct cache_manager_state {
  static constexpr auto name = "cache-manager";

  cache_manager_actor::pointer self = {};
  std::unordered_map<std::string, cache_actor> caches = {};

  explicit cache_manager_state(cache_manager_actor::pointer self) : self{self} {
  }

  auto make_behavior() -> cache_manager_actor::behavior_type {
    return {
      [this](atom::get, std::string& id,
             bool exclusive) -> caf::result<caf::actor> {
        return get(std::move(id), exclusive);
      },
      [this](atom::create, std::string& id, bool exclusive,
             shared_diagnostic_handler& diagnostics, uint64_t capacity,
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

  auto check_exclusive(const cache_actor& cache, bool exclusive) const
    -> caf::result<caf::actor> {
    TENZIR_ASSERT(cache);
    auto handle = caf::actor_cast<caf::actor>(cache);
    if (not exclusive) {
      return handle;
    }
    auto rp = self->make_response_promise<caf::actor>();
    self->mail(atom::write_v, atom::ok_v)
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
    auto cache = caches.find(id);
    if (cache == caches.end()) {
      return diagnostic::error("cache `{}` does not exist", id).to_error();
    }
    return check_exclusive(cache->second, exclusive);
  }

  auto create(std::string id, bool exclusive,
              shared_diagnostic_handler diagnostics, located<uint64_t> capacity,
              duration read_timeout, duration write_timeout)
    -> caf::result<caf::actor> {
    auto cache = caches.find(id);
    if (cache == caches.end()) {
      auto handle = self->spawn(caf::actor_from_state<cache_state>,
                                std::move(diagnostics), capacity, read_timeout,
                                write_timeout);
      self->monitor(handle,
                    [this, source = handle->address()](const caf::error&) {
                      for (const auto& [id, handle] : caches) {
                        if (handle.address() == source) {
                          caches.erase(id);
                          return;
                        }
                      }
                      TENZIR_UNREACHABLE();
                    });
      return caf::actor_cast<caf::actor>(
        caches.emplace_hint(cache, std::move(id), std::move(handle))->second);
    }
    return check_exclusive(cache->second, exclusive);
  }
};

class write_cache_operator final : public operator_base {
public:
  write_cache_operator() = default;

  explicit write_cache_operator(located<std::string> id,
                                located<uint64_t> capacity,
                                duration read_timeout, duration write_timeout)
    : id_{std::move(id)},
      sink_{true},
      capacity_{capacity},
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
              /* exclusive */ true, ctrl.shared_diagnostics(), capacity_.inner,
              capacity_.source, read_timeout_, write_timeout_)
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
        ->request(cache_manager, caf::infinite, atom::get_v, id_.inner,
                  /* exclusive */ true)
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
                              f.field("capacity", x.capacity_),
                              f.field("read_timeout", x.read_timeout_),
                              f.field("write_timeout", x.write_timeout_));
  }

private:
  located<std::string> id_ = {};
  bool sink_ = {};
  located<uint64_t> capacity_ = {};
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
                               located<uint64_t> capacity,
                               duration read_timeout, duration write_timeout)
    : id_{std::move(id)},
      capacity_{capacity},
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
                       ctrl.shared_diagnostics(), capacity_.inner,
                       capacity_.source, read_timeout_, write_timeout_)
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
        .request(cache, caf::infinite, atom::read_v)
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
                              f.field("capacity", x.capacity_),
                              f.field("read_timeout", x.read_timeout_),
                              f.field("write_timeout", x.write_timeout_));
  }

private:
  located<std::string> id_ = {};
  bool source_ = {};
  located<uint64_t> capacity_ = {};
  duration read_timeout_ = {};
  duration write_timeout_ = {};
};

class cache_plugin final : public virtual operator_factory_plugin,
                           public virtual operator_parser_plugin,
                           public virtual component_plugin {
public:
  auto name() const -> std::string override {
    return "cache";
  };

  auto component_name() const -> std::string override {
    return "cache-manager";
  }

  auto make_component(node_actor::stateful_pointer<node_state> self) const
    -> component_plugin_actor override {
    return self->spawn<caf::linked>(caf::actor_from_state<cache_manager_state>);
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
      capacity.emplace(defaults::max_partition_size, location::unknown);
    }
    if (not read_timeout) {
      read_timeout.emplace(std::chrono::minutes{1}, location::unknown);
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
      capacity.emplace(defaults::max_partition_size, inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `capacity` in `read` mode")
        .primary(capacity->source)
        .emit(ctx);
    }
    if (not read_timeout) {
      read_timeout.emplace(std::chrono::minutes{1}, inv.self.get_location());
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
      diagnostic::warning("ignoring argument `create_timeout` in `read` mode")
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
};

using write_cache_plugin = operator_inspection_plugin<write_cache_operator>;
using read_cache_plugin = operator_inspection_plugin<read_cache_operator>;

} // namespace

} // namespace tenzir::plugins::cache

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::cache_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::write_cache_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cache::read_cache_plugin)
