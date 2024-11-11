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

#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::cache {

namespace {

using cache_actor = caf::typed_actor<
  // Check if the cache already has a writer.
  auto(atom::write, atom::ok)->caf::result<bool>,
  // Write events into the cache.
  auto(atom::write, table_slice events)->caf::result<bool>,
  // Read events from the cache.
  auto(atom::read)->caf::result<table_slice>>;

struct cache_state {
  [[maybe_unused]] static constexpr auto name = "cache";

  cache_actor::pointer self = {};

  shared_diagnostic_handler diagnostics = {};

  located<uint64_t> capacity = {};
  uint64_t cache_size = {};
  std::vector<table_slice> cache = {};

  caf::actor_addr writer = {};
  bool done = {};

  duration ttl = {};
  duration max_ttl = {};
  caf::disposable on_ttl = {};
  caf::disposable on_max_ttl = {};

  struct reader {
    size_t offset = {};
    caf::typed_response_promise<table_slice> rp = {};
  };
  detail::flat_map<caf::actor_addr, reader> readers = {};

  auto reset_ttl() -> void {
    TENZIR_ASSERT(ttl > duration::zero());
    on_ttl.dispose();
    on_ttl = detail::weak_run_delayed(self, ttl, [this] {
      self->quit(diagnostic::error("cache expired").to_error());
    });
  }

  auto set_max_ttl() -> void {
    if (max_ttl == duration::zero()) {
      return;
    }
    TENZIR_ASSERT(max_ttl > duration::zero());
    on_max_ttl.dispose();
    on_max_ttl = detail::weak_run_delayed(self, max_ttl, [this] {
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
      self->monitor(writer);
      set_max_ttl();
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
      reset_ttl();
    }
    const auto sender = self->current_sender();
    TENZIR_ASSERT(sender);
    auto& reader = readers[sender->address()];
    TENZIR_ASSERT(not reader.rp.pending());
    TENZIR_ASSERT(reader.offset <= cache.size());
    if (reader.offset == 0) {
      self->monitor(sender);
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

  auto handle_down(const caf::down_msg& msg) -> void {
    if (msg.source == writer) {
      TENZIR_ASSERT(not done);
      done = true;
      reset_ttl();
      // We ignore error messages because they do not matter to the readers.
      for (auto& [_, reader] : readers) {
        if (reader.offset == cache.size() and reader.rp.pending()) {
          reader.rp.deliver(table_slice{});
        }
      }
      return;
    }
    const auto erased = readers.erase(msg.source);
    TENZIR_ASSERT(erased == 1);
  }
};

auto make_cache(cache_actor::stateful_pointer<cache_state> self,
                shared_diagnostic_handler diagnostics,
                located<uint64_t> capacity, duration ttl, duration max_ttl)
  -> cache_actor::behavior_type {
  self->state.self = self;
  self->state.diagnostics = std::move(diagnostics);
  self->state.capacity = capacity;
  self->state.ttl = ttl;
  self->state.max_ttl = max_ttl;
  self->set_down_handler([self](const caf::down_msg& msg) {
    self->state.handle_down(msg);
  });
  return {
    [self](atom::write, atom::ok) -> caf::result<bool> {
      return self->state.write_ok();
    },
    [self](atom::write, table_slice& events) -> caf::result<bool> {
      return self->state.write(std::move(events));
    },
    [self](atom::read) -> caf::result<table_slice> {
      return self->state.read();
    },
  };
}

using cache_manager_actor = caf::typed_actor<
  // Get the cache.
  auto(atom::get, std::string id, bool exclusive)->caf::result<caf::actor>,
  // Create the cache if it does not already exist.
  auto(atom::create, std::string id, bool exclusive,
       shared_diagnostic_handler diagnostics, uint64_t capacity,
       location capacity_loc, duration ttl, duration max_ttl)
    ->caf::result<caf::actor>
  // Plugin interface.
  >::extend_with<component_plugin_actor>;

struct cache_manager_state {
  [[maybe_unused]] static constexpr auto name = "cache-manager";

  cache_manager_actor::pointer self = {};
  std::unordered_map<std::string, cache_actor> caches = {};

  auto check_exclusive(const cache_actor& cache, bool exclusive) const
    -> caf::result<caf::actor> {
    TENZIR_ASSERT(cache);
    auto handle = caf::actor_cast<caf::actor>(cache);
    if (not exclusive) {
      return handle;
    }
    auto rp = self->make_response_promise<caf::actor>();
    self->request(cache, caf::infinite, atom::write_v, atom::ok_v)
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
              duration ttl, duration max_ttl) -> caf::result<caf::actor> {
    auto cache = caches.find(id);
    if (cache == caches.end()) {
      auto handle = self->spawn<caf::monitored>(
        make_cache, std::move(diagnostics), capacity, ttl, max_ttl);
      return caf::actor_cast<caf::actor>(
        caches.emplace_hint(cache, std::move(id), std::move(handle))->second);
    }
    return check_exclusive(cache->second, exclusive);
  }

  auto handle_down(const caf::down_msg& msg) -> void {
    for (const auto& [id, handle] : caches) {
      if (handle.address() == msg.source) {
        caches.erase(id);
        return;
      }
    }
    TENZIR_UNREACHABLE();
  }
};

auto make_cache_manager(
  cache_manager_actor::stateful_pointer<cache_manager_state> self)
  -> cache_manager_actor::behavior_type {
  self->state.self = self;
  self->set_down_handler([self](const caf::down_msg& msg) {
    self->state.handle_down(msg);
  });
  return {
    [self](atom::get, std::string& id,
           bool exclusive) -> caf::result<caf::actor> {
      return self->state.get(std::move(id), exclusive);
    },
    [self](atom::create, std::string& id, bool exclusive,
           shared_diagnostic_handler& diagnostics, uint64_t capacity,
           location capacity_loc, duration ttl,
           duration max_ttl) -> caf::result<caf::actor> {
      return self->state.create(std::move(id), exclusive,
                                std::move(diagnostics),
                                {capacity, capacity_loc}, ttl, max_ttl);
    },
    [](atom::ping) -> caf::result<void> {
      return {};
    },
  };
}

class write_cache_operator final : public operator_base {
public:
  write_cache_operator() = default;

  explicit write_cache_operator(located<std::string> id,
                                located<uint64_t> capacity, duration ttl,
                                duration max_ttl)
    : id_{std::move(id)},
      sink_{true},
      capacity_{capacity},
      ttl_{ttl},
      max_ttl_{max_ttl} {
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
        .request(cache_manager, caf::infinite, atom::create_v, id_.inner,
                 /* exclusive */ true, ctrl.shared_diagnostics(),
                 capacity_.inner, capacity_.source, ttl_, max_ttl_)
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
        .request(cache, caf::infinite, atom::write_v, std::move(events))
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
                              f.field("ttl", x.ttl_),
                              f.field("max_ttl", x.max_ttl_));
  }

private:
  located<std::string> id_ = {};
  bool sink_ = {};
  located<uint64_t> capacity_ = {};
  duration ttl_ = {};
  duration max_ttl_ = {};
};

class read_cache_operator final : public crtp_operator<read_cache_operator> {
public:
  read_cache_operator() = default;

  explicit read_cache_operator(located<std::string> id)
    : id_{std::move(id)}, source_{true} {
  }

  explicit read_cache_operator(located<std::string> id,
                               located<uint64_t> capacity, duration ttl,
                               duration max_ttl)
    : id_{std::move(id)}, capacity_{capacity}, ttl_{ttl}, max_ttl_{max_ttl} {
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
    (source_ ? ctrl.self().request(cache_manager, caf::infinite, atom::get_v,
                                   id_.inner, /* exclusive */ false)
             : ctrl.self().request(cache_manager, caf::infinite, atom::create_v,
                                   id_.inner, /* exclusive */ false,
                                   ctrl.shared_diagnostics(), capacity_.inner,
                                   capacity_.source, ttl_, max_ttl_))
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
                              f.field("ttl", x.ttl_),
                              f.field("max_ttl", x.max_ttl_));
  }

private:
  located<std::string> id_ = {};
  bool source_ = {};
  located<uint64_t> capacity_ = {};
  duration ttl_ = {};
  duration max_ttl_ = {};
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
    return self->spawn<caf::linked>(make_cache_manager);
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
    auto ttl = std::optional<located<duration>>{};
    auto max_ttl = std::optional<located<duration>>{};
    parser.add(id, "<id>");
    parser.add("--mode", mode, "<read|write|readwrite>");
    parser.add("--capacity", capacity, "<capacity>");
    parser.add("--ttl", ttl, "<duration>");
    parser.add("--max-ttl", max_ttl, "<duration>");
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
    if (not ttl) {
      ttl.emplace(std::chrono::minutes{1}, location::unknown);
    } else if (ttl->inner <= duration::zero()) {
      diagnostic::error("ttl must be a positive duration")
        .primary(ttl->source)
        .throw_();
    }
    if (not max_ttl) {
      max_ttl.emplace(duration::zero(), location::unknown);
    } else if (max_ttl->inner <= duration::zero()) {
      diagnostic::error("max_ttl must be a positive duration")
        .primary(max_ttl->source)
        .throw_();
    }
    if (not mode or mode->inner == "readwrite") {
      auto result = std::make_unique<pipeline>();
      result->append(std::make_unique<write_cache_operator>(id));
      result->append(std::make_unique<read_cache_operator>(
        std::move(id), *capacity, ttl->inner, max_ttl->inner));
      return result;
    }
    if (mode->inner == "write") {
      return std::make_unique<write_cache_operator>(std::move(id), *capacity,
                                                    ttl->inner, max_ttl->inner);
    }
    TENZIR_ASSERT(mode->inner == "read");
    return std::make_unique<read_cache_operator>(std::move(id));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto id = located<std::string>{};
    auto mode = std::optional<located<std::string>>{};
    auto capacity = std::optional<located<uint64_t>>{};
    auto ttl = std::optional<located<duration>>{};
    auto max_ttl = std::optional<located<duration>>{};
    argument_parser2::operator_("cache")
      .add(id, "<id>")
      .add("mode", mode)
      .add("capacity", capacity)
      .add("ttl", ttl)
      .add("max_ttl", max_ttl)
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
    if (not ttl) {
      ttl.emplace(std::chrono::minutes{1}, inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `ttl` in `read` mode")
        .primary(ttl->source)
        .emit(ctx);
    } else if (ttl->inner <= duration::zero()) {
      diagnostic::error("ttl must be a positive duration")
        .primary(ttl->source)
        .emit(ctx);
      failed = true;
    }
    if (not max_ttl) {
      max_ttl.emplace(duration::zero(), inv.self.get_location());
    } else if (mode and mode->inner == "read") {
      diagnostic::warning("ignoring argument `max_ttl` in `read` mode")
        .primary(max_ttl->source)
        .emit(ctx);
    } else if (max_ttl->inner <= duration::zero()) {
      diagnostic::error("max_ttl must be a positive duration")
        .primary(max_ttl->source)
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
        std::move(id), *capacity, ttl->inner, max_ttl->inner));
      return result;
    }
    if (mode->inner == "write") {
      return std::make_unique<write_cache_operator>(std::move(id), *capacity,
                                                    ttl->inner, max_ttl->inner);
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
