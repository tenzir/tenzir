//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/store.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/error.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

namespace {

void remove_down_source(auto* self, const caf::actor_addr& source,
                        const caf::error& err) {
  for (const auto& [query_id, state] : self->state().running_extractions) {
    if (state.sink->address() == source) {
      TENZIR_DEBUG("{} received DOWN from extract query {}: {}", *self,
                   query_id, err);
      self->state().running_extractions.erase(query_id);
      break; // a sink can only have one active extract query, so we stop
    }
  }
}

void continue_query(auto self, const uuid& query_id) {
  TENZIR_TRACE("{} continues working on extract query {}", *self, query_id);
  auto it = self->state().running_extractions.find(query_id);
  if (it == self->state().running_extractions.end()) {
    // We asynchronously erase when the client goes down.
    return;
  }
  auto& [_, state] = *it;
  auto slice = state.result_generator.next();
  if (not slice) {
    TENZIR_DEBUG("{} finished working on extract query {}", *self, query_id);
    state.rp.deliver(state.num_hits);
    self->state().running_extractions.erase(it);
    return;
  }
  state.num_hits += slice->rows();
  self->mail(std::move(*slice))
    .request(state.sink, caf::infinite)
    .then(
      [self, query_id] {
        continue_query(self, query_id);
      },
      [self, query_id](caf::error& err) {
        auto it = self->state().running_extractions.find(query_id);
        if (it == self->state().running_extractions.end()) {
          // We asynchronously erase when the client goes down.
          return;
        }
        it->second.rp.deliver(caf::make_error(
          ec::unspecified,
          fmt::format("{} got error from sink: {}", *self, err)));
        self->state().running_extractions.erase(it);
      });
}

// A query execution is performed incrementally on individual table slices.
// On each invocation, only a single table slice is processed via filter
// or count, then execution is paused to allow incremental processing and
// early exit (e.g., when the query limit is reached or the query client
// disconnects).
// To achieve that, the defaut store actor implementation:
// 1. Keeps track of all queries currently in progress in maps keyed by uuid,
//    separated by query type (count, extract).
// 2. After processing a table slice, sends a message to self, which is
//    the trigger to continue processing the qery, and makes processing
//    interruptable.
// 3. Monitors all query sinks and removes queries from the map whos sink is
//    no longer available, which allows timely cancellation and avoids
//    superfluous computations.
template <class Actor>
caf::result<uint64_t>
handle_query(const auto& self, const query_context& query_context) {
  TENZIR_DEBUG("{} got a query: {}", *self, query_context);
  const auto start = std::chrono::steady_clock::now();
  const auto schema = self->state().store->schema();
  const auto tailored_expr = tailor(query_context.expr, schema);
  if (! tailored_expr) {
    // In case the query was delegated from an active partition the
    // taxonomy resolution is not guaranteed to have worked (whenever the
    // type in this store did not exist in the catalog when the partition
    // was created). In that case we simply discard the query.
    if constexpr (std::is_same_v<default_active_store_actor, Actor>) {
      return 0ull;
    }
    return caf::make_error(ec::invalid_query,
                           fmt::format("{} failed to tailor '{}' to '{}'",
                                       *self, query_context.expr, schema));
  }
  auto rp = self->template make_response_promise<uint64_t>();
  auto f = detail::overload{
    [&](const extract_query_context& extract) -> void {
      // We monitor all query sinks, and remove queries associated with the sink.
      self->monitor(extract.sink, [self, source = extract.sink->address()](
                                    const caf::error& err) {
        remove_down_source(self, source, err);
      });
      auto [state, inserted] = self->state().running_extractions.try_emplace(
        query_context.id, extract_query_state{});
      if (! inserted) {
        rp.deliver(caf::make_error(
          ec::logic_error, fmt::format("{} received duplicated query id {}",
                                       *self, query_context.id)));
        return;
      }
      state->second.result_generator
        = self->state().store->extract(*tailored_expr);
      state->second.sink = extract.sink;
      state->second.start = start;
      state->second.rp = std::move(rp);
      continue_query(self, query_context.id);
    },
  };
  match(query_context.cmd, f);
  return rp;
}

} // namespace

type base_store::schema() const {
  for (const auto& slice : slices()) {
    return slice.schema();
  }
  return {};
}

generator<uint64_t> base_store::count(expression expr, ids selection) const {
  for (const auto& slice : slices()) {
    co_yield count_matching(slice, expr, selection);
  }
}

generator<table_slice> base_store::extract(expression expr) const {
  for (const auto& slice : slices()) {
    if (auto filtered_slice = filter(slice, expr)) {
      co_yield std::move(*filtered_slice);
    }
  }
}

default_passive_store_actor::behavior_type default_passive_store(
  default_passive_store_actor::stateful_pointer<default_passive_store_state>
    self,
  std::unique_ptr<passive_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type) {
  // Configure our actor state.
  self->state().self = self;
  self->state().filesystem = std::move(filesystem);
  self->state().store = std::move(store);
  self->state().path = std::move(path);
  self->state().store_type = std::move(store_type);
  // Load data from disk.
  self->mail(atom::mmap_v, self->state().path)
    .request(self->state().filesystem, caf::infinite)
    .await(
      [self](chunk_ptr& chunk) {
        auto load_error = self->state().store->load(std::move(chunk));
        if (load_error) {
          self->quit(std::move(load_error));
        }
      },
      [self](caf::error& error) {
        self->quit(std::move(error));
      });
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::result<uint64_t> {
      return handle_query<default_passive_store_actor>(self, query_context);
    },
    [self](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state().store->num_events();
      TENZIR_DEBUG("{} erases {} events", *self, num_events);
      TENZIR_ASSERT_EXPENSIVE(rank(selection) == 0
                              || rank(selection) == num_events);
      auto rp = self->make_response_promise<uint64_t>();
      self->mail(atom::erase_v, self->state().path)
        .request(self->state().filesystem, caf::infinite)
        .then(
          [rp, num_events](atom::done) mutable {
            rp.deliver(num_events);
          },
          [rp](caf::error& error) mutable {
            rp.deliver(std::move(error));
          });
      return rp;
    },
  };
}

default_active_store_actor::behavior_type default_active_store(
  default_active_store_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type) {
  // Configure our actor state.
  self->state().self = self;
  self->state().filesystem = std::move(filesystem);
  self->state().store = std::move(store);
  self->state().path = std::move(path);
  self->state().store_type = std::move(store_type);
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::result<uint64_t> {
      TENZIR_DEBUG("{} starts working on query {}", *self, query_context.id);
      if (self->state().store->num_events() == 0) {
        return 0ull;
      }
      return handle_query<default_active_store_actor>(self, query_context);
    },
    [self](atom::erase, const ids& selection) {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state().store->num_events();
      TENZIR_ASSERT_EXPENSIVE(rank(selection) == 0
                              || rank(selection) == num_events);
      // We don't actually need to erase anything in the store itself, but
      // rather just don't need to persist when shutting down the stream, so
      // we set a flag for that in the actor state.
      self->state().erased = true;
      return num_events;
    },
    [self](atom::persist) -> caf::result<resource> {
      if (self->state().erased) {
        return {};
      }
      auto chunk = self->state().store->finish();
      if (! chunk) {
        self->quit(diagnostic::error(std::move(chunk.error()))
                     .note("while persisting store to disk")
                     .to_error());
        return {};
      }
      auto rp = self->make_response_promise<resource>();
      auto res = resource{
        .url = fmt::format("file://{}", self->state().path),
        .size = (*chunk)->size(),
      };
      self->mail(atom::write_v, self->state().path, std::move(*chunk))
        .request(self->state().filesystem, caf::infinite)
        .then(
          [self, rp, res](atom::ok) mutable {
            TENZIR_DEBUG("{} ({}) persisted itself to {}", *self,
                         self->state().store_type, self->state().path);
            TENZIR_ASSERT(rp.pending());
            rp.deliver(res);
            self->quit();
          },
          [self, rp](caf::error& error) mutable {
            rp.deliver(error);
            self->quit(diagnostic::error(std::move(error))
                         .note("while persisting store to disk")
                         .to_error());
          });
      return rp;
    },
    [self](table_slice& slice) {
      if (self->state().erased) {
        return;
      }
      // TODO: Get rid of the vector.
      if (auto error
          = self->state().store->add(std::vector{std::move(slice)})) {
        self->quit(std::move(error));
      }
    },
    [self](atom::get) -> caf::result<std::vector<table_slice>> {
      auto result = std::vector<table_slice>{};
      for (auto slice : self->state().store->slices()) {
        result.push_back(std::move(slice));
      }
      return result;
    },
    [self](atom::status, status_verbosity, duration) {
      return record{
        {"events", self->state().store->num_events()},
        {"path", self->state().path.string()},
        {"store-type", self->state().store_type},
      };
    },
  };
}

} // namespace tenzir
