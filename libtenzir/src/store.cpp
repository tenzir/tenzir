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

#include <caf/attach_stream_sink.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

namespace {

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
  TENZIR_TRACE("{} got a query: {}", *self, query_context);
  const auto start = std::chrono::steady_clock::now();
  const auto schema = self->state.store->schema();
  const auto tailored_expr = tailor(query_context.expr, schema);
  if (!tailored_expr) {
    // In case the query was delegated from an active partition the
    // taxonomy resolution is not guaranteed to have worked (whenever the
    // type in this store did not exist in the catalog when the partition
    // was created). In that case we simply discard the query.
    if constexpr (std::is_same_v<default_active_store_actor, Actor>)
      return 0ull;
    return caf::make_error(ec::invalid_query,
                           fmt::format("{} failed to tailor '{}' to '{}'",
                                       *self, query_context.expr, schema));
  }
  auto rp = self->template make_response_promise<uint64_t>();
  auto f = detail::overload{
    [&](const extract_query_context& extract) -> void {
      self->monitor(extract.sink);
      auto [state, inserted] = self->state.running_extractions.try_emplace(
        query_context.id, extract_query_state{});
      if (!inserted) {
        rp.deliver(caf::make_error(
          ec::logic_error, fmt::format("{} received duplicated query id {}",
                                       *self, query_context.id)));
        return;
      }
      state->second.result_generator
        = self->state.store->extract(*tailored_expr, query_context.ids);
      state->second.result_iterator = state->second.result_generator.begin();
      state->second.sink = extract.sink;
      state->second.start = start;
      self
        ->request(static_cast<Actor>(self), caf::infinite, atom::internal_v,
                  atom::extract_v, query_context.id)
        .then(
          [self, issuer = query_context.issuer, query_id = query_context.id,
           rp]() mutable {
            TENZIR_DEBUG("{} finished working on extract query {}", *self,
                         query_id);
            auto it = self->state.running_extractions.find(query_id);
            if (it == self->state.running_extractions.end()) {
              TENZIR_DEBUG("{} cancelled extract query {}", *self, query_id);
              rp.deliver(uint64_t{0});
              return;
            }
            rp.deliver(it->second.num_hits);
            self->state.running_extractions.erase(it);
          },
          [self, expr = query_context.expr, query_id = query_context.id,
           rp](caf::error& err) mutable {
            TENZIR_WARN("{} failed to execute extract query {}: {}", *self,
                        query_id, err);
            rp.deliver(caf::make_error(
              ec::unspecified, fmt::format("{} failed to complete extract "
                                           "query '{}': {}",
                                           *self, expr, std::move(err))));
          });
    },
  };
  caf::visit(f, query_context.cmd);
  return rp;
}

auto remove_down_source(auto* self, const caf::down_msg& down_msg) {
  for (const auto& [query_id, state] : self->state.running_extractions) {
    if (state.sink->address() == down_msg.source) {
      TENZIR_DEBUG("{} received DOWN from extract query {}: {}", *self,
                   query_id, down_msg.reason);
      self->state.running_extractions.erase(query_id);
      break; // a sink can only have one active extract query, so we stop
    }
  }
  for (const auto& [query_id, state] : self->state.running_counts) {
    if (state.sink->address() == down_msg.source) {
      TENZIR_DEBUG("{} received DOWN from count query {}: {}", *self, query_id,
                   down_msg.reason);
      self->state.running_counts.erase(query_id);
      break; // a sink can only have one active count query, so we stop
    }
  }
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

generator<table_slice>
base_store::extract(expression expr, ids selection) const {
  for (const auto& slice : slices()) {
    if (auto filtered_slice = filter(slice, expr, selection))
      co_yield std::move(*filtered_slice);
  }
}

default_passive_store_actor::behavior_type default_passive_store(
  default_passive_store_actor::stateful_pointer<default_passive_store_state>
    self,
  std::unique_ptr<passive_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.store = std::move(store);
  self->state.path = std::move(path);
  self->state.store_type = std::move(store_type);
  // Load data from disk.
  self
    ->request(self->state.filesystem, caf::infinite, atom::mmap_v,
              self->state.path)
    .await(
      [self](chunk_ptr& chunk) {
        auto load_error = self->state.store->load(std::move(chunk));
        if (load_error)
          self->quit(std::move(load_error));
      },
      [self](caf::error& error) {
        self->quit(std::move(error));
      });
  // We monitor all query sinks, and remove queries associated with the sink.
  self->set_down_handler([self](const caf::down_msg& down_msg) {
    remove_down_source(self, down_msg);
  });
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::result<uint64_t> {
      TENZIR_DEBUG("{} starts working on query {}", *self, query_context.id);
      return handle_query<default_passive_store_actor>(self, query_context);
    },
    [self](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state.store->num_events();

      TENZIR_DEBUG("{} erases {} events", *self, num_events);
      TENZIR_ASSERT_EXPENSIVE(rank(selection) == 0
                              || rank(selection) == num_events);
      auto rp = self->make_response_promise<uint64_t>();
      self
        ->request(self->state.filesystem, caf::infinite, atom::erase_v,
                  self->state.path)
        .then(
          [rp, num_events](atom::done) mutable {
            rp.deliver(num_events);
          },
          [rp](caf::error& error) mutable {
            rp.deliver(std::move(error));
          });
      return rp;
    },
    [self](atom::internal, atom::extract,
           const uuid& query_id) -> caf::result<void> {
      TENZIR_DEBUG("{} continuous working on extract query {}", *self,
                   query_id);
      auto it = self->state.running_extractions.find(query_id);
      if (it == self->state.running_extractions.end())
        return {};
      auto& [_, state] = *it;
      if (state.result_iterator == state.result_generator.end()) {
        TENZIR_DEBUG("{} ignores extract continuation request for query {} "
                     "after "
                     "query already finished",
                     *self, query_id);
        return {};
      }
      auto slice = *state.result_iterator;
      state.num_hits += slice.rows();
      self->send(state.sink, std::move(slice));
      if (++state.result_iterator == state.result_generator.end()) {
        return {};
      }
      return self->delegate(static_cast<default_passive_store_actor>(self),
                            atom::internal_v, atom::extract_v, query_id);
    },
    [self](atom::internal, atom::count,
           const uuid& query_id) -> caf::result<void> {
      TENZIR_DEBUG("{} continuous working on count query {}", *self, query_id);
      auto it = self->state.running_counts.find(query_id);
      if (it == self->state.running_counts.end())
        return {};
      auto& [_, state] = *it;
      if (state.result_iterator == state.result_generator.end()) {
        TENZIR_DEBUG("{} ignores count continuation request for query {} after "
                     "query already finished",
                     *self, query_id);
        return {};
      }
      state.num_hits += *state.result_iterator;
      if (++state.result_iterator == state.result_generator.end()) {
        return {};
      }
      return self->delegate(static_cast<default_passive_store_actor>(self),
                            atom::internal_v, atom::count_v, query_id);
    },
  };
}

default_active_store_actor::behavior_type default_active_store(
  default_active_store_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, filesystem_actor filesystem,
  std::filesystem::path path, std::string store_type) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.store = std::move(store);
  self->state.path = std::move(path);
  self->state.store_type = std::move(store_type);
  self->set_down_handler([self](const caf::down_msg& down_msg) {
    remove_down_source(self, down_msg);
  });
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::result<uint64_t> {
      TENZIR_DEBUG("{} starts working on query {}", *self, query_context.id);
      if (self->state.store->num_events() == 0)
        return 0ull;
      return handle_query<default_active_store_actor>(self, query_context);
    },
    [self](atom::erase, const ids& selection) {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state.store->num_events();
      TENZIR_ASSERT_EXPENSIVE(rank(selection) == 0
                              || rank(selection) == num_events);
      // We don't actually need to erase anything in the store itself, but
      // rather just don't need to persist when shutting down the stream, so
      // we set a flag for that in the actor state.
      self->state.erased = true;
      return num_events;
    },
    [self](atom::persist) -> caf::result<resource> {
      if (const auto* res = std::get_if<resource>(&self->state.file)) {
        return *res;
      }
      auto rp = self->make_response_promise<resource>();
      self->state.file = rp;
      return rp;
    },
    [self](table_slice& slice) {
      if (self->state.erased) {
        return;
      }
      // TODO: Get rid of the vector.
      if (auto error = self->state.store->add(std::vector{std::move(slice)})) {
        self->quit(std::move(error));
      }
    },
    [self](caf::stream<table_slice> stream)
      -> caf::result<caf::inbound_stream_slot<table_slice>> {
      struct stream_state {
        // We intentionally store a strong reference here: The store lifetime
        // is ref-counted, it should exit after all currently active queries
        // for this store have finished, its partition has dropped out of the
        // cache, and it received all data from the incoming stream. This
        // pointer serves to keep the ref-count alive for the last part, and
        // is reset after the data has been written to disk.
        default_active_store_actor strong_self;
      };
      auto attach_sink_result = caf::attach_stream_sink(
        self, stream,
        [self](stream_state& stream_state) {
          stream_state.strong_self = self;
        },
        [self]([[maybe_unused]] stream_state& stream_state,
               std::vector<table_slice>& slices) {
          // If the store is marked for erasure we don't actually need to
          // add any further slices.
          if (self->state.erased)
            return;
          if (auto error = self->state.store->add(std::move(slices)))
            self->quit(std::move(error));
        },
        [self](stream_state& stream_state, const caf::error& error) {
          if (ec::end_of_input == error) {
            TENZIR_WARN("store encountered unexpected end of input before "
                        "shutdown");
          } else if (error) {
            self->quit(error);
            return;
          }
          // If the store is marked for erasure we don't actually need to
          // persist anything.
          if (self->state.erased)
            return;
          auto chunk = self->state.store->finish();
          if (!chunk) {
            self->quit(std::move(chunk.error()));
            return;
          }
          std::visit(detail::overload(
                       [&](std::monostate) {
                         self->state.file = resource{
                           .url = fmt::format("file://{}", self->state.path),
                           .size = (*chunk)->size(),
                         };
                       },
                       [&](const resource&) {
                         TENZIR_ASSERT(false);
                       },
                       [&](caf::typed_response_promise<resource>& rp) {
                         TENZIR_ASSERT(rp.pending());
                         rp.deliver(resource{
                           .url = fmt::format("file://{}", self->state.path),
                           .size = (*chunk)->size(),
                         });
                       }),
                     self->state.file);
          self
            ->request(self->state.filesystem, caf::infinite, atom::write_v,
                      self->state.path, std::move(*chunk))
            .then(
              [self, stream_state](atom::ok) {
                static_cast<void>(stream_state);
                TENZIR_DEBUG("{} ({}) persisted itself to {}", *self,
                             self->state.store_type, self->state.path);
              },
              [self, stream_state](caf::error& error) {
                static_cast<void>(stream_state);
                self->quit(std::move(error));
              });
        });
      return attach_sink_result.inbound_slot();
    },
    [self](atom::status, status_verbosity, duration) {
      return record{
        {"events", self->state.store->num_events()},
        {"path", self->state.path.string()},
        {"store-type", self->state.store_type},
      };
    },
    [self](atom::internal, atom::extract,
           const uuid& query_id) -> caf::result<void> {
      TENZIR_DEBUG("{} continuous working on extract query {}", *self,
                   query_id);
      auto it = self->state.running_extractions.find(query_id);
      if (it == self->state.running_extractions.end())
        return {};
      auto& [_, state] = *it;
      if (state.result_iterator == state.result_generator.end()) {
        TENZIR_DEBUG("{} ignores extract continuation request for query {} "
                     "after "
                     "query already finished",
                     *self, query_id);
        return {};
      }
      auto slice = *state.result_iterator;
      state.num_hits += slice.rows();
      self->send(state.sink, std::move(slice));
      if (++state.result_iterator == state.result_generator.end()) {
        return {};
      }
      return self->delegate(static_cast<default_active_store_actor>(self),
                            atom::internal_v, atom::extract_v, query_id);
    },
    [self](atom::internal, atom::count,
           const uuid& query_id) -> caf::result<void> {
      TENZIR_DEBUG("{} continuous working on count query {}", *self, query_id);
      auto it = self->state.running_counts.find(query_id);
      if (it == self->state.running_counts.end())
        return {};
      auto& [_, state] = *it;
      if (state.result_iterator == state.result_generator.end()) {
        TENZIR_DEBUG("{} ignores count continuation request for query {} after "
                     "query already finished",
                     *self, query_id);
        return {};
      }
      state.num_hits += *state.result_iterator;
      if (++state.result_iterator == state.result_generator.end()) {
        return {};
      }
      return self->delegate(static_cast<default_active_store_actor>(self),
                            atom::internal_v, atom::count_v, query_id);
    },
  };
}

} // namespace tenzir
