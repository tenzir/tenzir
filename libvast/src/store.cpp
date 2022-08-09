//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/store.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/query_context.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"

#include <caf/attach_stream_sink.hpp>

namespace vast {

namespace {

/// Handler for `vast::query` that is shared between active and passive stores.
/// @returns the number of events that match the query.
template <class Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const query_context& query_context,
              detail::generator<table_slice>&& slices) {
  // We're moving away from a global ID space; since new partitions can only
  // have a 1:1 mapping between partition and store, we no longer need to make
  // use of it and can simply evaluate the query for all events in this
  // store, as we already know it only affects events for its corresponding
  // partition. Additionally, we can assume homogeneous partitions here, so we
  // just take the first schema we get.
  auto tailored_expr = std::optional<expression>{};
  auto handle_query = detail::overload{
    [&](const query_context::count& count) -> caf::expected<uint64_t> {
      VAST_ASSERT(count.mode != query_context::count::estimate,
                  "estimate counts should not evaluate expressions");
      std::shared_ptr<arrow::RecordBatch> batch{};
      auto num_hits = uint64_t{};
      for (const auto& slice : slices) {
        if (!tailored_expr) {
          auto expr = tailor(query_context.expr, slice.layout());
          if (!expr)
            return expr.error();
          tailored_expr = std::move(*expr);
        }
        num_hits += count_matching(slice, *tailored_expr, query_context.ids);
      }
      self->send(count.sink, atom::receive_v, num_hits);
      return num_hits;
    },
    [&](const query_context::extract& extract) -> caf::expected<uint64_t> {
      auto num_hits = uint64_t{};
      for (const auto& slice : slices) {
        if (!tailored_expr) {
          auto expr = tailor(query_context.expr, slice.layout());
          if (!expr)
            return expr.error();
          tailored_expr = std::move(*expr);
        }
        auto final_slice = filter(slice, *tailored_expr, query_context.ids);
        if (final_slice) {
          num_hits += final_slice->rows();
          self->send(extract.sink, atom::receive_v, *final_slice);
        }
      }
      return num_hits;
    },
  };
  return caf::visit(handle_query, query_context.cmd);
}

} // namespace

caf::expected<uint64_t>
passive_store::lookup(system::store_actor::pointer self,
                      const query_context& query_context) const {
  return handle_lookup(self, query_context, slices());
}

size_t active_store::num_events() const {
  auto result = size_t{};
  for (const auto& slice : slices())
    result += slice.rows();
  return result;
}

caf::expected<uint64_t>
active_store::lookup(system::store_builder_actor::pointer self,
                     const query_context& query_context) const {
  return handle_lookup(self, query_context, slices());
}

system::store_actor::behavior_type default_passive_store(
  system::store_actor::stateful_pointer<default_passive_store_state> self,
  std::unique_ptr<passive_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant, std::filesystem::path path,
  std::string store_type) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.accountant = std::move(accountant);
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
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::expected<uint64_t> {
      const auto start = std::chrono::steady_clock::now();
      auto num_hits = self->state.store->lookup(self, query_context);
      if (!num_hits)
        return num_hits.error();
      const auto runtime = std::chrono::steady_clock::now() - start;
      const auto id_str = fmt::to_string(query_context.id);
      self->send(self->state.accountant,
                 fmt::format("{}.lookup.runtime", self->name()), runtime,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      self->send(self->state.accountant,
                 fmt::format("{}.lookup.hits", self->name()), *num_hits,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      return *num_hits;
    },
    [self](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state.store->num_events();
      VAST_ASSERT(rank(selection) == 0 || rank(selection) == num_events);
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
  };
}

system::store_builder_actor::behavior_type default_active_store(
  system::store_builder_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant, std::filesystem::path path,
  std::string store_type) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.accountant = std::move(accountant);
  self->state.store = std::move(store);
  self->state.path = std::move(path);
  self->state.store_type = std::move(store_type);
  return {
    [self](atom::query,
           const query_context& query_context) -> caf::expected<uint64_t> {
      const auto start = std::chrono::steady_clock::now();
      auto num_hits = self->state.store->lookup(self, query_context);
      if (!num_hits)
        return num_hits.error();
      const auto runtime = std::chrono::steady_clock::now() - start;
      const auto id_str = fmt::to_string(query_context.id);
      self->send(self->state.accountant,
                 fmt::format("{}.lookup.runtime", self->name()), runtime,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      self->send(self->state.accountant,
                 fmt::format("{}.lookup.hits", self->name()), *num_hits,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      return *num_hits;
    },
    [self](atom::erase, const ids& selection) -> caf::expected<uint64_t> {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = self->state.store->num_events();
      VAST_ASSERT(rank(selection) == 0 || rank(selection) == num_events);
      // We don't actually need to erase anything in the store itself, but
      // rather just don't need to persist when shutting down the stream, so we
      // set a flag for that in the actor state.
      self->state.erased = true;
      return num_events;
    },
    [self](caf::stream<table_slice> stream)
      -> caf::inbound_stream_slot<table_slice> {
      struct stream_state {
        // We intentionally store a strong reference here: The store lifetime
        // is ref-counted, it should exit after all currently active queries
        // for this store have finished, its partition has dropped out of the
        // cache, and it received all data from the incoming stream. This
        // pointer serves to keep the ref-count alive for the last part, and
        // is reset after the data has been written to disk.
        system::store_builder_actor strong_self;
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
          if (error) {
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
          self
            ->request(self->state.filesystem, caf::infinite, atom::write_v,
                      self->state.path, std::move(*chunk))
            .then(
              [self, stream_state](atom::ok) {
                static_cast<void>(stream_state);
                VAST_DEBUG("{} ({}) persisted itself to {}", *self,
                           self->state.store_type, self->state.path);
              },
              [self, stream_state](caf::error& error) {
                static_cast<void>(stream_state);
                self->quit(std::move(error));
              });
        });
      return attach_sink_result.inbound_slot();
    },
    [self](atom::status,
           [[maybe_unused]] system::status_verbosity verbosity) -> record {
      return {
        {"events", self->state.store->num_events()},
        {"path", self->state.path.string()},
        {"store-type", self->state.store_type},
      };
    },
  };
}

} // namespace vast
