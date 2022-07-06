//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/store.hpp"

#include "vast/error.hpp"
#include "vast/query.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"

namespace vast {

namespace {

/// Handler for `vast::query` that is shared between active and passive stores.
/// @returns the number of events that match the query.
template <class Actor>
caf::expected<uint64_t> handle_lookup(Actor& self, const query& query,
                                      const std::vector<table_slice>& slices) {
  if (slices.empty())
    return 0;
  // We're moving away from a global ID space; since new partitions can only
  // have a 1:1 mapping between partition and store, we no longer need to make
  // use of it and can simply evaluate the query for all events in this
  // store, as we already know it only affects events for its corresponding
  // partition. Additionally, we can assume homogeneous partitions here, so we
  // just take the first schema we get.
  auto expr = tailor(query.expr, slices.front().layout());
  if (!expr)
    return expr.error();
  auto handle_query = detail::overload{
    [&](const query::count& count) -> caf::expected<uint64_t> {
      VAST_ASSERT(count.mode != query::count::estimate,
                  "estimate counts should not evaluate expressions");
      std::shared_ptr<arrow::RecordBatch> batch{};
      auto num_hits = uint64_t{};
      for (const auto& slice : slices) {
        auto result = count_matching(slice, *expr, {});
        num_hits += result;
        self->send(count.sink, result);
      }
      return num_hits;
    },
    [&](const query::extract& extract) -> caf::expected<uint64_t> {
      auto num_hits = uint64_t{};
      for (const auto& slice : slices) {
        auto final_slice = filter(slice, *expr, {});
        if (final_slice) {
          num_hits += final_slice->rows();
          self->send(extract.sink, *final_slice);
        }
      }
      return num_hits;
    },
  };
  return caf::visit(handle_query, query.cmd);
}

} // namespace

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
    [self](const query& query) -> caf::expected<uint64_t> {
      const auto start = std::chrono::steady_clock::now();
      auto num_hits = handle_lookup(self, query, self->state.store->slices());
      if (!num_hits)
        return std::move(num_hits.error());
      const auto runtime = std::chrono::steady_clock::now() - start;
      const auto id_str = fmt::to_string(query.id);
      self->send(self->state.accountant, "passive-store.lookup.runtime",
                 runtime,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      self->send(self->state.accountant, "passive-store.lookup.hits", *num_hits,
                 system::metrics_metadata{
                   {"query", id_str},
                   {"store-type", self->state.store_type},
                 });
      return *num_hits;
    },
    [self](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // For new, partition-local stores we know that we always erase
      // everything.
      const auto num_events = rows(self->state.store->slices());
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
  system::accountant_actor accountant) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.accountant = std::move(accountant);
  self->state.store = std::move(store);
  return {
    [](const query& query) -> caf::result<uint64_t> {
      // FIXME: Handle query lookup
      return ec::unimplemented;
    },
    [](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // FIXME: Handle erasure
      return ec::unimplemented;
    },
    [](caf::stream<table_slice> stream)
      -> caf::inbound_stream_slot<table_slice> {
      return {};
    },
    [](atom::status, status_verbosity verbosity) -> caf::result<record> {
      return ec::unimplemented;
    },
  };
}

} // namespace vast
