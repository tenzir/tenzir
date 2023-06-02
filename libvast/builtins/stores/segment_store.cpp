//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/actors.hpp>
#include <vast/atoms.hpp>
#include <vast/detail/overload.hpp>
#include <vast/detail/zip_iterator.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/ids.hpp>
#include <vast/logger.hpp>
#include <vast/node_control.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/report.hpp>
#include <vast/segment.hpp>
#include <vast/status.hpp>
#include <vast/table_slice.hpp>

#include <caf/attach_stream_sink.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <flatbuffers/flatbuffers.h>
#include <fmt/format.h>

#include <chrono>
#include <span>
#include <vector>

namespace vast::plugins::segment_store {

namespace {

/// The STORE BUILDER actor interface.
using local_store_actor = typed_actor_fwd<
  // INTERNAL: Persist the actor.
  auto(atom::internal, atom::persist)->caf::result<void>>
  // Conform to the protocol of a STORE BUILDER actor.
  ::extend_with<store_builder_actor>::unwrap;

struct passive_store_state {
  /// Defaulted constructor to make this a non-aggregate.
  passive_store_state() = default;

  /// Holds requests that did arrive while the segment data
  /// was still being loaded from disk.
  using request
    = std::tuple<vast::query_context, caf::typed_response_promise<uint64_t>>;
  std::vector<request> deferred_requests = {};

  /// Holds erase requests that did arrive while the segment data
  /// was still being loaded from disk.
  using erasure = std::tuple<vast::ids, caf::typed_response_promise<uint64_t>>;
  std::vector<erasure> deferred_erasures = {};

  /// Actor handle of the accountant.
  accountant_actor accountant = {};

  /// The actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The path where the segment is stored.
  std::filesystem::path path = {};

  /// The segment corresponding to this local store.
  caf::optional<vast::segment> segment = {};

  /// A readable name for this active store.
  std::string name = {};

private:
  caf::result<atom::done> erase(const vast::ids&);
};

// Handler for `vast::query` that is shared between active and passive stores.
// Returns a the number of events that match the query.
template <typename Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const vast::query_context& query_context,
              const std::vector<table_slice>& slices) {
  const auto& ids = query_context.ids;
  std::vector<expression> checkers;
  uint64_t num_hits = 0ull;
  for (const auto& slice : slices) {
    if (query_context.expr == expression{}) {
      checkers.emplace_back();
    } else {
      auto c = tailor(query_context.expr, slice.schema());
      if (!c)
        return c.error();
      checkers.emplace_back(prune_meta_predicates(std::move(*c)));
    }
  }
  auto handle_query = detail::overload{
    [&](const count_query_context& count) {
      if (count.mode == count_query_context::estimate)
        die("logic error detected");
      for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices.at(i);
        const auto& checker = checkers.at(i);
        auto result = count_matching(slice, checker, ids);
        num_hits += result;
        self->send(count.sink, result);
      }
    },
    [&](const extract_query_context& extract) {
      VAST_ASSERT(slices.size() == checkers.size());
      for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        const auto& checker = checkers[i];
        auto final_slice = filter(slice, checker, ids);
        if (final_slice) {
          num_hits += final_slice->rows();
          self->send(extract.sink, *final_slice);
        }
      }
    },
  };
  caf::visit(handle_query, query_context.cmd);
  return num_hits;
}

std::filesystem::path
store_path_from_header(std::span<const std::byte> header) {
  std::string_view sv{reinterpret_cast<const char*>(header.data()),
                      header.size()};
  return std::filesystem::path{sv};
}

store_actor::behavior_type
passive_local_store(store_actor::stateful_pointer<passive_store_state> self,
                    accountant_actor accountant, filesystem_actor fs,
                    const std::filesystem::path& path) {
  const auto start = std::chrono::steady_clock::now();
  self->state.accountant = std::move(accountant);
  self->state.fs = std::move(fs);
  self->state.path = path;
  self->state.name = "passive-store";
  self->set_exit_handler([self](const caf::exit_msg&) {
    for (auto&& [expr, rp] : std::exchange(self->state.deferred_requests, {}))
      rp.deliver(caf::make_error(ec::lookup_error, "partition store shutting "
                                                   "down"));
    for (auto&& [expr, rp] : std::exchange(self->state.deferred_erasures, {}))
      rp.deliver(caf::make_error(ec::lookup_error, "partition store shutting "
                                                   "down"));
  });
  VAST_DEBUG("{} loads passive store from path {}", *self, self->state.path);
  self->request(self->state.fs, caf::infinite, atom::mmap_v, self->state.path)
    .then(
      [self, start](chunk_ptr chunk) {
        auto seg = segment::make(std::move(chunk));
        if (!seg) {
          VAST_ERROR("{} couldn't create segment from chunk: {}", *self,
                     seg.error());
          self->send_exit(self,
                          caf::make_error(vast::ec::format_error,
                                          fmt::format("{} failed to create "
                                                      "segment from chunk: {}",
                                                      *self, seg.error())));
          return;
        }
        self->state.segment = std::move(*seg);
        self->state.name
          = fmt::format("passive-store-{}", self->state.segment->id());
        // Delegate all deferred evaluations now that we have the partition chunk.
        VAST_DEBUG("{} delegates {} deferred evaluations", *self,
                   self->state.deferred_requests.size());
        for (auto&& [query, rp] :
             std::exchange(self->state.deferred_requests, {})) {
          VAST_TRACE("{} delegates {} (pending: {})", *self, query,
                     rp.pending());
          rp.delegate(static_cast<store_actor>(self), atom::query_v,
                      std::move(query));
        }
        for (auto&& [ids, rp] :
             std::exchange(self->state.deferred_erasures, {})) {
          VAST_TRACE("{} delegates erase (pending: {})", *self, rp.pending());
          rp.delegate(static_cast<store_actor>(self), atom::erase_v,
                      std::move(ids));
        }
        auto startup_duration = std::chrono::steady_clock::now() - start;
        self->send(self->state.accountant, atom::metrics_v,
                   "passive-store.init.runtime", startup_duration,
                   metrics_metadata{
                     {"store-type", "segment-store"},
                   });
      },
      [self](caf::error& err) {
        VAST_ERROR("{} could not map passive store segment into memory: {}",
                   *self, render(err));
        for (auto&& [_, rp] : std::exchange(self->state.deferred_requests, {}))
          rp.deliver(err);
        for (auto&& [_, rp] : std::exchange(self->state.deferred_erasures, {}))
          rp.deliver(err);
        self->quit(std::move(err));
      });
  return {
    [self](atom::query, query_context query_context) -> caf::result<uint64_t> {
      VAST_DEBUG("{} handles new query {}", *self, query_context.id);
      if (!self->state.segment) {
        auto rp = self->make_response_promise<uint64_t>();
        self->state.deferred_requests.emplace_back(query_context, rp);
        return rp;
      }
      auto start = std::chrono::steady_clock::now();
      auto slices = self->state.segment->lookup(query_context.ids);
      if (!slices)
        return slices.error();
      auto num_hits = handle_lookup(self, query_context, *slices);
      if (!num_hits)
        return num_hits.error();
      duration runtime = std::chrono::steady_clock::now() - start;
      auto id_str = fmt::to_string(query_context.id);
      self->send(self->state.accountant, atom::metrics_v,
                 "passive-store.lookup.runtime", runtime,
                 metrics_metadata{
                   {"query", id_str},
                   {"issuer", query_context.issuer},
                   {"store-type", "segment-store"},
                 });
      self->send(self->state.accountant, atom::metrics_v,
                 "passive-store.lookup.hits", *num_hits,
                 metrics_metadata{
                   {"query", id_str},
                   {"issuer", query_context.issuer},
                   {"store-type", "segment-store"},
                 });
      return *num_hits;
    },
    [self](atom::erase, ids xs) -> caf::result<uint64_t> {
      if (!self->state.segment) {
        // Treat this as an "erase" query for the purposes of storing it
        // until the segment is loaded.
        auto rp = self->make_response_promise<uint64_t>();
        self->state.deferred_erasures.emplace_back(std::move(xs), rp);
        return rp;
      }
      auto segment_ids = self->state.segment->ids();
      auto segment_size = rank(segment_ids);
      auto intersection = segment_ids & xs;
      auto intersection_size = rank(intersection);
      VAST_DEBUG("{} erases {} of {} events", *self, intersection_size,
                 segment_size);
      if (!is_subset(self->state.segment->ids(), xs))
        return caf::make_error(
          ec::logic_error, "attempted to partially erase a partition-local "
                           "segment store, which is an unsupported operation");
      VAST_VERBOSE("{} gets wholly erased from {}", *self, self->state.path);
      // There is a (small) chance one or more lookups are currently still in
      // progress, so we dont call `self->quit()` here but instead rely on
      // ref-counting. The lookups can still finish normally because the
      // `mmap()` is still valid even after the underlying segment file was
      // removed.
      auto rp = self->make_response_promise<uint64_t>();
      self
        ->request(self->state.fs, caf::infinite, atom::erase_v,
                  self->state.path)
        .then(
          [rp, intersection_size](atom::done) mutable {
            rp.deliver(intersection_size);
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
  };
}

class plugin final : public virtual store_actor_plugin {
public:
  using store_actor_plugin::builder_and_header;

  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "segment-store";
  };

  // store plugin API
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder([[maybe_unused]] accountant_actor accountant,
                     [[maybe_unused]] filesystem_actor fs,
                     [[maybe_unused]] const vast::uuid& id) const override {
    return caf::make_error(ec::logic_error, "segment-store plugin is read-only "
                                            "since VAST v2.4");
  }

  [[nodiscard]] caf::expected<store_actor>
  make_store(accountant_actor accountant, filesystem_actor fs,
             std::span<const std::byte> header) const override {
    auto path = store_path_from_header(header);
    return fs->home_system().spawn<caf::lazy_init>(passive_local_store,
                                                   accountant, fs, path);
  }
};

} // namespace

} // namespace vast::plugins::segment_store

VAST_REGISTER_PLUGIN(vast::plugins::segment_store::plugin)
