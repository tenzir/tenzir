//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/local_segment_store.hpp"

#include "vast/atoms.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/query.hpp"
#include "vast/segment_store.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"

#include <caf/settings.hpp>
#include <flatbuffers/flatbuffers.h>
#include <fmt/format.h>

#include <span>
#include <vector>

namespace vast::system {

namespace {

// Handler for `vast::query` that is shared between active and passive stores.
// Precondition: Query type is either `count` or `extract`.
template <typename Actor>
caf::result<atom::done> handle_lookup(Actor& self, const vast::query& query,
                                      const std::vector<table_slice>& slices) {
  const auto& ids = query.ids;
  std::vector<expression> checkers;
  for (const auto& slice : slices) {
    if (query.expr == expression{}) {
      checkers.emplace_back();
    } else {
      auto c = tailor(query.expr, slice.layout());
      if (!c)
        return c.error();
      checkers.emplace_back(prune_meta_predicates(std::move(*c)));
    }
  }
  auto handle_query = detail::overload{
    [&](const query::count& count) {
      if (count.mode == query::count::estimate)
        die("logic error detected");
      for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices.at(i);
        const auto& checker = checkers.at(i);
        auto result = count_matching(slice, checker, ids);
        self->send(count.sink, result);
      }
    },
    [&](const query::extract& extract) {
      VAST_ASSERT(slices.size() == checkers.size());
      for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        const auto& checker = checkers[i];
        if (extract.policy == query::extract::preserve_ids) {
          for (auto& sub_slice : select(slice, ids)) {
            if (query.expr == expression{}) {
              self->send(extract.sink, sub_slice);
            } else {
              auto hits = evaluate(checker, sub_slice);
              for (auto& final_slice : select(sub_slice, hits))
                self->send(extract.sink, final_slice);
            }
          }
        } else {
          auto final_slice = filter(slice, checker, ids);
          if (final_slice)
            self->send(extract.sink, *final_slice);
        }
      }
    },
    [&](query::erase) {
      // The caller should have special-cased this before calling.
      VAST_ASSERT(false, "cant lookup an 'erase' query");
    },
  };
  caf::visit(handle_query, query.cmd);
  return atom::done_v;
}

std::filesystem::path
store_path_from_header(std::span<const std::byte> header) {
  std::string_view sv{reinterpret_cast<const char*>(header.data()),
                      header.size()};
  return std::filesystem::path{sv};
}

} // namespace

std::filesystem::path store_path_for_partition(const uuid& partition_id) {
  auto store_filename = fmt::format("{}.store", partition_id);
  return std::filesystem::path{"archive"} / store_filename;
}

store_actor::behavior_type
passive_local_store(store_actor::stateful_pointer<passive_store_state> self,
                    filesystem_actor fs, const std::filesystem::path& path) {
  self->state.fs = std::move(fs);
  self->state.path = path;
  self->set_exit_handler([self](const caf::exit_msg&) {
    for (auto&& [expr, rp] : std::exchange(self->state.deferred_requests, {}))
      rp.deliver(caf::make_error(ec::lookup_error, "partition store shutting "
                                                   "down"));
  });
  VAST_DEBUG("loading passive store from path {}", path);
  self->request(self->state.fs, caf::infinite, atom::mmap_v, path)
    .then(
      [self](chunk_ptr chunk) {
        auto seg = segment::make(std::move(chunk));
        if (!seg) {
          VAST_ERROR("couldnt create segment from chunk: {}", seg.error());
          self->send_exit(self, caf::exit_reason::unhandled_exception);
          return;
        }
        self->state.segment = std::move(*seg);
        // Delegate all deferred evaluations now that we have the partition chunk.
        VAST_DEBUG("{} delegates {} deferred evaluations", *self,
                   self->state.deferred_requests.size());
        for (auto&& [query, rp] :
             std::exchange(self->state.deferred_requests, {})) {
          VAST_TRACE("{} delegates {} (pending: {})", *self, query,
                     rp.pending());
          rp.delegate(static_cast<store_actor>(self), std::move(query));
        }
      },
      [self](caf::error& err) {
        VAST_ERROR("{} could not map passive store segment into memory: {}",
                   *self, render(err));
        for (auto&& [_, rp] : std::exchange(self->state.deferred_requests, {}))
          rp.deliver(err);
        self->quit(std::move(err));
      });
  return {
    [self](query query) -> caf::result<atom::done> {
      VAST_DEBUG("{} handles new query", *self);
      if (!self->state.segment) {
        auto rp = self->make_response_promise<atom::done>();
        self->state.deferred_requests.emplace_back(query, rp);
        return rp;
      }
      // Special-case handling for "erase"-queries because their
      // implementation must be different depending on if we operate
      // in memory or on disk.
      if (caf::holds_alternative<query::erase>(query.cmd)) {
        return self->delegate(static_cast<store_actor>(self), atom::erase_v,
                              query.ids);
      }
      auto slices = self->state.segment->lookup(query.ids);
      if (!slices)
        return slices.error();
      return handle_lookup(self, query, *slices);
    },
    [self](atom::erase, ids xs) -> caf::result<atom::done> {
      if (!self->state.segment) {
        // Treat this as an "erase" query for the purposes of storing it
        // until the segment is loaded.
        auto rp = self->make_response_promise<atom::done>();
        auto query = query::make_erase({});
        query.ids = std::move(xs);
        self->state.deferred_requests.emplace_back(query, rp);
        return rp;
      }
      VAST_DEBUG("{} erases {} of {} events", *self,
                 rank(self->state.segment->ids() - xs),
                 rank(self->state.segment->ids()));
      if (is_subset(self->state.segment->ids(), xs)) {
        VAST_VERBOSE("{} gets wholly erased from {}", *self, self->state.path);
        // There is a (small) chance one or more lookups are currently still in
        // progress, so we dont call `self->quit()` here but instead rely on
        // ref-counting. The lookups can still finish normally because the
        // `mmap()` is still valid even after the underlying segment file was
        // removed.
        return self->delegate(self->state.fs, atom::erase_v, self->state.path);
      }
      auto new_segment = segment::copy_without(*self->state.segment, xs);
      if (!new_segment) {
        VAST_ERROR("could not remove ids from segment {}: {}",
                   self->state.segment->id(), render(new_segment.error()));
        return new_segment.error();
      }
      VAST_ASSERT(self->state.path.has_filename());
      auto old_path = self->state.path;
      auto new_path = self->state.path.replace_extension("next");
      auto rp = self->make_response_promise<atom::done>();
      self
        ->request(self->state.fs, caf::infinite, atom::write_v, new_path,
                  new_segment->chunk())
        .then(
          [seg = std::move(*new_segment), self, old_path, new_path,
           rp](atom::ok) mutable {
            std::error_code ec;
            // Re-use the old filename so that we don't have to write a new
            // partition flatbuffer with the changed store header as well.
            std::filesystem::rename(new_path, old_path, ec);
            if (ec)
              VAST_ERROR("failed to erase old data {}", seg.id());
            self->state.segment = std::move(seg);
            rp.deliver(atom::done_v);
          },
          [rp](caf::error& err) mutable {
            VAST_ERROR("failed to flush archive {}", to_string(err));
            rp.deliver(err);
          });
      return rp;
    },
  };
}

local_store_actor::behavior_type
active_local_store(local_store_actor::stateful_pointer<active_store_state> self,
                   filesystem_actor fs, const std::filesystem::path& path) {
  VAST_DEBUG("spawning active local store");
  self->state.self = self;
  self->state.fs = std::move(fs);
  self->state.path = path;
  self->state.builder
    = std::make_unique<segment_builder>(defaults::system::max_segment_size);
  self->set_exit_handler([self](const caf::exit_msg&) {
    VAST_DEBUG("active local store exits");
    // TODO: We should save the finished segment in the state, so we can
    //       answer queries that arrive after the stream has ended.
    self->quit();
  });
  auto result = local_store_actor::behavior_type{
    // store api
    [self](const query& query) -> caf::result<atom::done> {
      caf::expected<std::vector<table_slice>> slices = caf::error{};
      if (self->state.builder) {
        slices = self->state.builder->lookup(query.ids);
      } else {
        VAST_ASSERT(self->state.segment.has_value());
        slices = self->state.segment->lookup(query.ids);
      }
      if (!slices)
        return slices.error();
      if (caf::holds_alternative<query::erase>(query.cmd)) {
        return self->delegate(static_cast<store_actor>(self), atom::erase_v,
                              query.ids);
      }
      return handle_lookup(self, query, *slices);
    },
    [self](const atom::erase&, const ids& ids) -> caf::result<atom::done> {
      // TODO: There is a race here when ids are erased while we're waiting
      // for the filesystem actor to finish.
      auto seg = self->state.builder->finish();
      auto id = seg.id();
      auto slices = seg.erase(ids);
      if (!slices)
        return slices.error();
      self->state.builder->reset(id);
      for (auto&& slice : std::exchange(*slices, {}))
        self->state.builder->add(std::move(slice));
      return atom::done_v;
    },
    // store builder
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      return self
        ->make_sink(
          in, [=](caf::unit_t&) {},
          [=](caf::unit_t&, std::vector<table_slice>& batch) {
            VAST_TRACE("{} gets batch of {} table slices", *self, batch.size());
            for (auto& slice : batch) {
              if (auto error = self->state.builder->add(slice))
                VAST_ERROR("{} failed to add table slice to store {}", *self,
                           render(error));
              self->state.events += slice.rows();
            }
          },
          [=](caf::unit_t&, const caf::error&) {
            VAST_DEBUG("{} stream shuts down", *self);
            self->send(self, atom::internal_v, atom::persist_v);
          })
        .inbound_slot();
    },
    // Conform to the protocol of the STATUS CLIENT actor.
    [self](atom::status, status_verbosity) -> record {
      auto result = record{};
      auto store = record{};
      store["events"] = count{self->state.events};
      store["path"] = self->state.path.string();
      result["segment-store"] = std::move(store);
      return result;
    },
    // internal handlers
    [self](atom::internal, atom::persist) {
      self->state.segment = self->state.builder->finish();
      VAST_DEBUG("persisting segment {}", self->state.segment->id());
      self
        ->request(self->state.fs, caf::infinite, atom::write_v,
                  self->state.path, self->state.segment->chunk())
        .then(
          [self](atom::ok) {
            self->state.self = nullptr;
          },
          [self](caf::error& err) {
            VAST_ERROR("failed to flush archive {}", to_string(err));
            self->state.self = nullptr;
          });
      self->state.fs = nullptr;
    },
  };
  return result;
}

class local_store_plugin final : public virtual store_plugin {
public:
  using store_plugin::builder_and_header;

  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "segment-store";
  };

  // store plugin API
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(filesystem_actor fs, const vast::uuid& id) const override {
    auto path = store_path_for_partition(id);
    std::string path_str = path.string();
    auto header = chunk::make(std::move(path_str));
    auto builder = fs->home_system().spawn(active_local_store, fs, path);
    return builder_and_header{static_cast<store_builder_actor&&>(builder),
                              std::move(header)};
  }

  [[nodiscard]] caf::expected<system::store_actor>
  make_store(filesystem_actor fs,
             std::span<const std::byte> header) const override {
    auto path = store_path_from_header(header);
    // TODO: This should use `spawn<caf::lazy_init>`, but this leads to a
    // deadlock in unit tests.
    return fs->home_system().spawn(passive_local_store, fs, path);
  }
};

VAST_REGISTER_PLUGIN(vast::system::local_store_plugin)

} // namespace vast::system
