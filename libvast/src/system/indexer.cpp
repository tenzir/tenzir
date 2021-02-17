/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/indexer.hpp"

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/value_index.hpp"
#include "vast/value_index_factory.hpp"
#include "vast/view.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/binary_serializer.hpp>

#include <flatbuffers/flatbuffers.h>

namespace vast::system {

namespace {

vast::chunk_ptr chunkify(const value_index_ptr& idx) {
  std::vector<char> buf;
  caf::binary_serializer sink{nullptr, buf};
  auto error = sink(idx);
  if (error)
    return nullptr;
  return chunk::make(std::move(buf));
}

} // namespace

active_indexer_actor::behavior_type
active_indexer(active_indexer_actor::stateful_pointer<indexer_state> self,
               type index_type, caf::settings index_opts) {
  self->state.name = "indexer-" + to_string(index_type);
  self->state.has_skip_attribute = vast::has_skip_attribute(index_type);
  self->state.idx = factory<value_index>::make(index_type, index_opts);
  if (!self->state.idx) {
    VAST_ERROR("{} failed to construct value index", self);
    self->quit(caf::make_error(ec::unspecified, "failed to construct value "
                                                "index"));
    return active_indexer_actor::behavior_type::make_empty_behavior();
  }
  return {
    [self](caf::stream<table_slice_column> in)
      -> caf::inbound_stream_slot<table_slice_column> {
      VAST_DEBUG("{} got a new stream", self);
      self->state.stream_initiated = true;
      auto result = caf::attach_stream_sink(
        self, in,
        [=](caf::unit_t&) {
          VAST_DEBUG("{} initializes stream", self);
          // nop
        },
        [=](caf::unit_t&, const std::vector<table_slice_column>& columns) {
          VAST_ASSERT(self->state.idx != nullptr);
          VAST_WARN("{} received table slice column", self);
          // NOTE: It seems like having the `#skip` attribute should lead to
          // no index being created at all (as opposed to creating it and
          // never adding data), but that was the behaviour of the previous
          // implementation so we're keeping it for now.
          if (self->state.has_skip_attribute)
            return;
          for (auto& column : columns)
            for (size_t i = 0; i < column.size(); ++i)
              self->state.idx->append(column[i], column.slice().offset() + i);
        },
        [=](caf::unit_t&, const caf::error& err) {
          VAST_TRACE("indexer is closing stream");
          if (err) {
            // Exit reason `unreachable` means that the actor has exited,
            // so we can't safely use `self` anymore.
            // TODO: We also need to deliver the promise here *if* self
            // still exists and the promise is already pending.
            VAST_ERROR("indexer got a stream error: {}", render(err));
            return;
          }
          if (self->state.promise.pending())
            self->state.promise.deliver(chunkify(self->state.idx));
        });
      return result.inbound_slot();
    },
    [self](const curried_predicate& pred) {
      VAST_DEBUG("{} got predicate: {}", self, pred);
      VAST_ASSERT(self->state.idx);
      auto& idx = *self->state.idx;
      auto rep = to_internal(idx.type(), make_view(pred.rhs));
      return idx.lookup(pred.op, rep);
    },
    [self](atom::snapshot) {
      // The partition is only allowed to send a single snapshot atom.
      VAST_ASSERT(!self->state.promise.pending());
      self->state.promise = self->make_response_promise<chunk_ptr>();
      // Checking 'idle()' is not enough, since we emprically can
      // have data that was flushed in the upstream stage but is not
      // yet visible to the sink.
      if (self->state.stream_initiated
          && (self->stream_managers().empty()
              || self->stream_managers().begin()->second->done())) {
        self->state.promise.deliver(chunkify(self->state.idx));
      }
      return self->state.promise;
    },
    [self](atom::shutdown) {
      self->quit(caf::exit_reason::user_shutdown); // clang-format fix
    },
    [self](atom::status, status_verbosity) {
      caf::settings result;
      put(result, "memory-usage", self->state.idx->memusage());
      return result;
    },
  };
}

indexer_actor::behavior_type
passive_indexer(indexer_actor::stateful_pointer<indexer_state> self,
                uuid partition_id, value_index_ptr idx) {
  if (!idx) {
    VAST_ERROR("{} got invalid value index pointer", self);
    self->quit(caf::make_error(ec::end_of_input, "invalid value index "
                                                 "pointer"));
    return indexer_actor::behavior_type::make_empty_behavior();
  }
  self->state.name = "indexer-" + to_string(idx->type());
  self->state.partition_id = partition_id;
  self->state.idx = std::move(idx);
  return {
    [self](const curried_predicate& pred) {
      VAST_DEBUG("{} got predicate: {}", self, pred);
      VAST_ASSERT(self->state.idx);
      auto& idx = *self->state.idx;
      auto rep = to_internal(idx.type(), make_view(pred.rhs));
      return idx.lookup(pred.op, rep);
    },
    [self](atom::shutdown) { self->quit(caf::exit_reason::user_shutdown); },
  };
}

} // namespace vast::system
