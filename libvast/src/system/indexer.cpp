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

#include "vast/chunk.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/attach_stream_sink.hpp>

#include <flatbuffers/flatbuffers.h>

#include "caf/binary_serializer.hpp"
#include "caf/response_promise.hpp"
#include "caf/skip.hpp"
#include "caf/stateful_actor.hpp"

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

caf::behavior passive_indexer(caf::stateful_actor<indexer_state>* self,
                              uuid partition_id, value_index_ptr idx) {
  if (!idx) {
    VAST_ERROR(self, "got invalid index");
    self->quit(make_error(ec::end_of_input, "invalid index"));
    return {};
  }
  self->state.name = "indexer-" + to_string(idx->type());
  self->state.partition_id = partition_id;
  self->state.idx = std::move(idx);
  return {
    [=](caf::stream<table_slice_column>) {
      VAST_ASSERT(!"received incoming stream as read-only indexer");
    },
    [=](atom::snapshot) {
      VAST_ASSERT(!"received snapshot request as read-only indexer");
    },
    [=](const curried_predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      VAST_ASSERT(self->state.idx);
      auto& idx = *self->state.idx;
      auto rep = to_internal(idx.type(), make_view(pred.rhs));
      return idx.lookup(pred.op, rep);
    },
    [=](atom::shutdown) { self->quit(caf::exit_reason::user_shutdown); },
  };
}

caf::behavior active_indexer(caf::stateful_actor<indexer_state>* self,
                             type index_type, caf::settings index_opts) {
  self->state.name = "indexer-" + to_string(index_type);
  self->state.has_skip_attribute = vast::has_skip_attribute(index_type);
  return {
    [=](caf::stream<table_slice_column> in) {
      VAST_DEBUG(self, "received new table slice stream");
      return caf::attach_stream_sink(
        self, in,
        [=](caf::unit_t&) {
          self->state.idx = factory<value_index>::make(index_type, index_opts);
          if (!self->state.idx) {
            VAST_ERROR(self, "failed constructing index");
            self->quit(make_error(ec::unspecified, "fail constructing index"));
          }
        },
        [=](caf::unit_t&, const std::vector<table_slice_column>& xs) {
          VAST_ASSERT(self->state.idx != nullptr);
          self->state.stream_initiated = true;
          // NOTE: It seems like having the `#skip` attribute should lead to
          // no index being created at all (as opposed to creating it and
          // never adding data), but that was the behaviour of the previous
          // implementation so we're keeping it for now.
          if (self->state.has_skip_attribute)
            return;
          for (auto& x : xs) {
            for (size_t i = 0; i < x.slice->rows(); ++i) {
              auto v = x.slice->at(i, x.column);
              self->state.idx->append(v, x.slice->offset() + i);
            }
          }
        },
        [=](caf::unit_t&, const error& err) {
          if (err) {
            // Exit reason `user_shutdown` means that the actor has exited,
            // so we can't use `self` anymore.
            if (err == caf::exit_reason::user_shutdown)
              VAST_ERROR_ANON("indexer got a stream error:", err);
            else
              VAST_ERROR(self,
                         "got a stream error:", self->system().render(err));
            return;
          }
          if (self->state.promise.pending())
            self->state.promise.deliver(chunkify(self->state.idx));
        });
    },
    [=](const curried_predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      VAST_ASSERT(self->state.idx);
      auto& idx = *self->state.idx;
      auto rep = to_internal(idx.type(), make_view(pred.rhs));
      return idx.lookup(pred.op, rep);
    },
    [=](atom::snapshot) {
      // The partition is only allowed to send a single snapshot atom.
      VAST_ASSERT(!self->state.promise.pending());
      self->state.promise = self->make_response_promise();
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
    [=](atom::shutdown) {
      self->quit(caf::exit_reason::user_shutdown); // clang-format fix
    },
  };
}

} // namespace vast::system
