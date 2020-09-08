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

#include "caf/binary_serializer.hpp"
#include "caf/response_promise.hpp"
#include "caf/skip.hpp"
#include "caf/stateful_actor.hpp"

#include <flatbuffers/flatbuffers.h>

namespace vast::system {

namespace v2 {

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

caf::behavior active_indexer(caf::stateful_actor<indexer_state>* self, type index_type,
                      caf::settings index_opts) {
  self->state.name = "indexer-" + to_string(index_type);
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
      VAST_ASSERT(
        !self->state.promise.pending());
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

} // namespace v2

indexer_state::indexer_state() {
  // nop
}

indexer_state::~indexer_state() {
  col.~column_index();
}

caf::error
indexer_state::init(caf::event_based_actor* self, path filename,
                    type column_type, caf::settings index_opts,
                    caf::actor index, uuid partition_id, std::string fqn) {
  this->index = std::move(index);
  this->partition_id = partition_id;
  this->fqn = fqn;
  this->self = self;
  this->streaming_done = false;
  new (&col) column_index(self->system(), std::move(column_type),
                          std::move(index_opts), std::move(filename));
  return col.init();
}

void indexer_state::send_report() {
  VAST_ASSERT(accountant != nullptr);
  performance_report r;
  if (m.events > 0) {
    VAST_TRACE(self, "indexed", m.events, "events for column", fqn, "at",
               m.rate_per_sec(), "events/s");
    r.push_back({fqn, m});
    m = measurement{};
  }
  if (!r.empty())
    self->send(accountant, std::move(r));
}

caf::behavior indexer(caf::stateful_actor<indexer_state>* self, path filename,
                      type column_type, caf::settings index_opts,
                      caf::actor index, uuid partition_id, std::string fqn) {
  VAST_TRACE(VAST_ARG(filename), VAST_ARG(column_type));
  VAST_DEBUG(self, "operates for column of type", column_type);
  if (auto err
      = self->state.init(self, std::move(filename), std::move(column_type),
                         std::move(index_opts), std::move(index), partition_id,
                         std::move(fqn))) {
    self->quit(std::move(err));
    return {};
  }
  auto handle_batch = [=](const std::vector<table_slice_column>& xs) {
    auto t = timer::start(self->state.m);
    auto events = uint64_t{0};
    for (auto& x : xs) {
      events += x.slice->rows();
      self->state.col.add(x);
    }
    t.stop(events);
  };
  return {
    [=](const curried_predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      return self->state.col.lookup(pred.op, make_view(pred.rhs));
    },
    [=](atom::persist) -> caf::result<void> {
      if (auto err = self->state.col.flush_to_disk(); err != caf::none)
        return err;
      return caf::unit;
    },
    [=](caf::stream<table_slice_column> in) {
      self->make_sink(
        in,
        [=](caf::unit_t&) {
          // Assume that exactly one stream is created for each indexer.
          auto& st = self->state;
          if (st.accountant) {
            self->send(st.accountant, atom::announce_v, "indexer:" + st.fqn);
            self->delayed_send(self, defaults::system::telemetry_rate,
                               atom::telemetry_v);
          }
        },
        [=](caf::unit_t&, const std::vector<table_slice_column>& xs) {
          handle_batch(xs);
        },
        [=](caf::unit_t&, const error& err) {
          auto& st = self->state;
          st.streaming_done = true;
          if (auto flush_err = st.col.flush_to_disk())
            VAST_WARNING(self, "failed to persist state:",
                         self->system().render(flush_err));
          if (err && err != caf::exit_reason::user_shutdown) {
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
            return;
          }
          self->send(st.index, atom::done_v, st.partition_id);
        });
    },
    [=](const std::vector<table_slice_column>& xs) {
      handle_batch(xs); // clang-format fix
    },
    [=](atom::telemetry) {
      self->state.send_report();
      // The indexers are relying on caf's reference counting to shut down, so
      // we stop telemetry data once the table slice stream is finished.
      // Otherwise this loop would keep a reference to this indexer alive;
      // preventing the actor from quitting and blocking the shutdown of caf.
      if (!self->state.streaming_done)
        self->delayed_send(self, defaults::system::telemetry_rate,
                           atom::telemetry_v);
    },
    [=](atom::shutdown) {
      self->quit(caf::exit_reason::user_shutdown); // clang-format fix
    },
    [=](accountant_type accountant) {
      self->state.accountant = std::move(accountant);
    },
  };
}

} // namespace vast::system
