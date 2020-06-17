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

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/view.hpp"

#include <caf/attach_stream_sink.hpp>

#include <new>

namespace vast::system {

indexer_state::indexer_state() {
  // nop
}

indexer_state::~indexer_state() {
  col.~column_index();
}

caf::error indexer_state::init(caf::event_based_actor* self, path filename,
                               type column_type, caf::settings index_opts,
                               caf::actor index, uuid partition_id,
                               atomic_measurement* m) {
  this->index = std::move(index);
  this->partition_id = partition_id;
  this->measurement = m;
  new (&col) column_index(self->system(), std::move(column_type),
                          std::move(index_opts), std::move(filename));
  return col.init();
}

caf::behavior
indexer(caf::stateful_actor<indexer_state>* self, path filename,
        type column_type, caf::settings index_opts, caf::actor index,
        uuid partition_id, atomic_measurement* m) {
  VAST_TRACE(VAST_ARG(filename), VAST_ARG(column_type));
  VAST_DEBUG(self, "operates for column of type", column_type);
  if (auto err = self->state.init(self, std::move(filename),
                                  std::move(column_type), std::move(index_opts),
                                  std::move(index), partition_id, m)) {
    self->quit(std::move(err));
    return {};
  }
  auto handle_batch = [=](const std::vector<table_slice_column>& xs) {
    auto t = atomic_timer::start(*self->state.measurement);
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
        [](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, const std::vector<table_slice_column>& xs) {
          handle_batch(xs);
        },
        [=](caf::unit_t&, const error& err) {
          auto& st = self->state;
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
    [=](atom::shutdown) {
      self->quit(caf::exit_reason::user_shutdown); // clang-format fix
    },
  };
}

} // namespace vast::system
