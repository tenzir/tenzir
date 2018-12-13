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

#include <new>

#include <caf/all.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"

using namespace caf;

namespace vast::system {

indexer_state::indexer_state() {
  // nop
}

indexer_state::~indexer_state() {
  col.~column_index();
}

caf::error indexer_state::init(event_based_actor* self, path filename,
                               type column_type, size_t column) {
  new (&col) column_index(self->system(), std::move(column_type),
                          std::move(filename), column);
  return col.init();
}

behavior indexer(stateful_actor<indexer_state>* self, path dir,
                 type column_type, size_t column) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(column_type), VAST_ARG(column));
  VAST_DEBUG(self, "operates for column", column, "of type", column_type);
  if (auto err = self->state.init(self,
                                  std::move(dir) / "fields"
                                      / std::to_string(column),
                                  std::move(column_type), column)) {
    self->quit(std::move(err));
    return {};
  }
  return {
    [=](const curried_predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      return self->state.col.lookup(pred);
    },
    [=](persist_atom) -> result<void> {
      if (auto err = self->state.col.flush_to_disk(); err != caf::none)
        return err;
      return caf::unit;
    },
    [=](stream<table_slice_ptr> in) {
      self->make_sink(
        in,
        [](unit_t&) {
          // nop
        },
        [=](unit_t&, const std::vector<table_slice_ptr>& xs) {
          for (auto& x : xs)
            self->state.col.add(x);
        },
        [=](unit_t&, const error& err) {
          if (err && err != caf::exit_reason::user_shutdown)
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
        }
      );
    },
    [=](shutdown_atom) {
      self->quit(exit_reason::user_shutdown);
    },
  };
}

} // namespace vast::system
