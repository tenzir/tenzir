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
#include "vast/const_table_slice_handle.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"

using namespace caf;

namespace vast::system {

indexer_state::indexer_state() : initialized(false) {
  // nop
}

indexer_state::~indexer_state() {
  if (initialized)
    tbl.~table_index();
}

void indexer_state::init(table_index&& from) {
  VAST_ASSERT(!initialized);
  new (&tbl) table_index(std::move(from));
  initialized = true;
}

behavior indexer(stateful_actor<indexer_state>* self, path dir,
                 record_type layout) {
  auto maybe_tbl = make_table_index(self->system(), std::move(dir), layout);
  if (!maybe_tbl) {
    VAST_ERROR(self, "unable to generate table layout for", layout);
    return {};
  }
  self->state.init(std::move(*maybe_tbl));
  VAST_DEBUG(self, "operates for layout", layout);
  return {
    [=](const predicate& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      return self->state.tbl.lookup(pred);
    },
    [=](const expression& expr) {
      VAST_DEBUG(self, "got expression:", expr);
      return self->state.tbl.lookup(expr);
    },
    [=](persist_atom) -> result<void> {
      if (auto err = self->state.tbl.flush_to_disk(); err != caf::none)
        return err;
      return caf::unit;
    },
    [=](stream<const_table_slice_handle> in) {
      self->make_sink(
        in,
        [](unit_t&) {
          // nop
        },
        [=](unit_t&, const std::vector<const_table_slice_handle>& xs) {
          for (auto& x : xs)
            self->state.tbl.add(x);
        },
        [=](unit_t&, const error& err) {
          if (err && err != caf::exit_reason::user_shutdown) {
            VAST_ERROR(self, "got a stream error:", self->system().render(err));
          }
        }
      );
    },
    [=](shutdown_atom) { self->quit(exit_reason::user_shutdown); },
  };
}

} // namespace vast::system
