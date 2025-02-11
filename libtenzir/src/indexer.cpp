//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/indexer.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"
#include "tenzir/view.hpp"

namespace tenzir {

indexer_actor::behavior_type
passive_indexer(indexer_actor::stateful_pointer<indexer_state> self,
                uuid partition_id, value_index_ptr index) {
  TENZIR_ASSERT(index);
  TENZIR_DEBUG("{} spawned as passive indexer for a column of type {}", *self,
               index->type());
  self->state().partition_id = partition_id;
  self->state().idx = std::move(index);
  return {
    [self](atom::evaluate, const curried_predicate& pred) -> caf::result<ids> {
      TENZIR_DEBUG("{} got predicate: {}", *self, pred);
      TENZIR_ASSERT(self->state().idx);
      auto& idx = *self->state().idx;
      auto rep = to_internal(idx.type(), make_view(pred.rhs));
      return idx.lookup(pred.op, rep);
    },
    [self](atom::shutdown) {
      self->quit(caf::exit_reason::user_shutdown);
    },
  };
}

} // namespace tenzir
