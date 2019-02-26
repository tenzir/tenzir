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

#include "vast/column_index.hpp"

#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"
#include "vast/value_index_factory.hpp"

namespace vast {

// -- free functions -----------------------------------------------------------

caf::expected<column_index_ptr> make_column_index(caf::actor_system& sys,
                                                  path filename,
                                                  type column_type,
                                                  size_t column) {
  auto result = std::make_unique<column_index>(sys, std::move(column_type),
                                               std::move(filename), column);
  if (auto err = result->init())
    return err;
  return result;
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::column_index(caf::actor_system& sys, type index_type,
                           path filename, size_t column)
  : col_(column),
    has_skip_attribute_(vast::has_skip_attribute(index_type)),
    index_type_(std::move(index_type)),
    filename_(std::move(filename)),
    sys_(sys) {
  // nop
}

column_index::~column_index() {
  flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error column_index::init() {
  VAST_TRACE("");
  // Materialize the index when encountering persistent state.
  if (exists(filename_)) {
    if (auto err = load(nullptr, filename_, last_flush_, idx_)) {
      VAST_ERROR(this, "failed to load value index from disk", sys_.render(err));
      return err;
    } else {
      VAST_DEBUG(this, "loaded value index with offset", idx_->offset());
    }
    return caf::none;
  }
  // Otherwise construct a new one.
  idx_ = factory<value_index>::make(index_type_);
  if (idx_ == nullptr) {
    VAST_ERROR(this, "failed to construct index");
    return make_error(ec::unspecified, "failed to construct index");
  }
  VAST_DEBUG(this, "constructed new value index");
  return caf::none;
}

caf::error column_index::flush_to_disk() {
  VAST_TRACE("");
  // The value index is null if and only if `init()` failed.
  if (idx_ == nullptr || !dirty())
    return caf::none;
  // Check whether there's something to write.
  auto offset = idx_->offset();
  VAST_DEBUG(this, "flushes index (" << (offset - last_flush_) << '/' << offset,
             "new/total bits)");
  last_flush_ = offset;
  return save(nullptr, filename_, last_flush_, idx_);
}

// -- properties -------------------------------------------------------------

void column_index::add(const table_slice_ptr& x) {
  VAST_TRACE(VAST_ARG(x));
  if (has_skip_attribute_)
    return;
  x->append_column_to_index(col_, *idx_);
}

caf::expected<bitmap> column_index::lookup(relational_operator op,
                                           data_view rhs) {
  VAST_TRACE(VAST_ARG(op), VAST_ARG(rhs));
  VAST_ASSERT(idx_ != nullptr);
  auto result = idx_->lookup(op, rhs);
  VAST_DEBUG(this, VAST_ARG(result));
  return result;
}

bool column_index::dirty() const noexcept {
  VAST_ASSERT(idx_ != nullptr);
  return idx_->offset() != last_flush_;
}

} // namespace vast
