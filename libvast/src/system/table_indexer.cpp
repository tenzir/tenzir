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

#include "vast/system/table_indexer.hpp"

#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/index.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

namespace vast::system {

// -- constructors, destructors, and assignment operators ----------------------

table_indexer::table_indexer(partition* parent, const record_type& layout)
  : partition_(parent),
    type_erased_layout_(layout),
    last_flush_size_(0),
    skip_mask_(0) {
  VAST_ASSERT(partition_ != nullptr);
  VAST_ASSERT(layout.fields.size() > 0);
  VAST_TRACE(VAST_ARG(type_erased_layout_));
  // Compute which fields to skip.
  auto& fields = layout.fields;
  skip_mask_.reserve(fields.size());
  for (size_t column = 0; column < fields.size(); ++column)
    skip_mask_.emplace_back(has_skip_attribute(fields[column].type));
  // Fill indexers_ with default-constructed handles. We lazily spawn INDEXER
  // actors as we go.
  indexers_.resize(fields.size());
}

table_indexer::~table_indexer() noexcept {
  flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error table_indexer::init() {
  VAST_TRACE("");
  auto filename = row_ids_file();
  if (exists(filename))
    if (auto err = load(self()->system(), filename, row_ids_))
      return err;
  set_clean();
  return caf::none;
}

caf::error table_indexer::eager_init() {
  VAST_TRACE("");
  if (auto err = init())
    return err;
  for (size_t column = 0; column < indexers_.size(); ++column)
    if (!skips_column(column))
      indexer_at(column);
  return caf::none;
}

caf::error table_indexer::flush_to_disk() {
  // Unless `add` was called at least once there's nothing to flush.
  VAST_TRACE("");
  if (!dirty())
    return caf::none;
  if (auto err = save(self()->system(), row_ids_file(), row_ids_))
    return err;
  set_clean();
  return caf::none;
}

/// -- properties --------------------------------------------------------------

index_state& table_indexer::state() {
  return partition_->state();
}

caf::event_based_actor* table_indexer::self() {
  return state().self;
}

caf::actor& table_indexer::indexer_at(size_t column) {
  VAST_ASSERT(column < indexers_.size());
  auto& result = indexers_[column];
  if (!result) {
    result = state().make_indexer(column_file(column),
                                  layout().fields[column].type, column);
    VAST_ASSERT(result != nullptr);
  }
  return result;
}

path table_indexer::row_ids_file() const {
  return partition_dir() / "row_ids";
}

void table_indexer::materialize() {
  VAST_TRACE("");
  for (size_t column = 0; column < columns(); ++column) {
    // We ignore the returned reference, since we're only interested in the
    // side effect of lazily spinning up INDEXER actors.
    indexer_at(column);
  }
}

path table_indexer::partition_dir() const {
  return partition_->base_dir();
}

path table_indexer::base_dir() const {
  return partition_dir() / to_digest(layout());
}

path table_indexer::data_dir() const {
  return base_dir() / "data";
}

path table_indexer::column_file(size_t column) const {
  return data_dir() / detail::replace_all(layout().fields[column].name, ".",
                                          path::separator);
}

void table_indexer::add(const table_slice_ptr& x) {
  VAST_ASSERT(x != nullptr);
  VAST_ASSERT(x->layout() == layout());
  VAST_TRACE(VAST_ARG(x));
  // Store IDs of the new rows.
  auto first = x->offset();
  auto last = x->offset() + x->rows();
  VAST_ASSERT(first < last);
  VAST_ASSERT(first >= row_ids_.size());
  row_ids_.append_bits(false, first - row_ids_.size());
  row_ids_.append_bits(true, last - first);
}

} // namespace vast::system
