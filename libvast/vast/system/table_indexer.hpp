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

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include <caf/actor.hpp>
#include <caf/fwd.hpp>
#include <caf/ref_counted.hpp>

#include "vast/bitvector.hpp"
#include "vast/detail/range.hpp"
#include "vast/filesystem.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/system/fwd.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/type.hpp"

namespace vast::system {

/// Wraps multiple INDEXER actors according to a layout and dispatches queries.
class table_indexer {
public:
  // -- destructor, constructors, assignment operators, and factory ------------

  ~table_indexer() noexcept;

  table_indexer(const table_indexer&) = delete;
  table_indexer& operator=(const table_indexer&) = delete;

  table_indexer(table_indexer&&) = default;
  table_indexer& operator=(table_indexer&&) = default;

  /// @pre `parent != nullptr`
  static caf::expected<table_indexer> make(partition* parent,
                                           const record_type& layout);

  // -- persistence ------------------------------------------------------------

  /// Loads state from disk.
  caf::error init();

  /// Persists all indexes to disk.
  caf::error flush_to_disk();

  /// -- properties ------------------------------------------------------------

  /// @returns the number of columns.
  auto columns() const noexcept {
    return indexers_.size();
  }

  /// @returns the state of the INDEX.
  index_state& state();

  /// @returns the INDEX actor.
  caf::event_based_actor* self();

  /// @returns the INDEXER actor for given column, spawning it lazily if needed.
  /// @pre `column_size < columns()`
  /// @pre `!skip_column(column)`
  caf::actor& indexer_at(size_t column);

  /// @returns the path to the file for persisting `row_ids_`.
  path row_ids_file() const;

  /// @reutrns the IDs of all rows in this table.
  const ids& row_ids() const {
    return row_ids_;
  }

  /// Spawns all currently unloaded INDEXER actors.
  void spawn_indexers();

  /// @returns the list of all INDEXER actors.
  /// @warning may contain invalid actor handles
  /// @warning we fill this list lazily
  auto& indexers() const noexcept {
    return indexers_;
  }

  /// Iterates all loaded INDEXER actors, skipping all default-constructed
  /// actor handles in `indexers()`.
  template <class F>
  void for_each_indexer(F fun) {
    for (auto& hdl : indexers_)
      if (hdl)
        fun(hdl);
  }

  /// @returns the type defining this table's layout.
  const record_type& layout() const noexcept;

  /// @returns whether `add` was called at least once.
  auto dirty() const noexcept {
    return row_ids_.size() != last_flush_size_;
  }

  /// @returns the base directory for the parent partition.
  path partition_dir() const;

  /// @returns the base directory for persistent state.
  path base_dir() const;

  /// @returns the base directory for column indexes.
  path data_dir() const;

  /// @returns the file name for `column`.
  path column_file(size_t column) const;

  /// Indexes a slice for all columns.
  /// @param x Table slice for ingestion.
  void add(const table_slice_ptr& x);

private:
  // -- constructor ------------------------------------------------------------

  /// @pre `parent != nullptr`
  table_indexer(partition* parent, const record_type& layout);

  // -- utility functions ------------------------------------------------------

  /// Marks the state as clean, i.e. persistet.
  /// @post `dirty() == false`
  void set_clean() noexcept {
    last_flush_size_ = row_ids_.size();
  }

  /// @returns whether the meta indexer skips given column
  auto skips_column(size_t column) const noexcept {
    return skip_mask_[column];
  }

  // -- member variables -------------------------------------------------------

  /// Points to the partition managing this table_indexer.
  partition* partition_;

  /// Stores `layout_` in a type-erased handle. We need this type-erased
  /// representation in a few instances such as expression visitors.
  type type_erased_layout_;

  /// Columns of our type-dependant layout. Lazily filled with INDEXER actors.
  std::vector<caf::actor> indexers_;

  /// Instrumentation data store for the layout. One entry for each INDEXER.
  std::vector<atomic_measurement> measurements_;

  /// Stores what IDs are present in this table.
  ids row_ids_;

  /// Stores what size row_ids_ had when we last flushed.
  size_t last_flush_size_;

  /// Stores IDs of skipped columns.
  bitvector<> skip_mask_;

  friend struct index_state;
};

} // namespace vast::system
