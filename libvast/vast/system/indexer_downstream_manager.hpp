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

#include "vast/detail/assert.hpp"
#include "vast/system/fwd.hpp"
#include "vast/table_slice_column.hpp"

#include <caf/downstream_manager_base.hpp>

#include <deque>
#include <unordered_set>

namespace vast::system {

class indexer_downstream_manager : public caf::downstream_manager_base {
public:
  // -- member types -----------------------------------------------------------

  /// Base type.
  using super = caf::downstream_manager_base;

  using output_type = table_slice_column;

  /// Type of `paths_`.
  using typename super::map_type;

  /// Pointer to an outbound path.
  using typename super::path_ptr;

  /// Unique pointer to an outbound path.
  using typename super::unique_path_ptr;

  using buffer_type = std::deque<output_type>;

  using set_type = std::unordered_set<partition*>;

  // -- constructors, destructors, and assignment operators --------------------

  indexer_downstream_manager(caf::stream_manager* parent) : super(parent) {
    // nop
  }

  // -- properties -------------------------------------------------------------

  size_t buffered() const noexcept override;

  size_t buffered(partition& p) const noexcept;

  /// Returns the number of buffered elements for this specific slot, ignoring
  /// the central buffer.
  size_t buffered(caf::stream_slot slot) const noexcept override;

  int32_t max_capacity() const noexcept override;

  // Verbose naming because `register` is a keyword.
  void register_partition(partition* p);

  bool unregister(partition* p);

  // Required for the build - unused.
  buffer_type& buf();

  // -- overridden functions ---------------------------------------------------

  void emit_batches() override;

  void force_emit_batches() override;

protected:
  buffer_type buf_;

  // TODO(ch9680): We always erase the paths to an entire partition.
  // void about_to_erase(caf::outbound_path* ptr, bool silent,
  //                    caf::error* reason) override;

private:
  void cleanup_partition(set_type::iterator& it);

  void try_remove_partition(set_type::iterator& it);

  void emit_batches_impl(bool force_underfull);

  set_type partitions;
  set_type pending_partitions;
};

} // namespace vast::system
